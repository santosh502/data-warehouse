"""
Views for the warehouse dashboard and web interface.
"""

from django.shortcuts import render
from django.http import JsonResponse
from django.db.models import Count, Q
from django.views.generic import TemplateView
from django.contrib.postgres.search import SearchVector, SearchQuery, SearchRank
from django.utils import timezone
from datetime import timedelta
import json

from .models import (
    DataRecord, DataSchema, UnstructuredData, 
    DataRecordHistory, UserProfile, QueryLog
)


class DashboardView(TemplateView):
    """
    Main dashboard view showing system statistics and data visualization.
    """
    template_name = 'warehouse/dashboard.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # Basic statistics
        context.update({
            'total_records': DataRecord.objects.filter(is_active=True).count(),
            'total_schemas': DataSchema.objects.filter(is_active=True).count(),
            'total_unstructured': UnstructuredData.objects.filter(is_active=True).count(),
            'total_history_entries': DataRecordHistory.objects.count(),
        })
        
        return context


def dashboard_stats_api(request):
    """
    API endpoint for dashboard statistics.
    """
    try:
        # Time-based statistics
        now = timezone.now()
        last_24h = now - timedelta(hours=24)
        last_7d = now - timedelta(days=7)
        last_30d = now - timedelta(days=30)

        # Record statistics
        stats = {
            'overview': {
                'total_records': DataRecord.objects.filter(is_active=True).count(),
                'total_schemas': DataSchema.objects.filter(is_active=True).count(),
                'total_unstructured': UnstructuredData.objects.filter(is_active=True).count(),
                'total_history': DataRecordHistory.objects.count(),
            },
            'recent_activity': {
                'records_24h': DataRecord.objects.filter(created_at__gte=last_24h).count(),
                'records_7d': DataRecord.objects.filter(created_at__gte=last_7d).count(),
                'records_30d': DataRecord.objects.filter(created_at__gte=last_30d).count(),
            },
            'schema_distribution': list(
                DataRecord.objects.filter(is_active=True)
                .values('schema__name')
                .annotate(count=Count('id'))
                .order_by('-count')[:10]
            ),
            'daily_ingestion': list(
                DataRecord.objects.filter(created_at__gte=last_30d)
                .extra(select={'day': 'date(created_at)'})
                .values('day')
                .annotate(count=Count('id'))
                .order_by('day')
            ),
            'change_activity': list(
                DataRecordHistory.objects.filter(timestamp__gte=last_30d)
                .values('operation')
                .annotate(count=Count('id'))
            )
        }

        return JsonResponse(stats)
    
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


def search_data(request):
    """
    Advanced search endpoint supporting both structured and unstructured data.
    """
    try:
        query = request.GET.get('q', '').strip()
        schema_filter = request.GET.get('schema', '')
        data_type = request.GET.get('type', 'all')  # 'structured', 'unstructured', or 'all'
        limit = min(int(request.GET.get('limit', 50)), 100)

        results = {'structured': [], 'unstructured': []}

        if query:
            # Search structured data
            if data_type in ['structured', 'all']:
                structured_query = DataRecord.objects.filter(is_active=True)
                
                if schema_filter:
                    structured_query = structured_query.filter(schema__name=schema_filter)
                
                # JSONB search for structured data
                structured_query = structured_query.filter(
                    Q(data__icontains=query) |
                    Q(schema__name__icontains=query)
                )
                
                for record in structured_query[:limit]:
                    results['structured'].append({
                        'id': str(record.id),
                        'schema': record.schema.name,
                        'data': record.data,
                        'created_at': record.created_at.isoformat(),
                        'relevance': 'exact_match'  # Could implement proper scoring
                    })

            # Search unstructured data with full-text search
            if data_type in ['unstructured', 'all']:
                search_query = SearchQuery(query)
                search_vector = SearchVector('content', 'title')
                
                unstructured_results = (
                    UnstructuredData.objects
                    .filter(is_active=True)
                    .annotate(
                        search=search_vector,
                        rank=SearchRank(search_vector, search_query)
                    )
                    .filter(search=search_query)
                    .order_by('-rank')[:limit]
                )
                
                for item in unstructured_results:
                    results['unstructured'].append({
                        'id': str(item.id),
                        'title': item.title,
                        'content': item.content[:500] + ('...' if len(item.content) > 500 else ''),
                        'data_type': item.data_type,
                        'metadata': item.metadata,
                        'tags': item.tags,
                        'created_at': item.created_at.isoformat(),
                        'relevance': float(item.rank) if hasattr(item, 'rank') else 0
                    })

        return JsonResponse({
            'query': query,
            'results': results,
            'total_structured': len(results['structured']),
            'total_unstructured': len(results['unstructured'])
        })

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


def data_history_api(request, record_id):
    """
    Get change history for a specific data record.
    """
    try:
        history = DataRecordHistory.objects.filter(
            record_id=record_id
        ).order_by('-timestamp')[:50]

        history_data = []
        for entry in history:
            history_data.append({
                'id': str(entry.id),
                'operation': entry.operation,
                'old_data': entry.old_data,
                'new_data': entry.new_data,
                'changed_fields': entry.changed_fields,
                'timestamp': entry.timestamp.isoformat(),
                'changed_by': entry.changed_by.username if entry.changed_by else None,
                'ip_address': entry.ip_address
            })

        return JsonResponse({
            'record_id': record_id,
            'history': history_data,
            'total_changes': len(history_data)
        })

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


def aggregate_data_api(request):
    """
    Aggregation endpoint for generating reports and analytics.
    """
    try:
        agg_type = request.GET.get('type', 'schema_count')
        schema_filter = request.GET.get('schema', '')
        time_period = request.GET.get('period', '30d')  # 1d, 7d, 30d, 90d, 1y

        # Calculate time filter
        now = timezone.now()
        time_map = {
            '1d': timedelta(days=1),
            '7d': timedelta(days=7),
            '30d': timedelta(days=30),
            '90d': timedelta(days=90),
            '1y': timedelta(days=365)
        }
        time_filter = now - time_map.get(time_period, timedelta(days=30))

        # Base queryset
        base_query = DataRecord.objects.filter(
            is_active=True,
            created_at__gte=time_filter
        )

        if schema_filter:
            base_query = base_query.filter(schema__name=schema_filter)

        results = {}

        if agg_type == 'schema_count':
            results = list(
                base_query.values('schema__name')
                .annotate(count=Count('id'))
                .order_by('-count')
            )
        
        elif agg_type == 'daily_trend':
            results = list(
                base_query.extra(select={'day': 'date(created_at)'})
                .values('day')
                .annotate(count=Count('id'))
                .order_by('day')
            )
        
        elif agg_type == 'change_operations':
            results = list(
                DataRecordHistory.objects.filter(timestamp__gte=time_filter)
                .values('operation')
                .annotate(count=Count('id'))
            )

        return JsonResponse({
            'aggregation_type': agg_type,
            'period': time_period,
            'results': results
        })

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


def user_profiles_api(request):
    """
    API for user profile data (example structured data).
    """
    try:
        profiles = UserProfile.objects.all().select_related().prefetch_related(
            'addresses', 'incomes', 'goals'
        )[:50]

        profile_data = []
        for profile in profiles:
            profile_data.append({
                'id': str(profile.id),
                'full_name': f"{profile.first_name} {profile.last_name}",
                'title': profile.title,
                'age': profile.age,
                'addresses': [
                    {
                        'city': addr.city_town,
                        'country': addr.country,
                        'postcode': addr.postcode,
                        'is_primary': addr.is_primary
                    }
                    for addr in profile.addresses.all()
                ],
                'incomes': [
                    {
                        'category': income.category,
                        'frequency': income.frequency,
                        'gross_amount': float(income.gross_amount),
                        'net_amount': float(income.net_amount)
                    }
                    for income in profile.incomes.all()
                ],
                'goals_count': profile.goals.count(),
                'created_at': profile.created_at.isoformat()
            })

        return JsonResponse({
            'profiles': profile_data,
            'total': len(profile_data)
        })

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)
