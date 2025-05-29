"""
API views for the data warehouse system.
Provides RESTful endpoints for data ingestion, querying, and management.
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any

from django.http import JsonResponse, HttpResponse
from django.utils import timezone
from django.db.models import Count, Q
from django.db import models
from django.contrib.postgres.search import SearchVector, SearchQuery, SearchRank
from django.core.paginator import Paginator
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator

from rest_framework import status, generics, permissions
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.pagination import PageNumberPagination
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny

from warehouse.models import (
    DataSchema, DataRecord, DataRecordHistory, UnstructuredData,
    QueryLog, DataIngestionJob, UserProfile, Address, Income, Goal
)
from warehouse.services import (
    DataIngestionService, DataRecordHistoryService, QueryService, UserProfileService
)
from .serializers import (
    DataIngestionRequestSerializer, DataIngestionResponseSerializer,
    UnstructuredDataIngestionSerializer, FileUploadSerializer,
    SearchRequestSerializer, SearchResponseSerializer,
    AggregationRequestSerializer, AggregationResultSerializer,
    SchemaCreationSerializer, RecordUpdateSerializer,
    HistoryQuerySerializer, SystemStatsSerializer,
    HealthCheckSerializer, ErrorResponseSerializer,
    DataSchemaSerializer, DataRecordSerializer, DataRecordHistorySerializer,
    UnstructuredDataSerializer, UserProfileSerializer, UserProfileCreateSerializer
)

logger = logging.getLogger(__name__)


class StandardResultsSetPagination(PageNumberPagination):
    """Standard pagination class for API responses."""
    page_size = 50
    page_size_query_param = 'page_size'
    max_page_size = 100


class APIRootView(APIView):
    """
    API root view providing information about available endpoints.
    """
    permission_classes = [AllowAny]
    
    def get(self, request, format=None):
        """Return API information and available endpoints."""
        return Response({
            'message': 'Welcome to the Data Warehouse API',
            'version': '1.0',
            'timestamp': timezone.now().isoformat(),
            'endpoints': {
                'health': request.build_absolute_uri('/api/health/'),
                'schemas': request.build_absolute_uri('/api/schemas/'),
                'records': request.build_absolute_uri('/api/records/'),
                'unstructured': request.build_absolute_uri('/api/unstructured/'),
                'search': request.build_absolute_uri('/api/search/'),
                'aggregate': request.build_absolute_uri('/api/aggregate/'),
                'stats': request.build_absolute_uri('/api/stats/'),
                'profiles': request.build_absolute_uri('/api/profiles/'),
                'ingest': {
                    'structured': request.build_absolute_uri('/api/ingest/structured/'),
                    'unstructured': request.build_absolute_uri('/api/ingest/unstructured/'),
                    'csv': request.build_absolute_uri('/api/ingest/csv/'),
                    'json': request.build_absolute_uri('/api/ingest/json/'),
                    'bulk': request.build_absolute_uri('/api/ingest/bulk/'),
                }
            },
            'documentation': 'Visit /api/ for interactive API documentation'
        })


class HealthCheckView(APIView):
    """
    Health check endpoint for monitoring system status.
    """
    permission_classes = [AllowAny]
    
    def get(self, request, format=None):
        """Return system health status."""
        try:
            # Check database connectivity
            record_count = DataRecord.objects.count()
            schema_count = DataSchema.objects.count()
            
            health_data = {
                'status': 'healthy',
                'timestamp': timezone.now(),
                'version': '1.0',
                'database_status': 'connected',
                'total_records': record_count,
                'total_schemas': schema_count,
                'uptime': 'N/A'  # Would calculate actual uptime in production
            }
            
            serializer = HealthCheckSerializer(health_data)
            return Response(serializer.data, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
            return Response({
                'status': 'unhealthy',
                'timestamp': timezone.now(),
                'error': str(e),
                'database_status': 'disconnected'
            }, status=status.HTTP_503_SERVICE_UNAVAILABLE)


class StructuredDataIngestionView(APIView):
    """
    Ingest structured data records.
    """
    def post(self, request, format=None):
        """Ingest structured data in bulk."""
        serializer = DataIngestionRequestSerializer(data=request.data)
        if serializer.is_valid():
            try:
                start_time = time.time()
                
                success_count, error_count, error_messages = DataIngestionService.ingest_structured_data(
                    schema_name=serializer.validated_data['schema_name'],
                    data_list=serializer.validated_data['data'],
                    source_file=serializer.validated_data.get('source_file'),
                    user=request.user if request.user.is_authenticated else None
                )
                
                execution_time = time.time() - start_time
                
                response_data = {
                    'success': True,
                    'total_records': len(serializer.validated_data['data']),
                    'success_count': success_count,
                    'error_count': error_count,
                    'error_messages': error_messages[:10],  # Limit error messages
                    'execution_time': execution_time
                }
                
                response_serializer = DataIngestionResponseSerializer(response_data)
                return Response(response_serializer.data, status=status.HTTP_201_CREATED)
                
            except Exception as e:
                logger.error(f"Structured data ingestion error: {str(e)}")
                return Response({
                    'success': False,
                    'error': str(e),
                    'timestamp': timezone.now()
                }, status=status.HTTP_400_BAD_REQUEST)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class UnstructuredDataIngestionView(APIView):
    """
    Ingest unstructured data.
    """
    def post(self, request, format=None):
        """Ingest unstructured data."""
        serializer = UnstructuredDataIngestionSerializer(data=request.data)
        if serializer.is_valid():
            try:
                unstructured_data = DataIngestionService.ingest_unstructured_data(
                    content=serializer.validated_data['content'],
                    title=serializer.validated_data.get('title'),
                    data_type=serializer.validated_data.get('data_type', 'TEXT'),
                    metadata=serializer.validated_data.get('metadata', {}),
                    tags=serializer.validated_data.get('tags', []),
                    user=request.user if request.user.is_authenticated else None
                )
                
                response_serializer = UnstructuredDataSerializer(unstructured_data)
                return Response(response_serializer.data, status=status.HTTP_201_CREATED)
                
            except Exception as e:
                logger.error(f"Unstructured data ingestion error: {str(e)}")
                return Response({
                    'error': str(e),
                    'timestamp': timezone.now()
                }, status=status.HTTP_400_BAD_REQUEST)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class CSVIngestionView(APIView):
    """
    Ingest data from CSV files.
    """
    def post(self, request, format=None):
        """Process CSV file upload and ingest data."""
        serializer = FileUploadSerializer(data=request.data)
        if serializer.is_valid():
            try:
                file_content = serializer.validated_data['file'].read().decode('utf-8')
                schema_name = serializer.validated_data['schema_name']
                
                result = DataIngestionService.ingest_csv_file(
                    file_content=file_content,
                    schema_name=schema_name,
                    user=request.user if request.user.is_authenticated else None
                )
                
                if result['success']:
                    return Response(result, status=status.HTTP_201_CREATED)
                else:
                    return Response(result, status=status.HTTP_400_BAD_REQUEST)
                    
            except UnicodeDecodeError:
                return Response({
                    'error': 'File encoding not supported. Please use UTF-8 encoding.',
                    'timestamp': timezone.now()
                }, status=status.HTTP_400_BAD_REQUEST)
            except Exception as e:
                logger.error(f"CSV ingestion error: {str(e)}")
                return Response({
                    'error': str(e),
                    'timestamp': timezone.now()
                }, status=status.HTTP_400_BAD_REQUEST)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class JSONIngestionView(APIView):
    """
    Ingest data from JSON files.
    """
    def post(self, request, format=None):
        """Process JSON file upload and ingest data."""
        serializer = FileUploadSerializer(data=request.data)
        if serializer.is_valid():
            try:
                file_content = serializer.validated_data['file'].read().decode('utf-8')
                schema_name = serializer.validated_data['schema_name']
                
                result = DataIngestionService.ingest_json_file(
                    file_content=file_content,
                    schema_name=schema_name,
                    user=request.user if request.user.is_authenticated else None
                )
                
                if result['success']:
                    return Response(result, status=status.HTTP_201_CREATED)
                else:
                    return Response(result, status=status.HTTP_400_BAD_REQUEST)
                    
            except UnicodeDecodeError:
                return Response({
                    'error': 'File encoding not supported. Please use UTF-8 encoding.',
                    'timestamp': timezone.now()
                }, status=status.HTTP_400_BAD_REQUEST)
            except Exception as e:
                logger.error(f"JSON ingestion error: {str(e)}")
                return Response({
                    'error': str(e),
                    'timestamp': timezone.now()
                }, status=status.HTTP_400_BAD_REQUEST)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class BulkDataIngestionView(APIView):
    """
    High-performance bulk data ingestion endpoint.
    """
    def post(self, request, format=None):
        """Handle large-scale bulk data ingestion."""
        serializer = DataIngestionRequestSerializer(data=request.data)
        if serializer.is_valid():
            try:
                # For very large datasets, consider implementing async processing
                start_time = time.time()
                
                data_list = serializer.validated_data['data']
                if len(data_list) > 1000:
                    logger.info(f"Processing large bulk ingestion: {len(data_list)} records")
                
                success_count, error_count, error_messages = DataIngestionService.ingest_structured_data(
                    schema_name=serializer.validated_data['schema_name'],
                    data_list=data_list,
                    source_file=serializer.validated_data.get('source_file', 'bulk_api'),
                    user=request.user if request.user.is_authenticated else None
                )
                
                execution_time = time.time() - start_time
                
                response_data = {
                    'success': True,
                    'total_records': len(data_list),
                    'success_count': success_count,
                    'error_count': error_count,
                    'error_messages': error_messages[:5],  # Fewer errors for bulk
                    'execution_time': execution_time,
                    'records_per_second': len(data_list) / execution_time if execution_time > 0 else 0
                }
                
                return Response(response_data, status=status.HTTP_201_CREATED)
                
            except Exception as e:
                logger.error(f"Bulk ingestion error: {str(e)}")
                return Response({
                    'error': str(e),
                    'timestamp': timezone.now()
                }, status=status.HTTP_400_BAD_REQUEST)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class SearchView(APIView):
    """
    Universal search endpoint for both structured and unstructured data.
    """
    def get(self, request, format=None):
        """Perform search across all data types."""
        query_params = {
            'query': request.GET.get('q', ''),
            'schema': request.GET.get('schema', ''),
            'data_type': request.GET.get('type', 'all'),
            'limit': int(request.GET.get('limit', 50)),
            'offset': int(request.GET.get('offset', 0))
        }
        
        serializer = SearchRequestSerializer(data=query_params)
        if serializer.is_valid():
            try:
                start_time = time.time()
                
                query = serializer.validated_data['query']
                schema_filter = serializer.validated_data.get('schema')
                data_type = serializer.validated_data['data_type']
                limit = serializer.validated_data['limit']
                offset = serializer.validated_data['offset']
                
                results = []
                total_count = 0
                
                # Search structured data
                structured_results = []
                unstructured_results = []
                
                if data_type in ['structured', 'all']:
                    # Search DataRecord for structured data
                    structured_qs = DataRecord.objects.filter(is_active=True)
                    
                    if schema_filter:
                        structured_qs = structured_qs.filter(schema__name__icontains=schema_filter)
                    
                    # Search in JSON data fields
                    if query:
                        structured_qs = structured_qs.filter(
                            models.Q(data__icontains=query) |
                            models.Q(schema__name__icontains=query)
                        )
                    
                    structured_qs = structured_qs.select_related('schema')[offset:offset+limit]
                    
                    for record in structured_qs:
                        structured_results.append({
                            'id': str(record.id),
                            'type': 'structured',
                            'data': record.data,
                            'schema': record.schema.name,
                            'created_at': record.created_at.isoformat(),
                            'relevance': 1.0
                        })
                
                # Search unstructured data
                if data_type in ['unstructured', 'all']:
                    unstructured_qs = UnstructuredData.objects.filter(is_active=True)
                    
                    if query:
                        unstructured_qs = unstructured_qs.filter(
                            models.Q(title__icontains=query) |
                            models.Q(content__icontains=query) |
                            models.Q(tags__contains=[query])
                        )
                    
                    unstructured_qs = unstructured_qs[offset:offset+limit]
                    
                    for item in unstructured_qs:
                        unstructured_results.append({
                            'id': str(item.id),
                            'type': 'unstructured',
                            'title': item.title,
                            'content': item.content[:500] + ('...' if len(item.content) > 500 else ''),
                            'data_type': item.data_type,
                            'metadata': item.metadata,
                            'tags': item.tags,
                            'created_at': item.created_at.isoformat(),
                            'relevance': 0.8
                        })
                
                results = structured_results + unstructured_results
                total_count = len(results)
                
                execution_time = time.time() - start_time
                
                # Log search query
                QueryLog.objects.create(
                    query_type='universal_search',
                    query_params=serializer.validated_data,
                    execution_time=execution_time,
                    result_count=len(results),
                    user=request.user if request.user.is_authenticated else None
                )
                
                response_data = {
                    'query': query,
                    'total_count': total_count,
                    'execution_time': execution_time,
                    'results': {
                        'structured': structured_results,
                        'unstructured': unstructured_results
                    }
                }
                
                return Response(response_data, status=status.HTTP_200_OK)
                
            except Exception as e:
                logger.error(f"Search error: {str(e)}")
                return Response({
                    'error': str(e),
                    'timestamp': timezone.now()
                }, status=status.HTTP_400_BAD_REQUEST)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class StructuredSearchView(APIView):
    """
    Dedicated search endpoint for structured data only.
    """
    def get(self, request, format=None):
        """Search structured data with advanced filtering."""
        query_params = {
            'query': request.GET.get('q', ''),
            'schema': request.GET.get('schema', ''),
            'limit': int(request.GET.get('limit', 50)),
            'offset': int(request.GET.get('offset', 0))
        }
        
        try:
            result = QueryService.search_structured_data(
                query=query_params['query'],
                schema_name=query_params['schema'],
                limit=query_params['limit'],
                offset=query_params['offset']
            )
            
            return Response(result, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Structured search error: {str(e)}")
            return Response({
                'error': str(e),
                'timestamp': timezone.now()
            }, status=status.HTTP_400_BAD_REQUEST)


class UnstructuredSearchView(APIView):
    """
    Dedicated search endpoint for unstructured data only.
    """
    def get(self, request, format=None):
        """Search unstructured data with full-text search."""
        query_params = {
            'query': request.GET.get('q', ''),
            'data_type': request.GET.get('data_type', ''),
            'limit': int(request.GET.get('limit', 50)),
            'offset': int(request.GET.get('offset', 0))
        }
        
        try:
            result = QueryService.search_unstructured_data(
                query=query_params['query'],
                data_type=query_params['data_type'],
                limit=query_params['limit'],
                offset=query_params['offset']
            )
            
            return Response(result, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Unstructured search error: {str(e)}")
            return Response({
                'error': str(e),
                'timestamp': timezone.now()
            }, status=status.HTTP_400_BAD_REQUEST)


class AggregationView(APIView):
    """
    Data aggregation and analytics endpoint.
    """
    def get(self, request, format=None):
        """Perform data aggregation operations."""
        query_params = {
            'type': request.GET.get('type', 'record_count_by_schema'),
            'schema': request.GET.get('schema', ''),
            'period': request.GET.get('period', '30d'),
            'group_by': request.GET.get('group_by', '')
        }
        
        serializer = AggregationRequestSerializer(data=query_params)
        if serializer.is_valid():
            try:
                result = QueryService.aggregate_data(
                    aggregation_type=serializer.validated_data['type'],
                    schema_name=serializer.validated_data.get('schema'),
                    time_period=serializer.validated_data['period'],
                    group_by=serializer.validated_data.get('group_by')
                )
                
                response_serializer = AggregationResultSerializer(result)
                return Response(response_serializer.data, status=status.HTTP_200_OK)
                
            except Exception as e:
                logger.error(f"Aggregation error: {str(e)}")
                return Response({
                    'error': str(e),
                    'timestamp': timezone.now()
                }, status=status.HTTP_400_BAD_REQUEST)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class SystemStatsView(APIView):
    """
    System statistics and dashboard data endpoint.
    """
    def get(self, request, format=None):
        """Get comprehensive system statistics."""
        try:
            # Time-based statistics
            now = timezone.now()
            last_24h = now - timedelta(hours=24)
            last_7d = now - timedelta(days=7)
            last_30d = now - timedelta(days=30)

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
            
            serializer = SystemStatsSerializer(stats)
            return Response(serializer.data, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"System stats error: {str(e)}")
            return Response({
                'error': str(e),
                'timestamp': timezone.now()
            }, status=status.HTTP_400_BAD_REQUEST)


# Schema Management Views
class DataSchemaListCreateView(generics.ListCreateAPIView):
    """List and create data schemas."""
    queryset = DataSchema.objects.filter(is_active=True)
    serializer_class = DataSchemaSerializer
    pagination_class = StandardResultsSetPagination
    
    def perform_create(self, serializer):
        """Create new schema with user tracking."""
        serializer.save()


class DataSchemaDetailView(generics.RetrieveUpdateDestroyAPIView):
    """Retrieve, update, or delete a specific schema."""
    queryset = DataSchema.objects.filter(is_active=True)
    serializer_class = DataSchemaSerializer


# Data Record Views
class DataRecordListView(generics.ListAPIView):
    """List data records with filtering and pagination."""
    serializer_class = DataRecordSerializer
    pagination_class = StandardResultsSetPagination
    
    def get_queryset(self):
        """Filter records based on query parameters."""
        queryset = DataRecord.objects.filter(is_active=True).select_related('schema')
        
        schema_name = self.request.GET.get('schema')
        if schema_name:
            queryset = queryset.filter(schema__name=schema_name)
        
        start_date = self.request.GET.get('start_date')
        if start_date:
            queryset = queryset.filter(created_at__gte=start_date)
        
        end_date = self.request.GET.get('end_date')
        if end_date:
            queryset = queryset.filter(created_at__lte=end_date)
        
        return queryset.order_by('-created_at')


class DataRecordDetailView(generics.RetrieveAPIView):
    """Retrieve a specific data record."""
    queryset = DataRecord.objects.filter(is_active=True)
    serializer_class = DataRecordSerializer


class DataRecordUpdateView(APIView):
    """Update a data record with change tracking."""
    
    def put(self, request, pk, format=None):
        """Update a data record and track changes."""
        serializer = RecordUpdateSerializer(data=request.data)
        if serializer.is_valid():
            try:
                record = DataRecordHistoryService.update_record_with_history(
                    record_id=pk,
                    new_data=serializer.validated_data['data'],
                    user=request.user if request.user.is_authenticated else None,
                    ip_address=request.META.get('REMOTE_ADDR')
                )
                
                response_serializer = DataRecordSerializer(record)
                return Response(response_serializer.data, status=status.HTTP_200_OK)
                
            except Exception as e:
                logger.error(f"Record update error: {str(e)}")
                return Response({
                    'error': str(e),
                    'timestamp': timezone.now()
                }, status=status.HTTP_400_BAD_REQUEST)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class RecordHistoryView(APIView):
    """Get change history for a specific record."""
    
    def get(self, request, pk, format=None):
        """Retrieve change history for a record."""
        try:
            limit = int(request.GET.get('limit', 50))
            operation_filter = request.GET.get('operation', 'ALL')
            
            history = DataRecordHistoryService.get_record_history(
                record_id=pk, 
                limit=limit
            )
            
            if operation_filter != 'ALL':
                history = [h for h in history if h.operation == operation_filter]
            
            serializer = DataRecordHistorySerializer(history, many=True)
            return Response({
                'record_id': pk,
                'history': serializer.data,
                'total_changes': len(serializer.data)
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"History retrieval error: {str(e)}")
            return Response({
                'error': str(e),
                'timestamp': timezone.now()
            }, status=status.HTTP_400_BAD_REQUEST)


# Unstructured Data Views
class UnstructuredDataListCreateView(generics.ListCreateAPIView):
    """List and create unstructured data."""
    queryset = UnstructuredData.objects.filter(is_active=True)
    serializer_class = UnstructuredDataSerializer
    pagination_class = StandardResultsSetPagination
    
    def perform_create(self, serializer):
        """Create unstructured data with user tracking."""
        serializer.save(created_by=self.request.user if self.request.user.is_authenticated else None)


class UnstructuredDataDetailView(generics.RetrieveUpdateDestroyAPIView):
    """Retrieve, update, or delete unstructured data."""
    queryset = UnstructuredData.objects.filter(is_active=True)
    serializer_class = UnstructuredDataSerializer


# User Profile Views (Example Data)
class UserProfileListCreateView(generics.ListCreateAPIView):
    """List and create user profiles."""
    queryset = UserProfile.objects.all()
    pagination_class = StandardResultsSetPagination
    
    def get_serializer_class(self):
        """Use different serializers for read and write operations."""
        if self.request.method == 'POST':
            return UserProfileCreateSerializer
        return UserProfileSerializer


class UserProfileDetailView(generics.RetrieveUpdateDestroyAPIView):
    """Retrieve, update, or delete a user profile."""
    queryset = UserProfile.objects.all()
    serializer_class = UserProfileSerializer


class UserProfileSearchView(APIView):
    """Search user profiles."""
    
    def get(self, request, format=None):
        """Search user profiles."""
        query = request.GET.get('q', '')
        limit = int(request.GET.get('limit', 50))
        
        try:
            profiles = UserProfileService.search_profiles(query=query, limit=limit)
            serializer = UserProfileSerializer(profiles, many=True)
            
            return Response({
                'query': query,
                'results': serializer.data,
                'total_count': len(serializer.data)
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Profile search error: {str(e)}")
            return Response({
                'error': str(e),
                'timestamp': timezone.now()
            }, status=status.HTTP_400_BAD_REQUEST)


# Additional utility views for completeness
class SchemaAnalyticsView(APIView):
    """Analytics specific to schema usage."""
    
    def get(self, request, format=None):
        """Get schema-specific analytics."""
        try:
            analytics = {
                'schema_usage': list(
                    DataRecord.objects.filter(is_active=True)
                    .values('schema__name', 'schema__id')
                    .annotate(
                        record_count=Count('id'),
                        avg_size=Count('data')  # Simplified metric
                    )
                    .order_by('-record_count')
                ),
                'recent_schemas': list(
                    DataSchema.objects.filter(is_active=True)
                    .order_by('-created_at')[:10]
                    .values('name', 'created_at', 'version')
                )
            }
            
            return Response(analytics, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Schema analytics error: {str(e)}")
            return Response({
                'error': str(e),
                'timestamp': timezone.now()
            }, status=status.HTTP_400_BAD_REQUEST)


class TrendAnalyticsView(APIView):
    """Time-based trend analytics."""
    
    def get(self, request, format=None):
        """Get trend analytics over time."""
        try:
            period = request.GET.get('period', '30d')
            
            # Calculate date range
            time_map = {'1d': 1, '7d': 7, '30d': 30, '90d': 90, '1y': 365}
            days = time_map.get(period, 30)
            start_date = timezone.now() - timedelta(days=days)
            
            trends = {
                'daily_records': list(
                    DataRecord.objects.filter(created_at__gte=start_date)
                    .extra(select={'day': 'date(created_at)'})
                    .values('day')
                    .annotate(count=Count('id'))
                    .order_by('day')
                ),
                'daily_history': list(
                    DataRecordHistory.objects.filter(timestamp__gte=start_date)
                    .extra(select={'day': 'date(timestamp)'})
                    .values('day')
                    .annotate(count=Count('id'))
                    .order_by('day')
                )
            }
            
            return Response(trends, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Trend analytics error: {str(e)}")
            return Response({
                'error': str(e),
                'timestamp': timezone.now()
            }, status=status.HTTP_400_BAD_REQUEST)


class PerformanceStatsView(APIView):
    """Performance and system metrics."""
    
    def get(self, request, format=None):
        """Get performance statistics."""
        try:
            # Query performance metrics
            recent_queries = QueryLog.objects.filter(
                timestamp__gte=timezone.now() - timedelta(hours=24)
            ).order_by('-timestamp')[:100]
            
            if recent_queries:
                avg_execution_time = sum(q.execution_time for q in recent_queries) / len(recent_queries)
                max_execution_time = max(q.execution_time for q in recent_queries)
                min_execution_time = min(q.execution_time for q in recent_queries)
            else:
                avg_execution_time = max_execution_time = min_execution_time = 0
            
            performance = {
                'query_performance': {
                    'avg_execution_time': avg_execution_time,
                    'max_execution_time': max_execution_time,
                    'min_execution_time': min_execution_time,
                    'total_queries_24h': len(recent_queries)
                },
                'database_size': {
                    'total_records': DataRecord.objects.count(),
                    'total_unstructured': UnstructuredData.objects.count(),
                    'total_history_entries': DataRecordHistory.objects.count()
                }
            }
            
            return Response(performance, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Performance stats error: {str(e)}")
            return Response({
                'error': str(e),
                'timestamp': timezone.now()
            }, status=status.HTTP_400_BAD_REQUEST)


class IngestionJobListView(generics.ListAPIView):
    """List data ingestion jobs."""
    queryset = DataIngestionJob.objects.all().order_by('-started_at')
    serializer_class = DataIngestionResponseSerializer  # Reuse for simplicity
    pagination_class = StandardResultsSetPagination


class IngestionJobDetailView(generics.RetrieveAPIView):
    """Get details of a specific ingestion job."""
    queryset = DataIngestionJob.objects.all()
    serializer_class = DataIngestionResponseSerializer


class SystemHistoryView(APIView):
    """System-wide change history."""
    
    def get(self, request, format=None):
        """Get system-wide change history."""
        try:
            limit = int(request.GET.get('limit', 100))
            operation = request.GET.get('operation', 'ALL')
            
            history_query = DataRecordHistory.objects.all()
            
            if operation != 'ALL':
                history_query = history_query.filter(operation=operation)
            
            history = history_query.order_by('-timestamp')[:limit]
            serializer = DataRecordHistorySerializer(history, many=True)
            
            return Response({
                'history': serializer.data,
                'total_entries': len(serializer.data)
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"System history error: {str(e)}")
            return Response({
                'error': str(e),
                'timestamp': timezone.now()
            }, status=status.HTTP_400_BAD_REQUEST)


class ChangeHistoryView(APIView):
    """Change history with advanced filtering."""
    
    def get(self, request, format=None):
        """Get filtered change history."""
        try:
            schema_id = request.GET.get('schema_id')
            start_date = request.GET.get('start_date')
            end_date = request.GET.get('end_date')
            operation = request.GET.get('operation', 'ALL')
            limit = int(request.GET.get('limit', 100))
            
            history_query = DataRecordHistory.objects.all()
            
            if schema_id:
                history_query = history_query.filter(schema_id=schema_id)
            
            if start_date:
                history_query = history_query.filter(timestamp__gte=start_date)
            
            if end_date:
                history_query = history_query.filter(timestamp__lte=end_date)
            
            if operation != 'ALL':
                history_query = history_query.filter(operation=operation)
            
            history = history_query.order_by('-timestamp')[:limit]
            serializer = DataRecordHistorySerializer(history, many=True)
            
            return Response({
                'filters': {
                    'schema_id': schema_id,
                    'start_date': start_date,
                    'end_date': end_date,
                    'operation': operation
                },
                'history': serializer.data,
                'total_entries': len(serializer.data)
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Change history error: {str(e)}")
            return Response({
                'error': str(e),
                'timestamp': timezone.now()
            }, status=status.HTTP_400_BAD_REQUEST)


class SchemaExportView(APIView):
    """Export schema and related data."""
    
    def get(self, request, schema_id, format=None):
        """Export schema definition and sample data."""
        try:
            schema = DataSchema.objects.get(id=schema_id, is_active=True)
            records = DataRecord.objects.filter(schema=schema, is_active=True)[:100]
            
            export_data = {
                'schema': DataSchemaSerializer(schema).data,
                'sample_records': DataRecordSerializer(records, many=True).data,
                'export_timestamp': timezone.now(),
                'total_records': DataRecord.objects.filter(schema=schema, is_active=True).count()
            }
            
            return Response(export_data, status=status.HTTP_200_OK)
            
        except DataSchema.DoesNotExist:
            return Response({
                'error': f'Schema with ID {schema_id} not found',
                'timestamp': timezone.now()
            }, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            logger.error(f"Schema export error: {str(e)}")
            return Response({
                'error': str(e),
                'timestamp': timezone.now()
            }, status=status.HTTP_400_BAD_REQUEST)


class RecordExportView(APIView):
    """Export records with filtering."""
    
    def get(self, request, format=None):
        """Export filtered records."""
        try:
            schema_name = request.GET.get('schema')
            start_date = request.GET.get('start_date')
            end_date = request.GET.get('end_date')
            limit = int(request.GET.get('limit', 1000))
            
            records_query = DataRecord.objects.filter(is_active=True)
            
            if schema_name:
                records_query = records_query.filter(schema__name=schema_name)
            
            if start_date:
                records_query = records_query.filter(created_at__gte=start_date)
            
            if end_date:
                records_query = records_query.filter(created_at__lte=end_date)
            
            records = records_query.order_by('-created_at')[:limit]
            
            export_data = {
                'filters': {
                    'schema': schema_name,
                    'start_date': start_date,
                    'end_date': end_date,
                    'limit': limit
                },
                'records': DataRecordSerializer(records, many=True).data,
                'export_timestamp': timezone.now(),
                'total_exported': len(records)
            }
            
            return Response(export_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Record export error: {str(e)}")
            return Response({
                'error': str(e),
                'timestamp': timezone.now()
            }, status=status.HTTP_400_BAD_REQUEST)
