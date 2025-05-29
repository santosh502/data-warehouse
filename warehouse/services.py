"""
Service layer for data warehouse operations.
Implements business logic for data ingestion, querying, and change tracking.
"""

import json
import csv
import io
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from django.db import transaction, connection
from django.contrib.auth.models import User
from django.contrib.postgres.search import SearchVector, SearchQuery, SearchRank
from django.core.exceptions import ValidationError
from django.utils import timezone

from .models import (
    DataSchema, DataRecord, DataRecordHistory, UnstructuredData,
    QueryLog, DataIngestionJob, UserProfile, Address, Income, Goal
)

logger = logging.getLogger(__name__)


class DataIngestionService:
    """
    Service for handling data ingestion operations.
    """

    @staticmethod
    def create_schema(name: str, description: str, schema_definition: Dict, user: Optional[User] = None) -> DataSchema:
        """
        Create a new data schema definition.
        """
        try:
            # Validate schema definition (simplified)
            if not isinstance(schema_definition, dict):
                raise ValidationError("Schema definition must be a valid JSON object")
            
            schema = DataSchema.objects.create(
                name=name,
                description=description,
                schema_definition=schema_definition
            )
            
            logger.info(f"Created schema '{name}' with version {schema.version}")
            return schema
            
        except Exception as e:
            logger.error(f"Error creating schema '{name}': {str(e)}")
            raise

    @staticmethod
    def ingest_structured_data(
        schema_name: str, 
        data_list: List[Dict], 
        source_file: Optional[str] = None,
        user: Optional[User] = None
    ) -> Tuple[int, int, List[str]]:
        """
        Ingest structured data records in bulk.
        Returns: (success_count, error_count, error_messages)
        """
        try:
            schema = DataSchema.objects.get(name=schema_name, is_active=True)
        except DataSchema.DoesNotExist:
            raise ValidationError(f"Schema '{schema_name}' not found or inactive")

        success_count = 0
        error_count = 0
        error_messages = []

        with transaction.atomic():
            for i, data in enumerate(data_list):
                try:
                    # Validate data against schema (simplified validation)
                    if not isinstance(data, dict):
                        raise ValidationError(f"Record {i}: Data must be a JSON object")
                    
                    record = DataRecord.objects.create(
                        schema=schema,
                        data=data,
                        source_file=source_file,
                        source_line=i + 1,
                        created_by=user
                    )
                    
                    # Create history entry for INSERT operation
                    DataRecordHistoryService.create_history_entry(
                        record_id=record.id,
                        schema=schema,
                        operation='INSERT',
                        new_data=data,
                        user=user
                    )
                    
                    success_count += 1
                    
                except Exception as e:
                    error_count += 1
                    error_msg = f"Record {i}: {str(e)}"
                    error_messages.append(error_msg)
                    logger.warning(error_msg)

        logger.info(f"Ingested {success_count} records, {error_count} errors for schema '{schema_name}'")
        return success_count, error_count, error_messages

    @staticmethod
    def ingest_csv_file(file_content: str, schema_name: str, user: Optional[User] = None) -> Dict:
        """
        Ingest data from CSV file content.
        """
        try:
            # Parse CSV
            csv_reader = csv.DictReader(io.StringIO(file_content))
            data_list = list(csv_reader)
            
            if not data_list:
                raise ValidationError("CSV file is empty or has no valid data")
            
            # Ingest data
            success_count, error_count, error_messages = DataIngestionService.ingest_structured_data(
                schema_name=schema_name,
                data_list=data_list,
                source_file="uploaded_csv",
                user=user
            )
            
            return {
                'success': True,
                'total_records': len(data_list),
                'success_count': success_count,
                'error_count': error_count,
                'error_messages': error_messages[:10]  # Limit error messages
            }
            
        except Exception as e:
            logger.error(f"CSV ingestion error: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

    @staticmethod
    def ingest_json_file(file_content: str, schema_name: str, user: Optional[User] = None) -> Dict:
        """
        Ingest data from JSON file content.
        """
        try:
            # Parse JSON
            json_data = json.loads(file_content)
            
            # Handle both single object and array of objects
            if isinstance(json_data, dict):
                data_list = [json_data]
            elif isinstance(json_data, list):
                data_list = json_data
            else:
                raise ValidationError("JSON must be an object or array of objects")
            
            # Ingest data
            success_count, error_count, error_messages = DataIngestionService.ingest_structured_data(
                schema_name=schema_name,
                data_list=data_list,
                source_file="uploaded_json",
                user=user
            )
            
            return {
                'success': True,
                'total_records': len(data_list),
                'success_count': success_count,
                'error_count': error_count,
                'error_messages': error_messages[:10]
            }
            
        except json.JSONDecodeError as e:
            return {
                'success': False,
                'error': f"Invalid JSON format: {str(e)}"
            }
        except Exception as e:
            logger.error(f"JSON ingestion error: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

    @staticmethod
    def ingest_unstructured_data(
        content: str,
        title: Optional[str] = None,
        data_type: str = 'TEXT',
        metadata: Optional[Dict] = None,
        tags: Optional[List[str]] = None,
        user: Optional[User] = None
    ) -> UnstructuredData:
        """
        Ingest unstructured data.
        """
        try:
            unstructured_data = UnstructuredData.objects.create(
                title=title or '',
                content=content,
                data_type=data_type,
                metadata=metadata or {},
                tags=tags or [],
                created_by=user
            )
            
            logger.info(f"Created unstructured data record {unstructured_data.id}")
            return unstructured_data
            
        except Exception as e:
            logger.error(f"Error creating unstructured data: {str(e)}")
            raise


class DataRecordHistoryService:
    """
    Service for managing data record change history.
    """

    @staticmethod
    def create_history_entry(
        record_id: str,
        schema: DataSchema,
        operation: str,
        old_data: Optional[Dict] = None,
        new_data: Optional[Dict] = None,
        changed_fields: Optional[List[str]] = None,
        user: Optional[User] = None,
        ip_address: Optional[str] = None
    ) -> DataRecordHistory:
        """
        Create a history entry for a data record change.
        """
        try:
            history_entry = DataRecordHistory.objects.create(
                record_id=record_id,
                schema=schema,
                operation=operation,
                old_data=old_data,
                new_data=new_data,
                changed_fields=changed_fields or [],
                changed_by=user,
                ip_address=ip_address
            )
            
            logger.debug(f"Created history entry for record {record_id}: {operation}")
            return history_entry
            
        except Exception as e:
            logger.error(f"Error creating history entry: {str(e)}")
            raise

    @staticmethod
    def update_record_with_history(
        record_id: str,
        new_data: Dict,
        user: Optional[User] = None,
        ip_address: Optional[str] = None
    ) -> DataRecord:
        """
        Update a data record and create history entry.
        """
        try:
            record = DataRecord.objects.get(id=record_id, is_active=True)
            old_data = record.data.copy()
            
            # Find changed fields
            changed_fields = []
            for key in set(list(old_data.keys()) + list(new_data.keys())):
                if old_data.get(key) != new_data.get(key):
                    changed_fields.append(key)
            
            # Update record
            record.data = new_data
            record.updated_at = timezone.now()
            record.save()
            
            # Create history entry
            DataRecordHistoryService.create_history_entry(
                record_id=record.id,
                schema=record.schema,
                operation='UPDATE',
                old_data=old_data,
                new_data=new_data,
                changed_fields=changed_fields,
                user=user,
                ip_address=ip_address
            )
            
            logger.info(f"Updated record {record_id} with {len(changed_fields)} changed fields")
            return record
            
        except DataRecord.DoesNotExist:
            raise ValidationError(f"Record {record_id} not found or inactive")
        except Exception as e:
            logger.error(f"Error updating record {record_id}: {str(e)}")
            raise

    @staticmethod
    def get_record_history(record_id: str, limit: int = 50) -> List[DataRecordHistory]:
        """
        Get change history for a specific record.
        """
        try:
            return list(
                DataRecordHistory.objects
                .filter(record_id=record_id)
                .order_by('-timestamp')[:limit]
            )
        except Exception as e:
            logger.error(f"Error fetching history for record {record_id}: {str(e)}")
            return []


class QueryService:
    """
    Service for handling advanced queries and search operations.
    """

    @staticmethod
    def search_structured_data(
        query: str,
        schema_name: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Dict:
        """
        Search structured data using JSONB queries.
        """
        try:
            start_time = timezone.now()
            
            queryset = DataRecord.objects.filter(is_active=True)
            
            if schema_name:
                queryset = queryset.filter(schema__name=schema_name)
            
            # JSONB search
            queryset = queryset.filter(data__icontains=query)
            
            total_count = queryset.count()
            results = list(queryset[offset:offset + limit])
            
            execution_time = (timezone.now() - start_time).total_seconds()
            
            # Log query
            QueryLog.objects.create(
                query_type='structured_search',
                query_params={'query': query, 'schema': schema_name},
                execution_time=execution_time,
                result_count=total_count
            )
            
            return {
                'results': results,
                'total_count': total_count,
                'execution_time': execution_time
            }
            
        except Exception as e:
            logger.error(f"Structured search error: {str(e)}")
            return {'results': [], 'total_count': 0, 'error': str(e)}

    @staticmethod
    def search_unstructured_data(
        query: str,
        data_type: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Dict:
        """
        Search unstructured data using full-text search.
        """
        try:
            start_time = timezone.now()
            
            search_query = SearchQuery(query)
            search_vector = SearchVector('content', 'title')
            
            queryset = (
                UnstructuredData.objects
                .filter(is_active=True)
                .annotate(
                    search=search_vector,
                    rank=SearchRank(search_vector, search_query)
                )
                .filter(search=search_query)
            )
            
            if data_type:
                queryset = queryset.filter(data_type=data_type)
            
            queryset = queryset.order_by('-rank')
            
            total_count = queryset.count()
            results = list(queryset[offset:offset + limit])
            
            execution_time = (timezone.now() - start_time).total_seconds()
            
            # Log query
            QueryLog.objects.create(
                query_type='unstructured_search',
                query_params={'query': query, 'data_type': data_type},
                execution_time=execution_time,
                result_count=total_count
            )
            
            return {
                'results': results,
                'total_count': total_count,
                'execution_time': execution_time
            }
            
        except Exception as e:
            logger.error(f"Unstructured search error: {str(e)}")
            return {'results': [], 'total_count': 0, 'error': str(e)}

    @staticmethod
    def aggregate_data(
        aggregation_type: str,
        schema_name: Optional[str] = None,
        time_period: str = '30d',
        group_by: Optional[str] = None
    ) -> Dict:
        """
        Perform data aggregation operations.
        """
        try:
            start_time = timezone.now()
            
            # Time period mapping
            time_map = {
                '1d': 1, '7d': 7, '30d': 30, '90d': 90, '1y': 365
            }
            days = time_map.get(time_period, 30)
            time_filter = timezone.now() - timezone.timedelta(days=days)
            
            results = {}
            
            if aggregation_type == 'record_count_by_schema':
                queryset = DataRecord.objects.filter(
                    is_active=True,
                    created_at__gte=time_filter
                )
                if schema_name:
                    queryset = queryset.filter(schema__name=schema_name)
                
                results = list(
                    queryset.values('schema__name')
                    .annotate(count=models.Count('id'))
                    .order_by('-count')
                )
            
            elif aggregation_type == 'daily_ingestion':
                with connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT DATE(created_at) as day, COUNT(*) as count
                        FROM data_record
                        WHERE is_active = true AND created_at >= %s
                        GROUP BY DATE(created_at)
                        ORDER BY day
                    """, [time_filter])
                    
                    results = [
                        {'day': row[0].isoformat(), 'count': row[1]}
                        for row in cursor.fetchall()
                    ]
            
            execution_time = (timezone.now() - start_time).total_seconds()
            
            # Log query
            QueryLog.objects.create(
                query_type='aggregation',
                query_params={
                    'type': aggregation_type,
                    'schema': schema_name,
                    'period': time_period
                },
                execution_time=execution_time,
                result_count=len(results) if isinstance(results, list) else 1
            )
            
            return {
                'aggregation_type': aggregation_type,
                'results': results,
                'execution_time': execution_time
            }
            
        except Exception as e:
            logger.error(f"Aggregation error: {str(e)}")
            return {'results': [], 'error': str(e)}


class UserProfileService:
    """
    Service for handling user profile operations (example structured data).
    """

    @staticmethod
    def create_user_profile(profile_data: Dict) -> UserProfile:
        """
        Create a complete user profile with related data.
        """
        try:
            with transaction.atomic():
                # Extract nested data
                addresses_data = profile_data.pop('addresses', [])
                incomes_data = profile_data.pop('incomes', [])
                goals_data = profile_data.pop('goals', [])
                
                # Create profile
                profile = UserProfile.objects.create(**profile_data)
                
                # Create related objects
                for addr_data in addresses_data:
                    Address.objects.create(profile=profile, **addr_data)
                
                for income_data in incomes_data:
                    Income.objects.create(profile=profile, **income_data)
                
                for goal_data in goals_data:
                    Goal.objects.create(profile=profile, **goal_data)
                
                logger.info(f"Created user profile {profile.id}")
                return profile
                
        except Exception as e:
            logger.error(f"Error creating user profile: {str(e)}")
            raise

    @staticmethod
    def search_profiles(query: str, limit: int = 50) -> List[UserProfile]:
        """
        Search user profiles with full-text search on goals.
        """
        try:
            # Search in profile names and goals
            profiles = UserProfile.objects.filter(
                models.Q(first_name__icontains=query) |
                models.Q(last_name__icontains=query) |
                models.Q(goals__aim__search=query)
            ).distinct()[:limit]
            
            return list(profiles)
            
        except Exception as e:
            logger.error(f"Profile search error: {str(e)}")
            return []
