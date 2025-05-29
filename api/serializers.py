"""
API serializers for the data warehouse REST API endpoints.
"""

from rest_framework import serializers
from warehouse.models import (
    DataSchema, DataRecord, DataRecordHistory, UnstructuredData,
    QueryLog, DataIngestionJob, UserProfile, Address, Income, Goal
)
from warehouse.serializers import (
    DataSchemaSerializer as WarehouseDataSchemaSerializer,
    DataRecordSerializer as WarehouseDataRecordSerializer,
    DataRecordHistorySerializer as WarehouseDataRecordHistorySerializer,
    UnstructuredDataSerializer as WarehouseUnstructuredDataSerializer,
    UserProfileSerializer as WarehouseUserProfileSerializer,
    UserProfileCreateSerializer as WarehouseUserProfileCreateSerializer
)


class DataIngestionRequestSerializer(serializers.Serializer):
    """Serializer for bulk data ingestion requests."""
    schema_name = serializers.CharField(max_length=100)
    data = serializers.ListField(
        child=serializers.JSONField(),
        min_length=1,
        max_length=10000,
        help_text="Array of JSON objects to ingest"
    )
    source_file = serializers.CharField(
        max_length=255, 
        required=False,
        help_text="Optional source file identifier"
    )
    
    def validate_schema_name(self, value):
        """Validate that the schema exists and is active."""
        try:
            DataSchema.objects.get(name=value, is_active=True)
        except DataSchema.DoesNotExist:
            raise serializers.ValidationError(f"Schema '{value}' does not exist or is inactive")
        return value


class DataIngestionResponseSerializer(serializers.Serializer):
    """Serializer for data ingestion response."""
    success = serializers.BooleanField()
    total_records = serializers.IntegerField()
    success_count = serializers.IntegerField()
    error_count = serializers.IntegerField()
    error_messages = serializers.ListField(
        child=serializers.CharField(),
        required=False
    )
    job_id = serializers.UUIDField(required=False)


class UnstructuredDataIngestionSerializer(serializers.Serializer):
    """Serializer for unstructured data ingestion."""
    title = serializers.CharField(max_length=255, required=False, allow_blank=True)
    content = serializers.CharField(min_length=1)
    data_type = serializers.ChoiceField(
        choices=['TEXT', 'JSON', 'XML', 'MIXED'],
        default='TEXT'
    )
    metadata = serializers.JSONField(default=dict, required=False)
    tags = serializers.ListField(
        child=serializers.CharField(max_length=100),
        default=list,
        required=False
    )
    related_record_id = serializers.UUIDField(required=False, allow_null=True)


class FileUploadSerializer(serializers.Serializer):
    """Serializer for file upload operations."""
    file = serializers.FileField()
    schema_name = serializers.CharField(max_length=100)
    
    def validate_file(self, value):
        """Validate file type and size."""
        # Check file size (50MB limit)
        if value.size > 50 * 1024 * 1024:
            raise serializers.ValidationError("File size cannot exceed 50MB")
        
        # Check file extension
        allowed_extensions = ['.csv', '.json', '.txt']
        file_extension = value.name.lower().split('.')[-1]
        if f'.{file_extension}' not in allowed_extensions:
            raise serializers.ValidationError(
                f"File type not supported. Allowed types: {', '.join(allowed_extensions)}"
            )
        
        return value


class SearchRequestSerializer(serializers.Serializer):
    """Serializer for search requests."""
    query = serializers.CharField(max_length=500, min_length=1)
    schema = serializers.CharField(max_length=100, required=False, allow_blank=True)
    data_type = serializers.ChoiceField(
        choices=['structured', 'unstructured', 'all'],
        default='all'
    )
    limit = serializers.IntegerField(min_value=1, max_value=100, default=50)
    offset = serializers.IntegerField(min_value=0, default=0)


class SearchResultSerializer(serializers.Serializer):
    """Serializer for search results."""
    id = serializers.UUIDField()
    type = serializers.CharField()
    title = serializers.CharField(required=False, allow_null=True)
    content = serializers.CharField(required=False, allow_null=True)
    data = serializers.JSONField(required=False, allow_null=True)
    schema = serializers.CharField(required=False, allow_null=True)
    metadata = serializers.JSONField(required=False, allow_null=True)
    tags = serializers.ListField(required=False, allow_null=True)
    created_at = serializers.DateTimeField()
    relevance = serializers.FloatField(required=False, allow_null=True)


class SearchResponseSerializer(serializers.Serializer):
    """Serializer for search response."""
    query = serializers.CharField()
    total_count = serializers.IntegerField()
    execution_time = serializers.FloatField()
    results = SearchResultSerializer(many=True)


class AggregationRequestSerializer(serializers.Serializer):
    """Serializer for aggregation requests."""
    type = serializers.ChoiceField(
        choices=[
            'record_count_by_schema',
            'daily_ingestion',
            'change_operations',
            'unstructured_by_type',
            'schema_usage_trend'
        ],
        default='record_count_by_schema'
    )
    schema = serializers.CharField(max_length=100, required=False, allow_blank=True)
    period = serializers.ChoiceField(
        choices=['1d', '7d', '30d', '90d', '1y'],
        default='30d'
    )
    group_by = serializers.CharField(max_length=50, required=False, allow_blank=True)


class AggregationResultSerializer(serializers.Serializer):
    """Serializer for aggregation results."""
    aggregation_type = serializers.CharField()
    period = serializers.CharField()
    execution_time = serializers.FloatField()
    results = serializers.ListField(child=serializers.JSONField())


class SchemaCreationSerializer(serializers.Serializer):
    """Serializer for creating new data schemas."""
    name = serializers.CharField(max_length=100)
    description = serializers.CharField(required=False, allow_blank=True)
    schema_definition = serializers.JSONField()
    
    def validate_name(self, value):
        """Validate schema name uniqueness."""
        if DataSchema.objects.filter(name=value).exists():
            raise serializers.ValidationError(f"Schema with name '{value}' already exists")
        return value
    
    def validate_schema_definition(self, value):
        """Validate schema definition format."""
        if not isinstance(value, dict):
            raise serializers.ValidationError("Schema definition must be a JSON object")
        
        # Basic validation - in production, you'd use jsonschema
        required_fields = ['type', 'properties']
        for field in required_fields:
            if field not in value:
                raise serializers.ValidationError(f"Schema definition must include '{field}'")
        
        return value


class RecordUpdateSerializer(serializers.Serializer):
    """Serializer for updating data records."""
    data = serializers.JSONField()
    
    def validate_data(self, value):
        """Validate updated data format."""
        if not isinstance(value, dict):
            raise serializers.ValidationError("Data must be a JSON object")
        return value


class HistoryQuerySerializer(serializers.Serializer):
    """Serializer for history queries."""
    record_id = serializers.UUIDField()
    limit = serializers.IntegerField(min_value=1, max_value=100, default=50)
    operation = serializers.ChoiceField(
        choices=['INSERT', 'UPDATE', 'DELETE', 'ALL'],
        default='ALL',
        required=False
    )


class SystemStatsSerializer(serializers.Serializer):
    """Serializer for system statistics."""
    overview = serializers.DictField()
    recent_activity = serializers.DictField()
    schema_distribution = serializers.ListField(child=serializers.DictField())
    daily_ingestion = serializers.ListField(child=serializers.DictField())
    change_activity = serializers.ListField(child=serializers.DictField())
    performance_metrics = serializers.DictField(required=False)


class HealthCheckSerializer(serializers.Serializer):
    """Serializer for health check response."""
    status = serializers.CharField()
    timestamp = serializers.DateTimeField()
    version = serializers.CharField()
    database_status = serializers.CharField()
    total_records = serializers.IntegerField()
    total_schemas = serializers.IntegerField()
    uptime = serializers.CharField()


class ErrorResponseSerializer(serializers.Serializer):
    """Serializer for error responses."""
    error = serializers.CharField()
    details = serializers.CharField(required=False, allow_null=True)
    timestamp = serializers.DateTimeField()
    request_id = serializers.CharField(required=False, allow_null=True)


# Re-export warehouse serializers for consistency
DataSchemaSerializer = WarehouseDataSchemaSerializer
DataRecordSerializer = WarehouseDataRecordSerializer
DataRecordHistorySerializer = WarehouseDataRecordHistorySerializer
UnstructuredDataSerializer = WarehouseUnstructuredDataSerializer
UserProfileSerializer = WarehouseUserProfileSerializer
UserProfileCreateSerializer = WarehouseUserProfileCreateSerializer
