"""
Serializers for the warehouse data models.
"""

from rest_framework import serializers
from .models import (
    DataSchema, DataRecord, DataRecordHistory, UnstructuredData,
    QueryLog, DataIngestionJob, UserProfile, Address, Income, Goal
)


class DataSchemaSerializer(serializers.ModelSerializer):
    """Serializer for data schema definitions."""
    
    class Meta:
        model = DataSchema
        fields = '__all__'
        read_only_fields = ('created_at', 'updated_at')


class DataRecordSerializer(serializers.ModelSerializer):
    """Serializer for structured data records."""
    schema_name = serializers.CharField(source='schema.name', read_only=True)
    
    class Meta:
        model = DataRecord
        fields = '__all__'
        read_only_fields = ('id', 'created_at', 'updated_at', 'created_by')

    def validate_data(self, value):
        """
        Validate that the data conforms to the schema definition.
        This is a simplified validation - in production, you'd use jsonschema.
        """
        if not isinstance(value, dict):
            raise serializers.ValidationError("Data must be a valid JSON object")
        return value


class DataRecordHistorySerializer(serializers.ModelSerializer):
    """Serializer for data record change history."""
    schema_name = serializers.CharField(source='schema.name', read_only=True)
    changed_by_username = serializers.CharField(source='changed_by.username', read_only=True)
    
    class Meta:
        model = DataRecordHistory
        fields = '__all__'
        read_only_fields = ('id', 'timestamp')


class UnstructuredDataSerializer(serializers.ModelSerializer):
    """Serializer for unstructured data."""
    related_record_id = serializers.UUIDField(source='related_record.id', read_only=True)
    
    class Meta:
        model = UnstructuredData
        fields = '__all__'
        read_only_fields = ('id', 'created_at', 'updated_at', 'created_by')


class QueryLogSerializer(serializers.ModelSerializer):
    """Serializer for query logs."""
    user_username = serializers.CharField(source='user.username', read_only=True)
    
    class Meta:
        model = QueryLog
        fields = '__all__'
        read_only_fields = ('id', 'timestamp')


class DataIngestionJobSerializer(serializers.ModelSerializer):
    """Serializer for data ingestion jobs."""
    schema_name = serializers.CharField(source='schema.name', read_only=True)
    created_by_username = serializers.CharField(source='created_by.username', read_only=True)
    
    class Meta:
        model = DataIngestionJob
        fields = '__all__'
        read_only_fields = ('id',)


class AddressSerializer(serializers.ModelSerializer):
    """Serializer for address data."""
    
    class Meta:
        model = Address
        fields = '__all__'
        read_only_fields = ('id',)


class IncomeSerializer(serializers.ModelSerializer):
    """Serializer for income data."""
    
    class Meta:
        model = Income
        fields = '__all__'
        read_only_fields = ('id', 'created_at')


class GoalSerializer(serializers.ModelSerializer):
    """Serializer for goals data."""
    
    class Meta:
        model = Goal
        fields = '__all__'
        read_only_fields = ('id', 'created_at')


class UserProfileSerializer(serializers.ModelSerializer):
    """Serializer for user profiles with nested relationships."""
    addresses = AddressSerializer(many=True, read_only=True)
    incomes = IncomeSerializer(many=True, read_only=True)
    goals = GoalSerializer(many=True, read_only=True)
    full_name = serializers.SerializerMethodField()
    
    class Meta:
        model = UserProfile
        fields = '__all__'
        read_only_fields = ('id', 'created_at', 'updated_at')
    
    def get_full_name(self, obj):
        return f"{obj.first_name} {obj.last_name}"


class UserProfileCreateSerializer(serializers.ModelSerializer):
    """Serializer for creating user profiles."""
    addresses = AddressSerializer(many=True, required=False)
    incomes = IncomeSerializer(many=True, required=False)
    goals = GoalSerializer(many=True, required=False)
    
    class Meta:
        model = UserProfile
        fields = ['title', 'title_other', 'first_name', 'middle_name', 
                 'last_name', 'age', 'addresses', 'incomes', 'goals']
    
    def create(self, validated_data):
        addresses_data = validated_data.pop('addresses', [])
        incomes_data = validated_data.pop('incomes', [])
        goals_data = validated_data.pop('goals', [])
        
        profile = UserProfile.objects.create(**validated_data)
        
        for address_data in addresses_data:
            Address.objects.create(profile=profile, **address_data)
        
        for income_data in incomes_data:
            Income.objects.create(profile=profile, **income_data)
        
        for goal_data in goals_data:
            Goal.objects.create(profile=profile, **goal_data)
        
        return profile


class BulkDataIngestSerializer(serializers.Serializer):
    """Serializer for bulk data ingestion."""
    schema_name = serializers.CharField(max_length=100)
    data = serializers.ListField(
        child=serializers.JSONField(),
        min_length=1,
        max_length=10000  # Limit batch size
    )
    source_file = serializers.CharField(max_length=255, required=False)
    
    def validate_schema_name(self, value):
        try:
            DataSchema.objects.get(name=value, is_active=True)
        except DataSchema.DoesNotExist:
            raise serializers.ValidationError(f"Schema '{value}' does not exist or is inactive")
        return value


class SearchQuerySerializer(serializers.Serializer):
    """Serializer for search queries."""
    query = serializers.CharField(max_length=500)
    schema = serializers.CharField(max_length=100, required=False)
    data_type = serializers.ChoiceField(
        choices=['structured', 'unstructured', 'all'],
        default='all'
    )
    limit = serializers.IntegerField(min_value=1, max_value=100, default=50)
    offset = serializers.IntegerField(min_value=0, default=0)


class AggregationQuerySerializer(serializers.Serializer):
    """Serializer for aggregation queries."""
    type = serializers.ChoiceField(
        choices=['schema_count', 'daily_trend', 'change_operations'],
        default='schema_count'
    )
    schema = serializers.CharField(max_length=100, required=False)
    period = serializers.ChoiceField(
        choices=['1d', '7d', '30d', '90d', '1y'],
        default='30d'
    )
    group_by = serializers.CharField(max_length=50, required=False)
