"""
Data warehouse models for storing structured and unstructured data with change tracking.
"""

from django.db import models
from django.contrib.postgres.indexes import GinIndex
from django.utils import timezone
from django.contrib.auth.models import User
import uuid


class DataSchema(models.Model):
    """
    Defines schema definitions for structured data types.
    """
    name = models.CharField(max_length=100, unique=True)
    description = models.TextField(blank=True)
    schema_definition = models.JSONField(help_text="JSON schema definition")
    version = models.IntegerField(default=1)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = 'data_schema'
        indexes = [
            models.Index(fields=['name', 'version']),
        ]

    def __str__(self):
        return f"{self.name} v{self.version}"


class DataRecord(models.Model):
    """
    Main table for storing structured data records with horizontal scaling support.
    Uses UUID for distributed system compatibility.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    schema = models.ForeignKey(DataSchema, on_delete=models.CASCADE, related_name='records')
    data = models.JSONField(help_text="Structured data conforming to schema")
    source_file = models.CharField(max_length=255, blank=True, null=True)
    source_line = models.IntegerField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = 'data_record'
        indexes = [
            # GIN index for JSONB queries
            GinIndex(fields=['data']),
            models.Index(fields=['schema', 'created_at']),
            models.Index(fields=['created_at']),
            models.Index(fields=['is_active']),
        ]

    def __str__(self):
        return f"Record {self.id} ({self.schema.name})"


class DataRecordHistory(models.Model):
    """
    Audit trail for tracking all changes to structured data records.
    Implements temporal data pattern for historical analysis.
    """
    OPERATION_CHOICES = [
        ('INSERT', 'Insert'),
        ('UPDATE', 'Update'),
        ('DELETE', 'Delete'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    record_id = models.UUIDField(db_index=True)  # Reference to original record
    schema = models.ForeignKey(DataSchema, on_delete=models.CASCADE)
    operation = models.CharField(max_length=10, choices=OPERATION_CHOICES)
    old_data = models.JSONField(null=True, blank=True, help_text="Previous data state")
    new_data = models.JSONField(null=True, blank=True, help_text="New data state")
    changed_fields = models.JSONField(null=True, blank=True, help_text="List of changed field names")
    timestamp = models.DateTimeField(auto_now_add=True)
    changed_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    user_agent = models.TextField(blank=True)

    class Meta:
        db_table = 'data_record_history'
        indexes = [
            models.Index(fields=['record_id', 'timestamp']),
            models.Index(fields=['schema', 'timestamp']),
            models.Index(fields=['operation', 'timestamp']),
            models.Index(fields=['timestamp']),
            GinIndex(fields=['old_data']),
            GinIndex(fields=['new_data']),
        ]

    def __str__(self):
        return f"{self.operation} on {self.record_id} at {self.timestamp}"


class UnstructuredData(models.Model):
    """
    Storage for unstructured and semi-structured data with full-text search support.
    """
    DATA_TYPE_CHOICES = [
        ('TEXT', 'Free-form Text'),
        ('JSON', 'Semi-structured JSON'),
        ('XML', 'XML Document'),
        ('MIXED', 'Mixed Content'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    title = models.CharField(max_length=255, blank=True)
    content = models.TextField(help_text="Unstructured content")
    data_type = models.CharField(max_length=10, choices=DATA_TYPE_CHOICES, default='TEXT')
    metadata = models.JSONField(default=dict, blank=True, help_text="Additional metadata")
    tags = models.JSONField(default=list, blank=True, help_text="Tags for categorization")
    source_file = models.CharField(max_length=255, blank=True, null=True)
    related_record = models.ForeignKey(DataRecord, on_delete=models.SET_NULL, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = 'unstructured_data'
        indexes = [
            # Full-text search index
            GinIndex(fields=['content'], name='unstructured_content_gin_idx', opclasses=['gin_trgm_ops']),
            GinIndex(fields=['metadata'], name='unstructured_metadata_gin_idx'),
            GinIndex(fields=['tags'], name='unstructured_tags_gin_idx'),
            models.Index(fields=['data_type', 'created_at']),
            models.Index(fields=['created_at']),
        ]

    def __str__(self):
        return f"Unstructured: {self.title or self.id}"


class QueryLog(models.Model):
    """
    Logs all queries for performance monitoring and analytics.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    query_type = models.CharField(max_length=50)
    query_params = models.JSONField(default=dict)
    execution_time = models.FloatField(help_text="Execution time in seconds")
    result_count = models.IntegerField()
    user = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    timestamp = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'query_log'
        indexes = [
            models.Index(fields=['query_type', 'timestamp']),
            models.Index(fields=['user', 'timestamp']),
            models.Index(fields=['timestamp']),
        ]

    def __str__(self):
        return f"{self.query_type} query at {self.timestamp}"


class DataIngestionJob(models.Model):
    """
    Tracks data ingestion jobs for monitoring and debugging.
    """
    STATUS_CHOICES = [
        ('PENDING', 'Pending'),
        ('PROCESSING', 'Processing'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
        ('CANCELLED', 'Cancelled'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    job_name = models.CharField(max_length=255)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDING')
    schema = models.ForeignKey(DataSchema, on_delete=models.CASCADE, null=True, blank=True)
    file_name = models.CharField(max_length=255, blank=True)
    total_records = models.IntegerField(default=0)
    processed_records = models.IntegerField(default=0)
    failed_records = models.IntegerField(default=0)
    error_log = models.TextField(blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)

    class Meta:
        db_table = 'data_ingestion_job'
        indexes = [
            models.Index(fields=['status', 'started_at']),
            models.Index(fields=['created_by', 'started_at']),
        ]

    def __str__(self):
        return f"Job {self.job_name} - {self.status}"


# User profile data models based on the provided examples
class UserProfile(models.Model):
    """
    User profile model based on the provided data examples.
    This demonstrates how structured data would be stored.
    """
    TITLE_CHOICES = [
        ('Mr', 'Mr'),
        ('Mrs', 'Mrs'),
        ('Ms', 'Ms'),
        ('Dr', 'Dr'),
        ('Prof', 'Prof'),
        ('Other', 'Other'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    title = models.CharField(max_length=10, choices=TITLE_CHOICES)
    title_other = models.CharField(max_length=100, blank=True)
    first_name = models.CharField(max_length=100)
    middle_name = models.CharField(max_length=100, blank=True)
    last_name = models.CharField(max_length=100)
    age = models.PositiveIntegerField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'user_profile'
        indexes = [
            models.Index(fields=['last_name', 'first_name']),
            models.Index(fields=['age']),
        ]

    def __str__(self):
        return f"{self.first_name} {self.last_name}"


class Address(models.Model):
    """
    Address information for user profiles.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    profile = models.ForeignKey(UserProfile, on_delete=models.CASCADE, related_name='addresses')
    line1 = models.CharField(max_length=255)
    line2 = models.CharField(max_length=255, blank=True)
    line3 = models.CharField(max_length=255, blank=True)
    line4 = models.CharField(max_length=255, blank=True)
    city_town = models.CharField(max_length=100)
    county = models.CharField(max_length=100)
    country = models.CharField(max_length=100)
    postcode = models.CharField(max_length=20)
    is_primary = models.BooleanField(default=False)

    class Meta:
        db_table = 'address'
        indexes = [
            models.Index(fields=['postcode']),
            models.Index(fields=['city_town']),
            models.Index(fields=['country']),
        ]

    def __str__(self):
        return f"{self.city_town}, {self.country}"


class Income(models.Model):
    """
    Income information for user profiles.
    """
    CATEGORY_CHOICES = [
        ('SALARY', 'Salary Income'),
        ('RENTAL', 'Rental Income'),
        ('PENSION', 'Pension'),
        ('INVESTMENT', 'Investment Income'),
        ('OTHER', 'Other'),
    ]

    FREQUENCY_CHOICES = [
        ('MONTHLY', 'Per Month'),
        ('WEEKLY', 'Per Week'),
        ('ANNUALLY', 'Per Year'),
        ('QUARTERLY', 'Per Quarter'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    profile = models.ForeignKey(UserProfile, on_delete=models.CASCADE, related_name='incomes')
    category = models.CharField(max_length=20, choices=CATEGORY_CHOICES)
    description = models.TextField(blank=True)
    frequency = models.CharField(max_length=20, choices=FREQUENCY_CHOICES)
    gross_amount = models.DecimalField(max_digits=12, decimal_places=2)
    net_amount = models.DecimalField(max_digits=12, decimal_places=2)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'income'
        indexes = [
            models.Index(fields=['category']),
            models.Index(fields=['frequency']),
        ]

    def __str__(self):
        return f"{self.category} - {self.gross_amount}"


class Goal(models.Model):
    """
    Goals for user profiles - demonstrates unstructured text storage.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    profile = models.ForeignKey(UserProfile, on_delete=models.CASCADE, related_name='goals')
    aim = models.TextField(help_text="Unstructured long text in natural language")
    target_date = models.DateField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'goal'
        indexes = [
            GinIndex(fields=['aim'], name='goal_aim_gin_idx', opclasses=['gin_trgm_ops']),
            models.Index(fields=['target_date']),
        ]

    def __str__(self):
        return f"Goal for {self.profile} by {self.target_date}"
