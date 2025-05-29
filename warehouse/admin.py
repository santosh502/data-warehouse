"""
Django admin configuration for warehouse models.
"""

from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse
from django.utils.safestring import mark_safe
import json

from .models import (
    DataSchema, DataRecord, DataRecordHistory, UnstructuredData,
    QueryLog, DataIngestionJob, UserProfile, Address, Income, Goal
)


@admin.register(DataSchema)
class DataSchemaAdmin(admin.ModelAdmin):
    list_display = ('name', 'version', 'is_active', 'created_at', 'record_count')
    list_filter = ('is_active', 'created_at', 'version')
    search_fields = ('name', 'description')
    readonly_fields = ('created_at', 'updated_at')
    ordering = ('-created_at',)

    def record_count(self, obj):
        return obj.records.filter(is_active=True).count()
    record_count.short_description = 'Active Records'

    def get_queryset(self, request):
        return super().get_queryset(request).prefetch_related('records')


@admin.register(DataRecord)
class DataRecordAdmin(admin.ModelAdmin):
    list_display = ('id', 'schema', 'source_file', 'created_at', 'is_active', 'data_preview')
    list_filter = ('schema', 'is_active', 'created_at', 'source_file')
    search_fields = ('id', 'source_file')
    readonly_fields = ('id', 'created_at', 'updated_at', 'formatted_data')
    raw_id_fields = ('schema', 'created_by')
    ordering = ('-created_at',)

    def data_preview(self, obj):
        data_str = json.dumps(obj.data)
        if len(data_str) > 100:
            return data_str[:100] + '...'
        return data_str
    data_preview.short_description = 'Data Preview'

    def formatted_data(self, obj):
        return format_html(
            '<pre>{}</pre>',
            json.dumps(obj.data, indent=2)
        )
    formatted_data.short_description = 'Formatted Data'


@admin.register(DataRecordHistory)
class DataRecordHistoryAdmin(admin.ModelAdmin):
    list_display = ('record_id', 'operation', 'schema', 'timestamp', 'changed_by', 'changed_fields_count')
    list_filter = ('operation', 'schema', 'timestamp')
    search_fields = ('record_id', 'changed_by__username')
    readonly_fields = ('id', 'timestamp', 'formatted_old_data', 'formatted_new_data')
    raw_id_fields = ('schema', 'changed_by')
    ordering = ('-timestamp',)

    def changed_fields_count(self, obj):
        return len(obj.changed_fields) if obj.changed_fields else 0
    changed_fields_count.short_description = 'Changed Fields'

    def formatted_old_data(self, obj):
        if obj.old_data:
            return format_html('<pre>{}</pre>', json.dumps(obj.old_data, indent=2))
        return '-'
    formatted_old_data.short_description = 'Old Data'

    def formatted_new_data(self, obj):
        if obj.new_data:
            return format_html('<pre>{}</pre>', json.dumps(obj.new_data, indent=2))
        return '-'
    formatted_new_data.short_description = 'New Data'


@admin.register(UnstructuredData)
class UnstructuredDataAdmin(admin.ModelAdmin):
    list_display = ('id', 'title', 'data_type', 'content_preview', 'created_at', 'is_active')
    list_filter = ('data_type', 'is_active', 'created_at')
    search_fields = ('title', 'content', 'tags')
    readonly_fields = ('id', 'created_at', 'updated_at', 'formatted_metadata')
    raw_id_fields = ('related_record', 'created_by')
    ordering = ('-created_at',)

    def content_preview(self, obj):
        if len(obj.content) > 100:
            return obj.content[:100] + '...'
        return obj.content
    content_preview.short_description = 'Content Preview'

    def formatted_metadata(self, obj):
        return format_html('<pre>{}</pre>', json.dumps(obj.metadata, indent=2))
    formatted_metadata.short_description = 'Formatted Metadata'


@admin.register(QueryLog)
class QueryLogAdmin(admin.ModelAdmin):
    list_display = ('query_type', 'execution_time', 'result_count', 'user', 'timestamp')
    list_filter = ('query_type', 'timestamp', 'user')
    search_fields = ('query_type', 'user__username')
    readonly_fields = ('id', 'timestamp', 'formatted_query_params')
    raw_id_fields = ('user',)
    ordering = ('-timestamp',)

    def formatted_query_params(self, obj):
        return format_html('<pre>{}</pre>', json.dumps(obj.query_params, indent=2))
    formatted_query_params.short_description = 'Query Parameters'


@admin.register(DataIngestionJob)
class DataIngestionJobAdmin(admin.ModelAdmin):
    list_display = ('job_name', 'status', 'progress_percentage', 'started_at', 'completed_at', 'created_by')
    list_filter = ('status', 'started_at', 'schema')
    search_fields = ('job_name', 'file_name', 'created_by__username')
    readonly_fields = ('id', 'progress_percentage', 'formatted_error_log')
    raw_id_fields = ('schema', 'created_by')
    ordering = ('-started_at',)

    def progress_percentage(self, obj):
        if obj.total_records > 0:
            percentage = (obj.processed_records / obj.total_records) * 100
            return f"{percentage:.1f}%"
        return "0%"
    progress_percentage.short_description = 'Progress'

    def formatted_error_log(self, obj):
        if obj.error_log:
            return format_html('<pre>{}</pre>', obj.error_log)
        return '-'
    formatted_error_log.short_description = 'Error Log'


# User Profile Admin Classes
class AddressInline(admin.TabularInline):
    model = Address
    extra = 1


class IncomeInline(admin.TabularInline):
    model = Income
    extra = 1


class GoalInline(admin.TabularInline):
    model = Goal
    extra = 1


@admin.register(UserProfile)
class UserProfileAdmin(admin.ModelAdmin):
    list_display = ('full_name', 'title', 'age', 'created_at', 'address_count', 'income_count', 'goal_count')
    list_filter = ('title', 'age', 'created_at')
    search_fields = ('first_name', 'last_name', 'middle_name')
    readonly_fields = ('id', 'created_at', 'updated_at', 'full_name')
    inlines = [AddressInline, IncomeInline, GoalInline]
    ordering = ('-created_at',)

    def full_name(self, obj):
        return f"{obj.first_name} {obj.last_name}"
    full_name.short_description = 'Full Name'

    def address_count(self, obj):
        return obj.addresses.count()
    address_count.short_description = 'Addresses'

    def income_count(self, obj):
        return obj.incomes.count()
    income_count.short_description = 'Income Sources'

    def goal_count(self, obj):
        return obj.goals.count()
    goal_count.short_description = 'Goals'


@admin.register(Address)
class AddressAdmin(admin.ModelAdmin):
    list_display = ('profile', 'city_town', 'country', 'postcode', 'is_primary')
    list_filter = ('country', 'is_primary')
    search_fields = ('profile__first_name', 'profile__last_name', 'city_town', 'postcode')
    raw_id_fields = ('profile',)


@admin.register(Income)
class IncomeAdmin(admin.ModelAdmin):
    list_display = ('profile', 'category', 'frequency', 'gross_amount', 'net_amount', 'created_at')
    list_filter = ('category', 'frequency', 'created_at')
    search_fields = ('profile__first_name', 'profile__last_name', 'description')
    raw_id_fields = ('profile',)


@admin.register(Goal)
class GoalAdmin(admin.ModelAdmin):
    list_display = ('profile', 'aim_preview', 'target_date', 'created_at')
    list_filter = ('target_date', 'created_at')
    search_fields = ('profile__first_name', 'profile__last_name', 'aim')
    raw_id_fields = ('profile',)

    def aim_preview(self, obj):
        if len(obj.aim) > 100:
            return obj.aim[:100] + '...'
        return obj.aim
    aim_preview.short_description = 'Aim Preview'
