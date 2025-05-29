"""
URL configuration for the API app.
"""

from django.urls import path, include
from rest_framework.routers import DefaultRouter
from . import views

# Create a router for ViewSets
router = DefaultRouter()

app_name = 'api'

urlpatterns = [
    # Include router URLs
    path('', include(router.urls)),
    
    # API Root and Documentation
    path('', views.APIRootView.as_view(), name='api-root'),
    path('health/', views.HealthCheckView.as_view(), name='health-check'),
    
    # Data Ingestion Endpoints
    path('ingest/structured/', views.StructuredDataIngestionView.as_view(), name='ingest-structured'),
    path('ingest/unstructured/', views.UnstructuredDataIngestionView.as_view(), name='ingest-unstructured'),
    path('ingest/csv/', views.CSVIngestionView.as_view(), name='ingest-csv'),
    path('ingest/json/', views.JSONIngestionView.as_view(), name='ingest-json'),
    path('ingest/bulk/', views.BulkDataIngestionView.as_view(), name='ingest-bulk'),
    
    # Schema Management
    path('schemas/', views.DataSchemaListCreateView.as_view(), name='schema-list-create'),
    path('schemas/<int:pk>/', views.DataSchemaDetailView.as_view(), name='schema-detail'),
    
    # Data Records
    path('records/', views.DataRecordListView.as_view(), name='record-list'),
    path('records/<uuid:pk>/', views.DataRecordDetailView.as_view(), name='record-detail'),
    path('records/<uuid:pk>/history/', views.RecordHistoryView.as_view(), name='record-history'),
    path('records/<uuid:pk>/update/', views.DataRecordUpdateView.as_view(), name='record-update'),
    
    # Unstructured Data
    path('unstructured/', views.UnstructuredDataListCreateView.as_view(), name='unstructured-list-create'),
    path('unstructured/<uuid:pk>/', views.UnstructuredDataDetailView.as_view(), name='unstructured-detail'),
    
    # Search and Query
    path('search/', views.SearchView.as_view(), name='search'),
    path('search/structured/', views.StructuredSearchView.as_view(), name='search-structured'),
    path('search/unstructured/', views.UnstructuredSearchView.as_view(), name='search-unstructured'),
    
    # Aggregation and Analytics
    path('aggregate/', views.AggregationView.as_view(), name='aggregate'),
    path('analytics/schemas/', views.SchemaAnalyticsView.as_view(), name='analytics-schemas'),
    path('analytics/trends/', views.TrendAnalyticsView.as_view(), name='analytics-trends'),
    
    # System Statistics
    path('stats/', views.SystemStatsView.as_view(), name='system-stats'),
    path('stats/performance/', views.PerformanceStatsView.as_view(), name='performance-stats'),
    
    # User Profiles (Example Data)
    path('profiles/', views.UserProfileListCreateView.as_view(), name='profile-list-create'),
    path('profiles/<uuid:pk>/', views.UserProfileDetailView.as_view(), name='profile-detail'),
    path('profiles/search/', views.UserProfileSearchView.as_view(), name='profile-search'),
    
    # Data Management
    path('jobs/', views.IngestionJobListView.as_view(), name='job-list'),
    path('jobs/<uuid:pk>/', views.IngestionJobDetailView.as_view(), name='job-detail'),
    
    # History and Audit
    path('history/', views.SystemHistoryView.as_view(), name='system-history'),
    path('history/changes/', views.ChangeHistoryView.as_view(), name='change-history'),
    
    # Export and Backup
    path('export/schema/<int:schema_id>/', views.SchemaExportView.as_view(), name='schema-export'),
    path('export/records/', views.RecordExportView.as_view(), name='record-export'),
]
