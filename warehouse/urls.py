"""
URL configuration for the warehouse app.
"""

from django.urls import path
from . import views

app_name = 'warehouse'

urlpatterns = [
    # Main dashboard
    path('', views.DashboardView.as_view(), name='dashboard'),
    
    # API endpoints for dashboard data
    path('api/stats/', views.dashboard_stats_api, name='dashboard_stats'),
    path('api/search/', views.search_data, name='search_data'),
    path('api/history/<uuid:record_id>/', views.data_history_api, name='data_history'),
    path('api/aggregate/', views.aggregate_data_api, name='aggregate_data'),
    path('api/profiles/', views.user_profiles_api, name='user_profiles'),
]
