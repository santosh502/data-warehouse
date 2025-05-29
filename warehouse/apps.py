"""
Django app configuration for warehouse.
"""

from django.apps import AppConfig


class WarehouseConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'warehouse'
    verbose_name = 'Data Warehouse'

    def ready(self):
        """
        Initialize the app when Django starts.
        """
        # Import signal handlers if any
        # import warehouse.signals
        pass
