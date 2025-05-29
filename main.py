"""
WSGI entry point for the data warehouse Django application.
"""

import os
from django.core.wsgi import get_wsgi_application

# Set the default settings module
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'data_warehouse.settings')

# Get the WSGI application
app = get_wsgi_application()