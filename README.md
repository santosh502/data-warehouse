# Scalable Data Warehouse & Query Engine

A comprehensive Django-based data warehouse system designed to handle large volumes of structured and unstructured data with complete change history tracking.

## Features

### Core Capabilities
- **Multi-format Data Ingestion**: Support for CSV, JSON, and bulk data ingestion
- **Flexible Data Storage**: Handle both structured (with schemas) and unstructured data
- **Complete Change Tracking**: Audit trail for all data modifications with temporal data patterns
- **Advanced Query Engine**: Full-text search, JSONB queries, and aggregation operations
- **Horizontal Scalability**: Database design optimized for large-scale data operations
- **RESTful API**: Comprehensive REST API with interactive documentation

### Data Management
- **Schema Management**: Define and version data schemas with JSON Schema support
- **User Profiles**: Example implementation with addresses, income, and goals data
- **Bulk Operations**: High-performance bulk data ingestion with error handling
- **Data Validation**: Comprehensive validation and error reporting

### Analytics & Visualization
- **Real-time Dashboard**: Interactive dashboard with charts and statistics
- **Trend Analysis**: Historical data analysis and visualization
- **Performance Monitoring**: Query performance tracking and system metrics
- **Search Analytics**: Full-text search across structured and unstructured data

## Architecture

### Database Design
- **PostgreSQL** with JSONB support for flexible data storage
- **GIN indexes** for high-performance JSON queries and full-text search
- **UUID primary keys** for distributed system compatibility
- **Temporal data patterns** for complete change history tracking

### Application Stack
- **Django 4.2+** with Django REST Framework
- **PostgreSQL** with advanced indexing strategies
- **Bootstrap 5** with dark theme for responsive UI
- **Chart.js** for data visualization
- **Modular architecture** with separation of concerns

### Scalability Features
- **Horizontal scaling** ready database schema
- **Efficient indexing** for millions of records
- **Bulk operation support** with transaction management
- **Query optimization** with performance monitoring
- **Caching strategies** for frequently accessed data

## Requirements

### System Requirements
- Python 3.8+
- PostgreSQL 12+

### Python Dependencies
- Django 4.2+
- djangorestframework
- psycopg2-binary
- django-cors-headers

## Installation & Setup

### 1. Environment Setup
```bash
# Clone the repository
git clone https://github.com/santosh502/Data-Warehouse-Query-Engine.git
cd data-warehouse

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install django djangorestframework psycopg2-binary django-cors-headers django-extensions email-validator python-dotenv gunicorn
```

### 2. Database Setup
```bash
# Set up PostgreSQL database
# Option 1: Local PostgreSQL installation
createdb data_warehouse


# Set environment variable
export DATABASE_URL="postgresql://postgres:password@localhost:5432/data_warehouse"
```

### 3. Application Setup
```bash
# Run database migrations
python manage.py migrate

# Create PostgreSQL extensions (if needed)
python manage.py shell -c "
from django.db import connection
with connection.cursor() as cursor:
    cursor.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm;')
    cursor.execute('CREATE EXTENSION IF NOT EXISTS btree_gin;')
"

# Create superuser (optional)
python manage.py createsuperuser

# Collect static files
python manage.py collectstatic --noinput

# Run the development server
python manage.py runserver 0.0.0.0:8000

#To upload Sample data
 python manage.py populate_sample_data
