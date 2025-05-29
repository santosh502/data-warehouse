"""
Django management command to populate the database with sample data
based on the examples provided in the assignment PDF.
"""

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from warehouse.models import DataSchema, DataRecord, UserProfile, Address, Income, UnstructuredData
import json
from datetime import datetime, timedelta
import random


class Command(BaseCommand):
    help = 'Populate the database with sample data based on the PDF examples'

    def add_arguments(self, parser):
        parser.add_argument(
            '--count',
            type=int,
            default=10,
            help='Number of user profiles to create (default: 10)'
        )

    def handle(self, *args, **options):
        count = options['count']
        
        self.stdout.write(
            self.style.SUCCESS(f'Creating sample data for {count} user profiles...')
        )
        
        with transaction.atomic():
            # Create schemas
            self.create_schemas()
            
            # Create user profiles with related data
            self.create_user_profiles(count)
            
            # Create unstructured data (goals)
            self.create_unstructured_goals(count)
            
        self.stdout.write(
            self.style.SUCCESS(f'Successfully created sample data for {count} profiles!')
        )

    def create_schemas(self):
        """Create data schemas based on the PDF structure"""
        
        # User Profile Schema
        profile_schema, created = DataSchema.objects.get_or_create(
            name="user_profile",
            defaults={
                "description": "User personal details schema",
                "schema_definition": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string", "enum": ["Mr", "Mrs", "Dr", "Ms", "Other"]},
                        "title_details": {"type": "string"},
                        "first_name": {"type": "string"},
                        "middle_name": {"type": "string"},
                        "surname": {"type": "string"},
                        "age": {"type": "integer", "minimum": 18, "maximum": 120}
                    },
                    "required": ["first_name", "surname", "age"]
                }
            }
        )
        
        # Address Schema
        address_schema, created = DataSchema.objects.get_or_create(
            name="address",
            defaults={
                "description": "Address details schema",
                "schema_definition": {
                    "type": "object",
                    "properties": {
                        "owner": {"type": "string"},
                        "line1": {"type": "string"},
                        "line2": {"type": "string"},
                        "line3": {"type": "string"},
                        "line4": {"type": "string"},
                        "city_town": {"type": "string"},
                        "county": {"type": "string"},
                        "country": {"type": "string"},
                        "postcode": {"type": "string"}
                    },
                    "required": ["owner", "line1", "city_town", "country", "postcode"]
                }
            }
        )
        
        # Income Schema
        income_schema, created = DataSchema.objects.get_or_create(
            name="income",
            defaults={
                "description": "Income details schema",
                "schema_definition": {
                    "type": "object",
                    "properties": {
                        "owner": {"type": "string"},
                        "category": {"type": "string", "enum": ["Salary", "Rental Income", "Pension", "Investment", "Other"]},
                        "description": {"type": "string"},
                        "frequency": {"type": "string", "enum": ["Monthly", "Quarterly", "Annually", "Weekly"]},
                        "gross_amount": {"type": "number", "minimum": 0},
                        "net_amount": {"type": "number", "minimum": 0}
                    },
                    "required": ["owner", "category", "frequency", "gross_amount", "net_amount"]
                }
            }
        )
        
        self.stdout.write('Created schemas: user_profile, address, income')

    def create_user_profiles(self, count):
        """Create user profiles with addresses and income data"""
        
        # Sample data based on PDF structure
        titles = ["Mr", "Mrs", "Dr", "Ms"]
        first_names = ["John", "Sarah", "Michael", "Emma", "David", "Lisa", "James", "Rachel", "Robert", "Jennifer"]
        surnames = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
        cities = ["London", "Manchester", "Birmingham", "Liverpool", "Leeds", "Sheffield", "Bristol", "Newcastle", "Nottingham", "Cardiff"]
        counties = ["Greater London", "Greater Manchester", "West Midlands", "Merseyside", "West Yorkshire", "South Yorkshire", "Bristol", "Tyne and Wear", "Nottinghamshire", "Cardiff"]
        countries = ["United Kingdom", "England", "Wales", "Scotland"]
        
        profile_schema = DataSchema.objects.get(name="user_profile")
        address_schema = DataSchema.objects.get(name="address")
        income_schema = DataSchema.objects.get(name="income")
        
        for i in range(count):
            # Create user profile
            first_name = random.choice(first_names)
            surname = random.choice(surnames)
            
            profile_data = {
                "title": random.choice(titles),
                "first_name": first_name,
                "middle_name": random.choice(["James", "Marie", "Lee", "Ann", "Paul", ""]),
                "surname": surname,
                "age": random.randint(25, 75)
            }
            
            profile_record = DataRecord.objects.create(
                schema=profile_schema,
                data=profile_data
            )
            
            # Create corresponding UserProfile
            user_profile = UserProfile.objects.create(
                title=profile_data['title'],
                first_name=profile_data['first_name'],
                middle_name=profile_data['middle_name'],
                last_name=profile_data['surname'],
                age=profile_data['age']
            )
            
            # Create 1-3 addresses for each user
            num_addresses = random.randint(1, 3)
            for addr_idx in range(num_addresses):
                city = random.choice(cities)
                county = counties[cities.index(city)]
                
                # Create Address model instance
                Address.objects.create(
                    profile=user_profile,
                    line1=f"{random.randint(1, 999)} {random.choice(['High Street', 'Main Road', 'Church Lane', 'Mill Road', 'Victoria Street'])}",
                    line2=random.choice(["", "Apartment 2B", "Flat 5", "Unit 10"]),
                    line3="",
                    line4="",
                    city_town=city,
                    county=county,
                    country=random.choice(countries),
                    postcode=f"{random.choice(['SW', 'NW', 'SE', 'NE', 'M', 'B', 'L', 'LS'])}{random.randint(1,99)} {random.randint(1,9)}{random.choice(['AA', 'BB', 'CC', 'DD'])}"
                )
                
                # Also create as DataRecord for schema tracking
                address_data = {
                    "owner": f"{user_profile.first_name} {user_profile.last_name}",
                    "line1": f"{random.randint(1, 999)} {random.choice(['High Street', 'Main Road', 'Church Lane', 'Mill Road', 'Victoria Street'])}",
                    "line2": random.choice(["", "Apartment 2B", "Flat 5", "Unit 10"]),
                    "line3": "",
                    "line4": "",
                    "city_town": city,
                    "county": county,
                    "country": random.choice(countries),
                    "postcode": f"{random.choice(['SW', 'NW', 'SE', 'NE', 'M', 'B', 'L', 'LS'])}{random.randint(1,99)} {random.randint(1,9)}{random.choice(['AA', 'BB', 'CC', 'DD'])}"
                }
                
                DataRecord.objects.create(
                    schema=address_schema,
                    data=address_data
                )
            
            # Create 1-4 income sources for each user
            num_incomes = random.randint(1, 4)
            categories = ["Salary", "Rental Income", "Pension", "Investment"]
            frequencies = ["Monthly", "Annually", "Quarterly"]
            
            for inc_idx in range(num_incomes):
                category = random.choice(categories)
                frequency = random.choice(frequencies)
                gross = random.randint(2000, 8000) if frequency == "Monthly" else random.randint(25000, 95000)
                net = gross * random.uniform(0.7, 0.85)  # Tax deduction
                
                # Create Income model instance
                Income.objects.create(
                    profile=user_profile,
                    category=category,
                    description=f"{category} from primary employment" if category == "Salary" else f"{category} details",
                    frequency=frequency,
                    gross_amount=round(gross, 2),
                    net_amount=round(net, 2)
                )
                
                # Also create as DataRecord for schema tracking
                income_data = {
                    "owner": f"{user_profile.first_name} {user_profile.last_name}",
                    "category": category,
                    "description": f"{category} from primary employment" if category == "Salary" else f"{category} details",
                    "frequency": frequency,
                    "gross_amount": round(gross, 2),
                    "net_amount": round(net, 2)
                }
                
                DataRecord.objects.create(
                    schema=income_schema,
                    data=income_data
                )
        
        self.stdout.write(f'Created {count} user profiles with addresses and income data')

    def create_unstructured_goals(self, count):
        """Create unstructured goal data as mentioned in the PDF"""
        
        sample_goals = [
            "I want to save enough money to buy a house in the next 5 years. I'm currently renting and paying £1,200 per month, but I'd like to have my own place with a garden where I can grow vegetables and have space for my family to visit.",
            "My goal is to retire comfortably by age 65 with enough savings to travel the world. I've always wanted to visit Japan, Australia, and South America. I plan to save £500 per month towards this goal.",
            "I need to build an emergency fund of at least 6 months of expenses. Currently, I have very little in savings and this makes me anxious about unexpected costs like car repairs or medical bills.",
            "I want to start my own consulting business within the next 2 years. I need to save enough to cover living expenses for at least the first 6 months while I build up my client base.",
            "My priority is paying off my student loans completely within the next 3 years. I currently owe £25,000 and the interest is adding up quickly. I want to be debt-free before I turn 30.",
            "I'm planning to get married next year and need to save for the wedding costs. We're estimating around £15,000 for a modest celebration with close family and friends.",
            "I want to invest in my education by completing a master's degree program. The tuition costs about £12,000 and I'd like to avoid taking on more debt if possible.",
            "My goal is to help my parents with their mortgage payments as they approach retirement. They've supported me throughout my life and I want to return the favor.",
            "I need to save for a new car as my current one is 15 years old and constantly breaking down. I'm looking at spending around £8,000 for a reliable used vehicle.",
            "I want to create a college fund for my newborn daughter. Starting early with regular contributions should help ensure she has options for higher education without the burden of student debt."
        ]
        
        goal_dates = [
            "2029-12-31",  # 5 years
            "2039-06-30",  # Retirement
            "2025-12-31",  # Emergency fund
            "2026-08-15",  # Business
            "2027-05-01",  # Student loans
            "2025-09-15",  # Wedding
            "2026-01-30",  # Education
            "2030-12-31",  # Parents
            "2025-06-30",  # Car
            "2042-08-31"   # College fund
        ]
        
        # Create goals for random users
        user_profiles = list(UserProfile.objects.all())
        
        for i in range(min(len(sample_goals), len(user_profiles))):
            profile = user_profiles[i]
            goal_text = sample_goals[i]
            target_date = goal_dates[i]
            
            UnstructuredData.objects.create(
                title=f"Financial Goal - {profile.first_name} {profile.last_name}",
                content=goal_text,
                data_type='TEXT',
                metadata={
                    "target_date": target_date,
                    "goal_type": "financial",
                    "owner": f"{profile.first_name} {profile.last_name}",
                    "category": "personal_finance"
                },
                tags=["financial_goal", "planning", "savings"],
                related_record_id=None
            )
        
        self.stdout.write(f'Created {min(len(sample_goals), len(user_profiles))} unstructured goal records')