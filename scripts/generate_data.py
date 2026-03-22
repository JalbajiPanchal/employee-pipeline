"""
generate_data.py
Generates employees_raw.csv with 1000+ records including deliberate data quality issues.
Run: pip install faker && python generate_data.py
"""

import csv
import random
from datetime import date, timedelta
from faker import Faker

fake = Faker()
random.seed(42)

DEPARTMENTS = ['IT', 'Analytics', 'HR', 'Finance', 'Marketing', 'Operations', 'Legal']
JOB_TITLES  = [
    'Software Engineer', 'Data Analyst', 'Manager', 'Director',
    'HR Specialist', 'Financial Analyst', 'Marketing Manager',
    'DevOps Engineer', 'Data Engineer', 'Product Manager',
]
STATUSES    = ['Active', 'active', 'ACTIVE', 'Inactive', 'inactive', 'Terminated']

def random_salary():
    """Return salary string with deliberate formatting issues."""
    amount = random.randint(35_000, 150_000)
    fmt = random.choice(['clean', 'dollar', 'comma', 'both'])
    if fmt == 'clean':
        return str(amount)
    if fmt == 'dollar':
        return f'${amount}'
    if fmt == 'comma':
        return f'{amount:,}'
    return f'${amount:,}'

def random_email(first, last):
    """Mix of valid / invalid emails."""
    domain = random.choice([
        'company.com', 'COMPANY.COM', 'corp.org', 'example.net', 'mail.co'
    ])
    style = random.choice(['valid1', 'valid2', 'invalid'])
    if style == 'valid1':
        return f'{first.lower()}.{last.lower()}@{domain}'
    if style == 'valid2':
        return f'{first[0].lower()}{last.lower()}@{domain}'

    return random.choice([
        f'{first.lower()}{last.lower()}',         
        f'{first.lower()}@{last.lower()}',          
        '',                                          
    ])

def random_hire_date():
    """Mostly valid past dates, ~5 % future dates."""
    if random.random() < 0.05:
        return date.today() + timedelta(days=random.randint(30, 730))  
    return fake.date_between(start_date='-20y', end_date='today')

def random_name_case(name):
    """Introduce inconsistent casing."""
    style = random.choice(['lower', 'upper', 'proper', 'mixed'])
    if style == 'lower':  return name.lower()
    if style == 'upper':  return name.upper()
    if style == 'proper': return name.capitalize()
    return name  

TOTAL = 1100
managers = list(range(1001, 1030))  

with open('data/employees_raw.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow([
        'employee_id','first_name','last_name','email','hire_date',
        'job_title','department','salary','manager_id','address',
        'city','state','zip_code','birth_date','status'
    ])

    seen_ids = set()
    for i in range(TOTAL):
        if random.random() < 0.03 and seen_ids:
            emp_id = random.choice(list(seen_ids))
        else:
            emp_id = 1001 + i
            seen_ids.add(emp_id)

        first = fake.first_name()
        last  = fake.last_name()

        hire_date  = random_hire_date()
        birth_date = fake.date_of_birth(minimum_age=20, maximum_age=60)

        mgr = '' if random.random() < 0.05 else random.choice(managers)
        address = '' if random.random() < 0.03 else fake.street_address()
        zip_code = '' if random.random() < 0.02 else fake.zipcode()

        writer.writerow([
            emp_id,
            random_name_case(first),
            random_name_case(last),
            random_email(first, last),
            hire_date,
            random.choice(JOB_TITLES),
            random.choice(DEPARTMENTS),
            random_salary(),
            mgr,
            address,
            fake.city(),
            fake.state_abbr(),
            zip_code,
            birth_date,
            random.choice(STATUSES),
        ])

print(f"Generated data/employees_raw.csv  ({TOTAL} rows including duplicates/errors)")