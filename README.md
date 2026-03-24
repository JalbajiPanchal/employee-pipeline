# Employee Data Pipeline

Takes raw messy employee data → cleans it with Spark → stores it in PostgreSQL.
All running in Docker, no local installs needed.

---

## Quick Start

```bash
# 1. Start everything
docker-compose up -d

# 2. Copy files into Spark container
docker cp scripts/postgresql-42.7.3.jar spark_master:/opt/spark/jars/
docker cp spark/etl_job.py spark_master:/opt/spark/
docker cp data/employees_raw.csv spark_master:/opt/spark/

# 3. Run the pipeline
docker exec spark_master /opt/spark/bin/spark-submit \
  --master local[*] \
  --jars /opt/spark/jars/postgresql-42.7.3.jar \
  /opt/spark/etl_job.py

# 4. Check the results
docker exec -it employee_postgres psql -U admin -d employee_db
```

---

## What This Pipeline Does

```
employees_raw.csv (1100 messy records)
        ↓
   Apache Spark
   - removes duplicates
   - rejects invalid emails
   - rejects future hire dates
   - cleans salary ($75,000 → 75000.00)
   - fixes name casing (JOHN → John)
   - calculates age, tenure, salary band
        ↓
  ┌─────────────────┐     ┌───────────────────┐
  │ employees_clean │     │ employees_rejected │
  │   667 records   │     │    405 records     │
  └─────────────────┘     └───────────────────┘
```

---

## Project Structure

```
employee-pipeline/
├── docker-compose.yml          ← spins up Spark + PostgreSQL
├── data/
│   └── employees_raw.csv       ← raw input data (1100 records)
├── sample_data/
│   ├── sample_input.csv        ← sample raw records (before cleaning)
│   └── sample_output.csv       ← sample cleaned records (after cleaning)
├── scripts/
│   ├── generate_data.py        ← how the raw data was created
│   ├── init_db.sql             ← creates the database tables
│   └── postgresql-42.7.3.jar  ← JDBC driver for Spark ↔ PostgreSQL
├── spark/
│   ├── etl_job.py              ← Python ETL job (used for running)
│   ├── EmployeePipeline.scala  ← Scala ETL job (preferred implementation)
│   └── build.sbt               ← Scala build file
└── sql/
    └── init.sql                ← views and indexes
```

---

## ETL Implementation

The pipeline is written in both **Python** and **Scala**.

The Scala version (`EmployeePipeline.scala`) is the preferred implementation as per assignment requirements. It uses the same logic — deduplication, email validation, salary cleaning, transformations, and loading to PostgreSQL.

The Python version (`etl_job.py`) is used for running since it requires no compilation step, making it easier to execute inside the Docker container directly with `spark-submit`.

---

## What Gets Cleaned

| Problem | Fix |
|---|---|
| Duplicate employee IDs | Keep first, drop the rest |
| Invalid emails | Move to rejected table |
| Future hire dates | Move to rejected table |
| Salary like `$75,000` | Strip symbols → `75000.00` |
| Names in random case | Convert to proper case |
| Status like ACTIVE, active | Normalize to Active |

## What Gets Added

| Column | Description |
|---|---|
| full_name | first + last name combined |
| email_domain | extracted from email |
| age | calculated from birth_date |
| tenure_years | years at company |
| salary_band | Junior / Mid / Senior |

---

## Sample Input and Output Data

Sample files are available in the `sample_data/` folder.

### Sample Input (`sample_data/sample_input.csv`)

Raw data with intentional quality issues:

```
employee_id,first_name,last_name,email,hire_date,job_title,department,salary,manager_id,address,city,state,zip_code,birth_date,status
1001,john,DOE,John.Doe@company.com,2020-01-15,Software Engineer,IT,"$75,000",2001,123 Main St,New York,NY,10001,1990-05-15,Active
1002,jane,smith,jane.smith@COMPANY.COM,2019-03-20,Data Analyst,Analytics,65000,2002,456 Oak Ave,Los Angeles,CA,90210,1988-08-22,active
1003,Bob,johnson,bob@company,2028-01-01,Manager,IT,"$95,000",,789 Pine Rd,Chicago,IL,60601,1985-12-10,ACTIVE
1004,ALICE,BROWN,,2021-06-10,HR Executive,HR,"$45,000",2001,321 Elm St,Houston,TX,77001,1993-02-28,Active
1001,john,DOE,John.Doe@company.com,2020-01-15,Software Engineer,IT,"$75,000",2001,123 Main St,New York,NY,10001,1990-05-15,Active
```

Issues visible in sample input:
- Row 1001 appears twice (duplicate)
- Row 1002 has uppercase email domain
- Row 1003 has an invalid email and a future hire date (2028)
- Row 1004 has a missing email and ALL CAPS name
- Salary values contain `$` symbols and commas

---

### Sample Output (`sample_data/sample_output.csv`)

Cleaned and enriched data after Spark processing:

```
employee_id,first_name,last_name,full_name,email,email_domain,hire_date,job_title,department,salary,salary_band,manager_id,address,city,state,zip_code,birth_date,age,tenure_years,status
1001,John,Doe,John Doe,john.doe@company.com,company.com,2020-01-15,Software Engineer,IT,75000.00,Mid,2001,123 Main St,New York,NY,10001,1990-05-15,34,5.2,Active
1002,Jane,Smith,Jane Smith,jane.smith@company.com,company.com,2019-03-20,Data Analyst,Analytics,65000.00,Mid,2002,456 Oak Ave,Los Angeles,CA,90210,1988-08-22,36,6.9,Active
```

What changed after cleaning:
- Duplicate record 1001 removed
- Emails converted to lowercase
- Salary symbols stripped and converted to numeric
- Names converted to proper case
- full_name, email_domain, age, tenure_years, salary_band columns added
- Status normalized to proper case
- Rows 1003 and 1004 moved to `employees_rejected` table due to invalid email and future hire date

---

## Results

```
Raw:       1,100 records
Clean:       667 records
Rejected:    405 records

Salary bands:
  Senior (>80k)   → 422
  Mid (50k-80k)   → 156
  Junior (<50k)   →  89
```

---

## Useful Commands

```bash
# See running containers
docker ps

# Open the database
docker exec -it employee_postgres psql -U admin -d employee_db

# Queries inside psql
SELECT COUNT(*) FROM employees_clean;
SELECT salary_band, COUNT(*) FROM employees_clean GROUP BY salary_band;
SELECT department, ROUND(AVG(salary),2) FROM employees_clean GROUP BY department;
SELECT * FROM employees_rejected LIMIT 5;
\q

# View Spark logs
docker logs spark_master

# Stop everything
docker-compose down
```

---

## Stack

| Tool | Version | Purpose |
|---|---|---|
| Apache Spark | 3.5.0 | data cleaning + transformation |
| PostgreSQL | 15 | storing results |
| Docker | latest | running everything |
| PySpark | 3.5.0 | Python ETL job |
| Scala | 2.12.18 | Scala ETL job |
| Faker | latest | generating test data |

---

## Troubleshooting

**"views depend on table" error**
```sql
DROP VIEW IF EXISTS dept_summary CASCADE;
DROP VIEW IF EXISTS salary_band_summary CASCADE;
```
Then run the pipeline again.

**Port 5432 already in use**
Change `5432:5432` to `5433:5432` in docker-compose.yml.

**Spark container not starting**
```bash
docker-compose logs spark-master
```