"""
etl_job.py  –  Employee Data Pipeline (PySpark)
Run inside the Spark container:
  spark-submit --jars /opt/spark/jars/postgresql-42.6.0.jar \
               --master spark://spark-master:7077 \
               /opt/spark-apps/etl_job.py
"""

import re
import logging
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("EmployeePipeline")

PG_URL  = "jdbc:postgresql://employee_postgres:5432/employee_db"
PG_OPTS = {"user": "admin", "password": "admin123", "driver": "org.postgresql.Driver"}
INPUT   = "/opt/spark/employees_raw.csv"
TODAY   = date.today().isoformat()

EMAIL_RE = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"

spark = (
    SparkSession.builder
    .appName("EmployeeDataPipeline")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

log.info("Reading raw CSV …")
raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")       
    .option("multiLine", "true")
    .option("escape", '"')
    .csv(INPUT)
)
log.info(f"Raw row count: {raw.count()}")


deduped = raw.dropDuplicates(["employee_id"])
log.info(f"After dedup: {deduped.count()}")

flagged = (
    deduped
    .withColumn("_email_valid",
        F.col("email").rlike(EMAIL_RE))
    .withColumn("_hire_future",
        F.to_date("hire_date", "yyyy-MM-dd") > F.lit(TODAY))
    .withColumn("_missing_required",
        F.col("employee_id").isNull() |
        F.col("first_name").isNull() |
        F.col("last_name").isNull()  |
        F.col("email").isNull()      |
        F.col("hire_date").isNull())
    .withColumn("_reject_reason",
        F.when(F.col("_missing_required"), "Missing required field")
         .when(~F.col("_email_valid"),     "Invalid email")
         .when(F.col("_hire_future"),      "Future hire_date")
         .otherwise(None))
)

good     = flagged.filter(F.col("_reject_reason").isNull())
rejected = flagged.filter(F.col("_reject_reason").isNotNull())
log.info(f"Good: {good.count()}  |  Rejected: {rejected.count()}")


clean_salary_udf = F.udf(
    lambda s: re.sub(r"[^\d.]", "", s) if s else None
)

transformed = (
    good
    .withColumn("first_name", F.initcap(F.trim("first_name")))
    .withColumn("last_name",  F.initcap(F.trim("last_name")))
    .withColumn("full_name",  F.concat_ws(" ", "first_name", "last_name"))

    .withColumn("email",        F.lower(F.trim("email")))
    .withColumn("email_domain", F.regexp_extract("email", r"@(.+)$", 1))

    .withColumn("salary_str", clean_salary_udf(F.col("salary")))
    .withColumn("salary",
        F.col("salary_str").cast(DecimalType(10, 2)))
    .drop("salary_str")

    .withColumn("salary_band",
        F.when(F.col("salary") < 50_000,  "Junior")
         .when(F.col("salary") <= 80_000, "Mid")
         .otherwise("Senior"))

    .withColumn("hire_date",  F.to_date("hire_date",  "yyyy-MM-dd"))
    .withColumn("birth_date", F.to_date("birth_date", "yyyy-MM-dd"))

    .withColumn("age",
        F.floor(F.datediff(F.current_date(), F.col("birth_date")) / 365.25)
         .cast(IntegerType()))
    .withColumn("tenure_years",
        (F.datediff(F.current_date(), F.col("hire_date")) / 365.25)
         .cast(DecimalType(3, 1)))

    .withColumn("status",
        F.initcap(F.lower(F.trim("status"))))

    .withColumn("employee_id", F.col("employee_id").cast(IntegerType()))
    .withColumn("manager_id",
        F.when(F.col("manager_id") != "", F.col("manager_id")).cast(IntegerType()))

    .drop("_email_valid", "_hire_future", "_missing_required", "_reject_reason")
)

final = transformed.select(
    "employee_id","first_name","last_name","full_name",
    "email","email_domain","hire_date","job_title","department",
    "salary","salary_band","manager_id","address","city","state",
    "zip_code","birth_date","age","tenure_years","status"
)

log.info(f"Final clean row count: {final.count()}")
final.printSchema()
final.show(5, truncate=False)

log.info("Writing clean data to PostgreSQL …")
(
    final.write
    .format("jdbc")
    .option("url", PG_URL)
    .option("dbtable", "employees_clean")
    .options(**PG_OPTS)
    .mode("overwrite")      
    .save()
)
log.info("employees_clean loaded ✓")

rejected_out = (
    rejected
    .select(
        F.col("employee_id").alias("raw_employee_id"),
        F.col("_reject_reason").alias("rejection_reason"),
        F.to_json(F.struct("*")).alias("raw_data"),
    )
)
(
    rejected_out.write
    .format("jdbc")
    .option("url", PG_URL)
    .option("dbtable", "employees_rejected")
    .options(**PG_OPTS)
    .mode("append")
    .save()
)
log.info("employees_rejected saved ✓")

spark.stop()
log.info("Pipeline complete.")