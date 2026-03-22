package com.employee.pipeline

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.LocalDate
import org.apache.log4j.Logger

object EmployeePipeline {

  val log = Logger.getLogger(getClass.getName)

  val PG_URL      = "jdbc:postgresql://employee_postgres:5432/employee_db"
  val PG_USER     = "admin"
  val PG_PASS     = "admin123"
  val PG_DRIVER   = "org.postgresql.Driver"
  val INPUT_PATH  = "/opt/spark/employees_raw.csv"
  val TODAY       = LocalDate.now().toString
  val EMAIL_REGEX = "^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}$"

  val pgProps = {
    val p = new java.util.Properties()
    p.setProperty("user",     PG_USER)
    p.setProperty("password", PG_PASS)
    p.setProperty("driver",   PG_DRIVER)
    p
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("EmployeeDataPipeline")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    log.info("Reading raw CSV...")
    val raw = spark.read
      .option("header",      "true")
      .option("inferSchema", "false")
      .option("multiLine",   "true")
      .option("escape",      "\"")
      .csv(INPUT_PATH)

    log.info(s"Raw row count: ${raw.count()}")

    val deduped = raw.dropDuplicates("employee_id")
    log.info(s"After dedup: ${deduped.count()}")

    val flagged = deduped
      .withColumn("_email_valid",
        col("email").rlike(EMAIL_REGEX))
      .withColumn("_hire_future",
        to_date(col("hire_date"), "yyyy-MM-dd") > lit(TODAY))
      .withColumn("_missing_required",
        col("employee_id").isNull ||
        col("first_name").isNull  ||
        col("last_name").isNull   ||
        col("email").isNull       ||
        col("hire_date").isNull)
      .withColumn("_reject_reason",
        when(col("_missing_required"), "Missing required field")
        .when(!col("_email_valid"),    "Invalid email")
        .when(col("_hire_future"),     "Future hire_date")
        .otherwise(null))

    val good     = flagged.filter(col("_reject_reason").isNull)
    val rejected = flagged.filter(col("_reject_reason").isNotNull)
    log.info(s"Good: ${good.count()}  |  Rejected: ${rejected.count()}")

    val transformed = good
      .withColumn("first_name",   initcap(trim(col("first_name"))))
      .withColumn("last_name",    initcap(trim(col("last_name"))))
      .withColumn("full_name",    concat_ws(" ", col("first_name"), col("last_name")))
      .withColumn("email",        lower(trim(col("email"))))
      .withColumn("email_domain", regexp_extract(col("email"), "@(.+)$", 1))
      .withColumn("salary",
        regexp_replace(col("salary"), "[^\\d.]", "").cast(DecimalType(10, 2)))
      .withColumn("salary_band",
        when(col("salary") < 50000,   "Junior")
        .when(col("salary") <= 80000, "Mid")
        .otherwise("Senior"))
      .withColumn("hire_date",    to_date(col("hire_date"),  "yyyy-MM-dd"))
      .withColumn("birth_date",   to_date(col("birth_date"), "yyyy-MM-dd"))
      .withColumn("age",
        floor(datediff(current_date(), col("birth_date")) / 365.25).cast(IntegerType))
      .withColumn("tenure_years",
        (datediff(current_date(), col("hire_date")) / 365.25).cast(DecimalType(3, 1)))
      .withColumn("status",
        initcap(lower(trim(col("status")))))
      .withColumn("employee_id",
        col("employee_id").cast(IntegerType))
      .withColumn("manager_id",
        when(col("manager_id") =!= "", col("manager_id")).cast(IntegerType))
      .drop("_email_valid", "_hire_future", "_missing_required", "_reject_reason")

    val final_df = transformed.select(
      "employee_id", "first_name", "last_name", "full_name",
      "email", "email_domain", "hire_date", "job_title", "department",
      "salary", "salary_band", "manager_id", "address", "city", "state",
      "zip_code", "birth_date", "age", "tenure_years", "status"
    )

    log.info(s"Final clean row count: ${final_df.count()}")
    final_df.printSchema()
    final_df.show(5, truncate = false)

    final_df.write
      .mode("overwrite")
      .jdbc(PG_URL, "employees_clean", pgProps)
    log.info("employees_clean loaded")

    val rejected_out = rejected.select(
      col("employee_id").alias("raw_employee_id"),
      col("_reject_reason").alias("rejection_reason"),
      to_json(struct(rejected.columns.map(col): _*)).alias("raw_data")
    )
    rejected_out.write
      .mode("append")
      .jdbc(PG_URL, "employees_rejected", pgProps)
    log.info("employees_rejected saved")

    spark.stop()
    log.info("Pipeline complete.")
  }
}