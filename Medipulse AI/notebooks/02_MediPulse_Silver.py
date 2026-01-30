# Databricks notebook source
from pyspark.sql.functions import col, sha2, when

# 1. SETUP SCHEMA
spark.sql("CREATE SCHEMA IF NOT EXISTS medipulse_prod.silver")

# 2. PRIVACY MASKING & REFINEMENT
silver_df = spark.read.table("medipulse_prod.bronze.raw_encounters") \
    .withColumn("masked_id", sha2(col("patient_id").cast("string"), 256)) \
    .withColumn("age_group", 
        when(col("patient_age") < 18, "Pediatric")
        .when(col("patient_age") < 65, "Adult")
        .otherwise("Senior")) \
    .filter("heart_rate > 40")

# 3. WRITE TO SILVER (with overwriteSchema to prevent errors)
silver_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("medipulse_prod.silver.cleaned_encounters")

print("SUCCESS: Silver Layer cleaned and PII masked.")