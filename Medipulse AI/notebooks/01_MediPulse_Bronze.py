# Databricks notebook source
from pyspark.sql.functions import expr, current_timestamp, when, col

# 1. SETUP CATALOG & SCHEMA
spark.sql("CREATE CATALOG IF NOT EXISTS medipulse_prod")
spark.sql("CREATE SCHEMA IF NOT EXISTS medipulse_prod.bronze")

# 2. GENERATE "SMART" DATA (100,000 Records)
# We build a pattern: Seniors with high heart rates = High Readmission risk
raw_df = spark.range(0, 100000).select(
    expr("id").alias("encounter_id"),
    expr("cast(rand()*100000 as int)").alias("patient_id"),
    expr("cast(rand()*80 + 10 as int)").alias("patient_age"),
    expr("cast(rand()*60 + 50 as int)").alias("heart_rate")
).withColumn("is_readmitted", 
    when((col("patient_age") > 65) & (col("heart_rate") > 100), 
         when(expr("rand()") < 0.95, 1).otherwise(0)) # 95% chance if high risk
    .otherwise(when(expr("rand()") < 0.05, 1).otherwise(0)) # 5% chance otherwise
).withColumn("gender", expr("CASE WHEN rand() > 0.5 THEN 'M' ELSE 'F' END")) \
 .withColumn("ingestion_time", current_timestamp())

# 3. WRITE TO BRONZE
raw_df.write.format("delta").mode("overwrite").saveAsTable("medipulse_prod.bronze.raw_encounters")
print("SUCCESS: 100k Smart Records ingested into Bronze.")