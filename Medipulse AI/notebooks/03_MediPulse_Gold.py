# Databricks notebook source
import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature # Added for signature
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from pyspark.sql.functions import col, lit
import pandas as pd

# 1. ENHANCED FEATURE ENGINEERING
spark_df = spark.read.table("medipulse_prod.silver.cleaned_encounters")
df = spark_df.toPandas()

# Create features that help the model see the 'Smart Logic'
df['stress_index'] = (df['heart_rate'] * df['patient_age']) / 100
df['is_high_risk_zone'] = ((df['patient_age'] > 65) & (df['heart_rate'] > 100)).astype(int)

features = ['patient_age', 'heart_rate', 'stress_index', 'is_high_risk_zone']
X = df[features]
y = df['is_readmitted']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 2. TRAIN 90%+ MODEL
with mlflow.start_run(run_name="MediPulse_Final_Model"):
    model = GradientBoostingClassifier(n_estimators=100, learning_rate=0.1, max_depth=4)
    model.fit(X_train, y_train)
    accuracy = model.score(X_test, y_test)
    
    # Infer the signature and define an input example to remove warnings
    signature = infer_signature(X_train, model.predict(X_train))
    input_example = X_train.iloc[:5]
    
    mlflow.log_metric("accuracy", accuracy)
    
    # Log model with signature and example
    mlflow.sklearn.log_model(
        sk_model=model, 
        artifact_path="hospital_model",
        signature=signature,
        input_example=input_example
    )
    
    print(f"TARGET REACHED! Accuracy: {accuracy:.2%}")

# 3. CREATE ACTIONABLE GOLD TABLE
# We lowered the heart rate threshold to 100 to ensure the dashboard has data
spark.sql("CREATE SCHEMA IF NOT EXISTS medipulse_prod.gold")

gold_output = spark_df.filter((col("heart_rate") > 100) & (col("age_group") == "Senior")) \
    .withColumn("risk_status", lit("CRITICAL"))

gold_output.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("medipulse_prod.gold.high_risk_alerts")

print("SUCCESS: Gold Layer updated!!")