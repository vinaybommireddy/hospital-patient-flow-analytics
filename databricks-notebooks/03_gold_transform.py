from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col, current_timestamp
from pyspark.sql import Window

#ADLS configuration 
spark.conf.set(
  "fs.azure.account.key.<<Storageaccount_name>>.dfs.core.windows.net",
  "<<Storage_Account_access_key>>"
)

# Paths
silver_path = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/patient_flow"
gold_dim_patient = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/patient_flow"
gold_dim_department = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/patient_flow"
gold_fact = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/patient_flow"

# Read silver data
silver_df = spark.read.format("delta").load(silver_path)

# Keep latest record per patient
w = Window.partitionBy("patient_id").orderBy(F.col("admission_time").desc())

silver_df = (
    silver_df
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# =========================
# PATIENT DIMENSION
# =========================

incoming_patient = (silver_df
                    .select("patient_id", "gender", "age")
                    .withColumn("effective_from", current_timestamp())
                   )

#  Safe casting for Synapse
patient_final = incoming_patient \
    .withColumn("surrogate_key", F.monotonically_increasing_id().cast("long")) \
    .withColumn("effective_from", col("effective_from").cast("string")) \
    .withColumn("effective_to", lit(None).cast("string")) \
    .withColumn("is_current", lit("true"))  # string

# Clean old files
dbutils.fs.rm(gold_dim_patient, True)

# Write Parquet
patient_final.write.mode("overwrite").parquet(gold_dim_patient)


# =========================
# DEPARTMENT DIMENSION
# =========================

incoming_dept = (silver_df
                 .select("department", "hospital_id")
                 .dropDuplicates(["department", "hospital_id"])
                 .withColumn("surrogate_key", F.monotonically_increasing_id().cast("long"))
                )

dbutils.fs.rm(gold_dim_department, True)

incoming_dept.select("surrogate_key", "department", "hospital_id") \
    .write.mode("overwrite").parquet(gold_dim_department)


# =========================
# FACT TABLE (FINAL FIXED)
# =========================

dim_patient_df = spark.read.parquet(gold_dim_patient) \
    .select(col("surrogate_key").alias("patient_sk"), "patient_id")

dim_dept_df = spark.read.parquet(gold_dim_department) \
    .select(col("surrogate_key").alias("department_sk"), "department", "hospital_id")

fact_base = (silver_df
             .select("patient_id", "department", "hospital_id", "admission_time", "discharge_time", "bed_id")
            )

fact_enriched = (fact_base
                 .join(dim_patient_df, on="patient_id", how="left")
                 .join(dim_dept_df, on=["department", "hospital_id"], how="left")
                )

# FULL SAFE CASTING (CRITICAL FIX)
fact_final = fact_enriched.select(
    F.monotonically_increasing_id().cast("long").alias("fact_id"),
    col("patient_sk").cast("long"),
    col("department_sk").cast("long"),

    col("admission_time").cast("string"),
    col("discharge_time").cast("string"),

    col("admission_time").cast("string").alias("admission_date"),

    ((F.unix_timestamp(col("discharge_time")) - F.unix_timestamp(col("admission_time"))) / 3600.0)
        .cast("double")
        .alias("length_of_stay_hours"),

    F.when(col("discharge_time") > current_timestamp(), "true")
     .otherwise("false")
     .alias("is_currently_admitted"),

    col("bed_id").cast("int"),

    current_timestamp().cast("string").alias("event_ingestion_time")
)


fact_final.write.mode("overwrite").parquet(gold_fact)


# =========================
# SANITY CHECK
# =========================

print("Patient dim:", spark.read.parquet(gold_dim_patient).count())
print("Department dim:", spark.read.parquet(gold_dim_department).count())
print("Fact rows:", spark.read.parquet(gold_fact).count())