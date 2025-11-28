import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
#from utils.logger import log

def validate_trips(spark: SparkSession, project_id: str, execution_date: str):
    """Perform data quality validation on the trips data for the given execution date.

    Args:
        spark (SparkSession): The Spark session.
        project_id (str): GCP project ID.
        execution_date (str): Execution date in YYYY-MM-DD format.
    """
    # Load trips data from BigQuery for the given partition_date
    trips_df = (
        spark.read.format("bigquery")
        .option("table", f"{project_id}.raw.yellow_trips")
        .load()
        .filter(F.col("partition_date") >= execution_date)
    )

    print(f"Processing {trips_df.count()} rows for date: {execution_date}")

    # Start with an array<string> dq_failures column
    df = trips_df.withColumn("dq_failures", F.array().cast("array<string>"))

    # Rule 1: Same pickup/dropoff with zero distance
    df = df.withColumn(
        "dq_failures",
        F.when(
            (F.col("PULocationID") == F.col("DOLocationID"))
            & (F.col("trip_distance") == 0),
            F.array_union(
                F.col("dq_failures"),
                F.array(F.lit("Same pickup/dropoff with zero distance")),
            ),
        ).otherwise(F.col("dq_failures")),
    )

    # Rule 2: Invalid passenger count (<=0 or NULL)
    df = df.withColumn(
        "dq_failures",
        F.when(
            (F.col("passenger_count").isNull())
            | (F.col("passenger_count") <= 0),
            F.array_union(
                F.col("dq_failures"),
                F.array(F.lit("Invalid passenger count (<=0 or NULL)")),
            ),
        ).otherwise(F.col("dq_failures")),
    )

    # Rule 3: Fare components vs total_amount
    df = df.withColumn(
        "aggregated_charge",
        F.coalesce(F.col("extra"), F.lit(0))
        + F.coalesce(F.col("mta_tax"), F.lit(0))
        + F.coalesce(F.col("tolls_amount"), F.lit(0))
        + F.coalesce(F.col("improvement_surcharge"), F.lit(0))
        + F.coalesce(F.col("congestion_surcharge"), F.lit(0))
        + F.coalesce(F.col("airport_fee"), F.lit(0))
        + F.coalesce(F.col("cbd_congestion_fee"), F.lit(0))
        + F.coalesce(F.col("fare_amount"), F.lit(0)),
    )

    df = df.withColumn(
        "dq_failures",
        F.when(
            F.abs(F.coalesce(F.col("total_amount"), F.lit(0)))
            != F.abs(F.col("aggregated_charge")),
            F.array_union(
                F.col("dq_failures"),
                F.array(F.lit("Fare amount mismatch with total amount")),
            ),
        ).otherwise(F.col("dq_failures")),
    ).drop("aggregated_charge")

    # Rule 4: Invalid trip distance (<0 or NULL or == 0)
    df = df.withColumn(
        "dq_failures",
        F.when(
            (F.col("trip_distance").isNull()) | (F.col("trip_distance") <= 0),
            F.array_union(
                F.col("dq_failures"),
                F.array(F.lit("Invalid trip distance (<=0 or NULL)")),
            ),
        ).otherwise(F.col("dq_failures")),
    )

    # Rule 5: Dropoff before pickup
    df = df.withColumn(
        "dq_failures",
        F.when(
            F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"),
            F.array_union(
                F.col("dq_failures"),
                F.array(F.lit("Dropoff before pickup time")),
            ),
        ).otherwise(F.col("dq_failures")),
    )

    # Rule 6: Unrealistic trip duration (<1 min or > 12 hours / 720 minutes)
    df = df.withColumn(
        "trip_duration_minutes",
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime"))
        / 60,
    )

    df = df.withColumn(
        "dq_failures",
        F.when(
            (F.col("trip_duration_minutes") < 1)
            | (F.col("trip_duration_minutes") > 720),
            F.array_union(
                F.col("dq_failures"),
                F.array(
                    F.lit(
                        "Unrealistic trip duration (<1 min or >720 minutes)"
                    )
                ),
            ),
        ).otherwise(F.col("dq_failures")),
    ).drop("trip_duration_minutes")

    # Rule 7: Invalid pickup / dropoff location IDs (NULL or <=0)
    df = df.withColumn(
        "dq_failures",
        F.when(
            (F.col("PULocationID").isNull())
            | (F.col("PULocationID") <= 0)
            | (F.col("DOLocationID").isNull())
            | (F.col("DOLocationID") <= 0),
            F.array_union(
                F.col("dq_failures"),
                F.array(
                    F.lit(
                        "Invalid pickup/dropoff location ID (<=0 or NULL)"
                    )
                ),
            ),
        ).otherwise(F.col("dq_failures")),
    )

    # Split clean vs quarantined
    clean_df = df.filter(F.size(F.col("dq_failures")) == 0)
    quarantined_df = df.filter(F.size(F.col("dq_failures")) > 0)

    print(
        f"valid rows: {clean_df.count()}, invalid rows: {quarantined_df.count()}"
    )

    return clean_df, quarantined_df


def write_quarantine_records(quarantine_df, source_table, project_id: str):
    """Write quarantined records to BigQuery quarantine.dq_failures table."""

    if quarantine_df.count() == 0:
        print("No quarantined records to write.")
        return

    quarantine_output = quarantine_df.select(
        F.lit(source_table).alias("source_table"),
        F.concat_ws("; ", F.col("dq_failures")).alias("failure_reason"),
        F.current_timestamp().alias("ingestion_timestamp"),
        F.struct("*").alias("record_content"),
        F.col("partition_date").alias("ingestion_date"),
    )

    (
        quarantine_output.write.format("bigquery") \
        .option("table", f"{project_id}.quarantine.dq_failures") \
        .option("temporaryGcsBucket", "nyc-analytics-dev-data") \
        .mode("append") \
        .save()
    )

    print(
        f"Wrote {quarantine_output.count()} quarantined records to {project_id}.quarantine.dq_failures"
    )


def main():
    # For now, hard-code the project and execution date for testing
    project_id = "nyc-analytics-dev-477913"
    execution_date = "2025-01-01"

    spark = (
        SparkSession.builder.appName(f"DQ Validation - {execution_date}")
        .getOrCreate()
    )

    # Materialization dataset for temporary BQ objects used by the connector
    spark.conf.set("materializationDataset", f"{project_id}.ops")

    print(
        f"Starting DQ validation for date: {execution_date} in project: {project_id}"
    )
    print("=== Starting DQ Validation trips data ===")

    clean_trips_df, quarantined_trips_df = validate_trips(
        spark, project_id, execution_date
    )

    # Optionally persist quarantined records
    write_quarantine_records(
        quarantined_trips_df, "raw.yellow_trips", project_id
    )

    spark.stop()


if __name__ == "__main__":
    main()