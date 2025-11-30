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
    clean_df = df.filter(F.size(F.col("dq_failures")) == 0).drop("dq_failures")
    quarantined_df = df.filter(F.size(F.col("dq_failures")) > 0)

    print(
        f"valid rows: {clean_df.count()}, invalid rows: {quarantined_df.count()}"
    )

    return clean_df, quarantined_df

def validate_weather(spark: SparkSession, project_id: str, execution_date: str):
    """Perform data quality validation on the weather data for the given execution date.

    Args:
        spark (SparkSession): The Spark session.
        project_id (str): GCP project ID.
        execution_date (str): Execution date in YYYY-MM-DD format.
    """
    # Load weather data from BigQuery for the given partition_date
    weather_df = (
        spark.read.format("bigquery")
        .option("table", f"{project_id}.raw.weather")
        .load()
        .filter(F.col("partition_date") >= execution_date)
    )

    print(f"Processing {weather_df.count()} rows for date: {execution_date}")

    # Start with an array<string> dq_failures column
    df = weather_df.withColumn("dq_failures", F.array().cast("array<string>"))

    # Rule 1: Missing latitude/longitude or out of bounds
    df = df.withColumn(
    "dq_failures",
    F.when(
        F.col("latitude").isNull()
        | F.col("longitude").isNull()
        | (F.col("latitude") < -90)
        | (F.col("latitude") > 90)
        | (F.col("longitude") < -180)
        | (F.col("longitude") > 180),
        F.array_union(
            F.col("dq_failures"),
            F.array(F.lit("Invalid or missing latitude/longitude")),
        ),
    ).otherwise(F.col("dq_failures")),
    )

    # Rule 2: Missing timezone
    df = df.withColumn(
        "dq_failures",
        F.when(
            F.col("timezone").isNull() | (F.col("timezone") == ""),
            F.array_union(
                F.col("dq_failures"),
                F.array(F.lit("Missing timezone")),
            ),
        ).otherwise(F.col("dq_failures")),
    )

    # Rule 3: Temperature out of expected range (-60, 60)
    df = df.withColumn(
        "dq_failures",
        F.when(
            F.expr(
                "EXISTS(hourly.temperature_2m, x -> x < -60 OR x > 60)"
            ),
            F.array_union(
                F.col("dq_failures"),
                F.array(F.lit("Temperature out of expected range (-60, 60)")),
            ),
        ).otherwise(F.col("dq_failures")),
    )

    # Rule 4: Non-Negative precipitation values
    df = df.withColumn(
        "dq_failures",
        F.when(
            F.expr(
                "EXISTS(hourly.precipitation, x -> x < 0)"
            ),
            F.array_union(
                F.col("dq_failures"),
                F.array(F.lit("Negative precipitation value")),
            ),
        ).otherwise(F.col("dq_failures")),
    )

    clean_weather_df = df.filter(F.size("dq_failures") == 0).drop("dq_failures")
    quarantined_weather_df = df.filter(F.size("dq_failures") > 0)

    return clean_weather_df, quarantined_weather_df

def validate_zones(spark: SparkSession, project_id: str, execution_date: str):
    """Perform data quality validation on the taxi zones data.

    Args:
        spark (SparkSession): The Spark session.
        project_id (str): GCP project ID.
        execution_date (str): Execution date in YYYY-MM-DD format.
    """
    # Load taxi zones data from BigQuery
    zones_df = (
        spark.read.format("bigquery")
        .option("table", f"{project_id}.raw.taxi_zones")
        .load()
    )

    print(f"Processing {zones_df.count()} rows for taxi zones")

    # Start with an array<string> dq_failures column
    df = zones_df.withColumn("dq_failures", F.array().cast("array<string>"))

    # Rule 1: Missing LocationID
    df = df.withColumn(
        "dq_failures",
        F.when(
            F.col("LocationID").isNull() | (F.col("LocationID") <= 0),
            F.array_union(
                F.col("dq_failures"),
                F.array(F.lit("Missing or invalid LocationID")),
            ),
        ).otherwise(F.col("dq_failures")),
    )

    # Rule 2: Missing Borough or Zone
    df = df.withColumn(
        "dq_failures",
        F.when(
            F.col("Borough").isNull() | (F.col("Borough") == "") |
            F.col("Zone").isNull() | (F.col("Zone") == ""),
            F.array_union(
                F.col("dq_failures"),
                F.array(F.lit("Missing Borough or Zone")),
            ),
        ).otherwise(F.col("dq_failures")),
    )

    # Split clean vs quarantined
    clean_df = df.filter(F.size("dq_failures") == 0).drop("dq_failures")
    quarantined_df = df.filter(F.size("dq_failures") > 0)

    print(
        f"valid zone rows: {clean_df.count()}, invalid zone rows: {quarantined_df.count()}"
    )

    return clean_df, quarantined_df

def write_quarantine_records(quarantine_df, source_table: str, project_id: str):
    """Write quarantined records to BigQuery quarantine.dq_failures table."""

    if quarantine_df is None or quarantine_df.rdd.isEmpty():
        print("No quarantined records to write.")
        return

    cols = quarantine_df.columns

    # Pick the right ingestion date column and cast to DATE
    if "partition_date" in cols:
        ingestion_col = F.col("partition_date").cast("date")
    elif "ingestion_date" in cols:
        ingestion_col = F.col("ingestion_date").cast("date")
    else:
        ingestion_col = F.lit(None).cast("date")

    # Build final output
    quarantine_output = quarantine_df.select(
        F.lit(source_table).alias("source_table"),
        F.concat_ws("; ", F.col("dq_failures")).alias("failure_reason"),
        F.current_timestamp().alias("ingestion_timestamp"),
        F.to_json(
            F.struct(
                *[F.col(c) for c in cols if c != "dq_failures"]
            )
        ).alias("record_content"),
        ingestion_col.alias("ingestion_date"),  # must be DATE now
    )

    (
        quarantine_output.write.format("bigquery")
        .option("table", f"{project_id}.quarantine.dq_failures")
        .option("temporaryGcsBucket", "nyc-analytics-dev-data")
        .mode("append")
        .save()
    )

    print(
        f"Wrote {quarantine_output.count()} quarantined records to {project_id}.quarantine.dq_failures"
    )


def main():
    if len(sys.argv) != 4:
        raise ValueError(
            "Usage: dq_validation.py <project_id> <execution_date> <task_name>"
        )

    project_id = sys.argv[1]
    execution_date = "2025-01-01" # sys.argv[2]
    task_name = sys.argv[3]

    spark = (
        SparkSession.builder.appName(f"DQ Validation - {task_name} - {execution_date}")
        .getOrCreate()
    )
    spark.conf.set("materializationDataset", f"{project_id}.ops")

    print(
        f"Starting DQ validation task={task_name} date={execution_date} project={project_id}"
    )

    if task_name == "validate_trips":
        clean_df, quarantined_df = validate_trips(spark, project_id, execution_date)
        source_table = "raw.yellow_trips"
    elif task_name == "validate_weather":
        clean_df, quarantined_df = validate_weather(spark, project_id, execution_date)
        source_table = "raw.weather"
    elif task_name == "validate_zones":
        clean_df, quarantined_df = validate_zones(spark, project_id, execution_date)
        source_table = "raw.taxi_zones"
    else:
        raise ValueError(f"Unknown task_name: {task_name}")

    write_quarantine_records(quarantined_df, source_table, project_id)
    spark.stop()


if __name__ == "__main__":
    main()