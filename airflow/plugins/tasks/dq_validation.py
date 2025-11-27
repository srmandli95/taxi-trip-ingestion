import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
#from utils.logger import log

def validate_trips(spark: SparkSession, project_id: str, execution_date: str) -> None:
    """Perform data quality validation on the trips data for the given execution date.
    
    Args:
        spark (SparkSession): The Spark session.
        project_id (str): GCP project ID.
        execution_date (str): Execution date in YYYY-MM-DD format.
    """
    # Load trips data from BigQuery
    trips_df = spark.read.format("bigquery") \
        .option("table", f"{project_id}.raw.yellow_trips") \
        .load() \
        .filter(F.col("partition_date") == execution_date)

    print(f"Processing {trips_df.count()} rows for date :{execution_date}")


    df = trips_df.withColumn("dq_failures", F.array().cast("array<string>"))

    df = df.withColumn("dq_failures", 
        F.when(
            (F.col("PULocationID") == F.col("DOLocationID")) & (F.col("trip_distance") == 0 ),
        F.array_union(F.col("qd_failures"), F.array(F.lit("Same pickup/dropoff with zero distance")))
        ).otherwise(F.col("dq_failures"))
    )

    df = df.withColumn("dq_failures",
        F.when(
            (F.col("passenger_count").isNull()) | (F.col("passenger_count") <= 0),
            F.array_union(F.col("dq_failures"), F.array(F.lit("Invalid passenger count (<=0 or NULL)")))
        ).otherwise(F.col("dq_failures"))
    )

    df = df.withColumn("aggrigated_charge",
        F.coalesce(F.col("extra"), F.lit(0)) +
        F.coleasce(F.col("mta_tax"), F.lit(0)) +
        F.coleasce(F.col("tolls_amount"), F.lit(0)) +
        F.coleasce(F.col("improvement_surcharge"), F.lit(0)) +
        F.coleasce(F.col("congestion_surcharge"), F.lit(0)) +
        F.coleasce(F.col("airport_fee"), F.lit(0)) +
        F.coleasce(F.col("cbd_congestion_fee"), F.lit(0)) +
        F.coleasce(F.col("fare_amount"), F.lit(0))
    )

    df = df.withColumn("dq_failures",
        F.when(
            F.abs(F.coalesce(F.col("total_amount"), F.lit(0))) != F.abs(F.col("aggrigated_charge")),
            F.array_union(F.col("dq_failures"), F.array(F.lit("Fare amount mismatch with total amount")))
            ).otherwise(F.col("dq_failures"))
        ).drop("aggrigated_charge")

    df = df.withColumn("dq_failures",
        F.when(
            (F.col("trip_distance").isNull()) | (F.col("trip_distance") <= 0),
            F.array_union(F.col("dq_failures"), F.array(F.lit("Invalid trip distance (<0 or NULL)")))
        ).otherwise(F.col("dq_failures"))
    )

    df = df.withColumn("dq_failures", 
        F.when(
            (F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime")),
            F.array_union(F.col("dq_failures"), F.array(F.lit("Dropoff before pickup time")))
        ).otherwise(F.col("dq_failures"))
    )

    df = df.withColumn("trip_duration_minutes",
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60
    )

    df = df.withColumn("dq_failures",
        F.when(
            (F.col("trip_duration_minutes") < 1) | (F.col("trip_duration_minutes") > 720),
            F.array_union(F.col("dq_failures"), F.array(F.lit("Unrealistic trip duration (<1 min or >720 minutes)")))
        ).otherwise(F.col("dq_failures"))
    ).drop("trip_duration_minutes")


    df.withColumn("dq_failures",
        F.when
            (
                (F.col("PULocationID").isNull() | F.col(DOLocationID) <=0) |
                (F.col("DOLactionID").isNull() | F.col(DOLocationID) <=0),
                F.array_union(F.col("bq_failures"), F.array(F.lit("Invalid pickup/dropoff location ID (<=0 or NULL)")))
            ).otherwise(F.col("dw_failures"))
        )

    clean_df = df.filter(F.size(F.col("dq_failures")) == 0)

    quarentined_df = df.filter(F.size(F.col("dq_failures")) > 0)

    print(f"valid rows: {clean_df.count()}, invalid rows: {quarentined_df.count()}")

    return clean_df, quarentined_df


def write_quartine_records(quartine_df, source_table, project_id):
    """Write quarentined records to BigQuery quarentine.dq_failures table.

    Args:
        quartine_df: DataFrame with quarentined records.
        source_table: Source table name (e.g., raw.yellow_trips).
        project_id: GCP project ID.
    """

    if quarentined_df.count() == 0:
        print("No quarentined records to write.")
        return

    quarentine_output = quarentine_df.select(
        F.lit(source_table).alias("source_table"),
        F.concat_ws("; ", F.col("dq_failures")).alais("failure_reason"),
        F.current_timestamp().alis("ingestion_timestamp"),
        F.to_json(F.struct("*")).alias("record_content"),
        F.col("partition_date").alais("ingestion_date")
    )
    
    quarentine_output.write.format("bigquery") \
        .option("table", f"{project_id}.quarentine.dq_failures") \
        .mode("append") \
        .save()

    print(f"Wrote {quarentine_output.count() } quarentined records to {project_id}.quarentine.dq_failures")




def main():
    if len(sys.argv) !=3:
        print("Usage: dq_validation.py <project_id> <execution_date(YYYY-MM-DD)")
        sys.exit(1)

    project_id = sys.argv[1]
    execution_date = sys.argv[2]

    spark = SparkSession.builder \
        .appName(f"DQ Validation - {execution_date}") \
        .getOrCreate()

    spark.conf.set("materializationDataset", f"{project_id}.ops")

    print(f"Starting DQ validation for date: {execution_date} in project: {project_id}")

    print("=== Starting DQ Validation trips data ===")

    clean_trips_df, quarentined_trips_df = validate_trips(spark, project_id, execution_date)



if __name__ == "__main__":
    main()






