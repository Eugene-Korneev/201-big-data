import pyspark.sql.functions as spark_func
from pyspark.sql import SparkSession
from pyspark.sql import Window


# HDFS paths
HOTELS_AND_WEATHER_PATH = "/201_hw_dataset/hotels_weather"
EXPEDIA_PATH = "/201_hw_dataset/expedia"
OUTPUT_PATH = "/201_hw_dataset/expedia_validated"


def init_spark() -> SparkSession:
    spark = (SparkSession.builder
             .appName("SparkBatching")
             .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    return spark


def main() -> None:
    # init and read tables
    spark = init_spark()

    try:
        hotels_weather_df = spark.read.parquet(HOTELS_AND_WEATHER_PATH)
    except Exception as e:
        print(f"Error while reading Hotels and Weather table: {e}")
        raise e
    else:
        print("Hotels and Weather table has been read successfully")

    try:
        expedia_df = spark.read.format("com.databricks.spark.avro").load(EXPEDIA_PATH)
    except Exception as e:
        print(f"Error while reading Expedia table: {e}")
        raise e
    else:
        print("Expedia table has been read successfully")

    # Calculate idle days for every hotel
    expedia_df = expedia_df.repartition("hotel_id")
    window = Window.partitionBy("hotel_id").orderBy("hotel_id", "srch_ci")

    expedia_df = expedia_df.select("*", spark_func.lag("srch_ci").over(window).alias("previous_ci"))
    expedia_df = expedia_df\
        .withColumn("idle_days", spark_func.datediff(expedia_df["srch_ci"], expedia_df["previous_ci"]))\
        .fillna(0, subset="idle_days")\
        .drop("previous_ci")

    # Validate data
    original_columns = expedia_df.columns
    expedia_df = expedia_df\
        .withColumn("is_valid",
                    spark_func.when((spark_func.col("idle_days") >= 2) & (spark_func.col("idle_days") < 30), 0)
                    .otherwise(1))

    invalid_hotels_df = expedia_df.groupBy("hotel_id")\
                                  .agg(spark_func.min("is_valid").alias("is_valid_hotel"))\
                                  .filter(spark_func.col("is_valid_hotel") == 0)

    print(f"Found invalid hotels qty: {invalid_hotels_df.count()}")

    hotels_info_df = hotels_weather_df["id", "name", "country", "city", "address", "lat", "lng", "geohash"]\
        .dropDuplicates()

    print("Invalid hotels info:")
    invalid_hotels_df\
        .join(hotels_info_df, invalid_hotels_df["hotel_id"] == hotels_info_df["id"], "left")\
        .select(hotels_info_df.columns)\
        .show()

    print("Subtracting invalid hotels data from Expedia table")
    length_before_subtracting = expedia_df.count()
    print(f"Rows qty before: {length_before_subtracting}")

    valid_hotels_df = expedia_df \
        .join(invalid_hotels_df,
              expedia_df["hotel_id"] == invalid_hotels_df["hotel_id"].alias("invalid_id"),
              "left_anti")[original_columns]

    length_after_subtracting = valid_hotels_df.count()
    print(f"Rows qty after: {length_after_subtracting}")
    print(f"Qty of invalid rows removed: {length_before_subtracting - length_after_subtracting}")

    # Group data to calculate bookings counts
    valid_hotels_with_locations_df = valid_hotels_df\
        .join(hotels_info_df, valid_hotels_df["hotel_id"] == hotels_info_df["id"], "left")\
        .select("hotel_id", "country", "city")

    print("Bookings counts by hotel country:")
    valid_hotels_with_locations_df.groupBy("country").count().withColumnRenamed("count", "bookings_counts").show()

    print("Bookings counts by hotel city:")
    valid_hotels_with_locations_df.groupBy("city").count().withColumnRenamed("count", "bookings_counts").show()

    # Store valid Expedia data in parquet partitioned by year of srch_ci
    try:
        valid_hotels_df \
            .withColumn('year', spark_func.year("srch_ci")) \
            .write.parquet(OUTPUT_PATH, mode="overwrite", partitionBy="year")
    except Exception as e:
        print(f"Error while writing to parquet: {e}")
        raise e
    else:
        print("Table has been written to parquet successfully")


if __name__ == '__main__':
    main()
