from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, unix_timestamp, lead, array_except, array, size, split, when, array_join, first, last, collect_list
from pyspark.sql.window import Window

merged_df = spark.read.json("s3://flight-price-etl-github/merged-flight-data")

# Initialize Spark Session
spark = SparkSession.builder.appName("FlattenJSON").getOrCreate()

# Load raw JSON data from S3
## df_raw = spark.read.option("multiline", "true").json("s3://flightpricedataanalysis/flight_prices/*/*.json")

merged_df_test= spark.read.json("s3://flight-price-etl-github/merged-flight-data")

# Flatten JSON: Extract Itineraries
df_flattened = merged_df_test.select(
    col("fetch_date"),
    explode(col("data.itineraries")).alias("itinerary")
)

# Extract Price & Legs
df_itineraries = df_flattened.select(
    col("fetch_date"),
    col("itinerary.price.formatted").alias("formattedPrice"),
    explode(col("itinerary.legs")).alias("legs")
)

# Extract Flight Segment Details
df_segments = df_itineraries.select(
    col("fetch_date"),
    col("formattedPrice"),
    col("legs.departure").alias("departure"),
    col("legs.arrival").alias("arrival"),
    col("legs.durationInMinutes").alias("duration"),
    explode(col("legs.segments")).alias("segment")
).select(
    col("fetch_date"),
    col("formattedPrice"),
    col("departure"),
    col("arrival"),
    col("duration"),
    col("segment.flightNumber").alias("flightNumber"),
    col("segment.marketingCarrier.name").alias("marketingCarrier"),
    col("segment.operatingCarrier.name").alias("operatingCarrier"),
    col("segment.origin.displayCode").alias("origin"),
    col("segment.destination.displayCode").alias("destination")
)

# Window Spec for Layover Calculation
window_spec = Window.partitionBy("fetch_date", "flightNumber").orderBy("departure")

# Add Layover Information
df_with_layover = df_segments.withColumn(
    "next_departure", lead("departure").over(window_spec)
).withColumn(
    "next_arrival", lead("arrival").over(window_spec)
).withColumn(
    "layover_duration",
    (unix_timestamp("next_departure") - unix_timestamp("arrival")) / 60
)

# Group Flights & Calculate Layovers
df_grouped = df_segments.groupBy("fetch_date", "formattedPrice", "departure") \
    .agg(
        first("origin").alias("origin"),
        last("destination").alias("destination"),
        collect_list("destination").alias("layovers"),
        first("duration").alias("duration"),
        first("marketingCarrier").alias("marketingCarrier"),
        first("operatingCarrier").alias("operatingCarrier"),
        first("flightNumber").alias("flightNumber")
    ).withColumn(
        "layover",
        array_except(col("layovers"), array(col("origin"), col("destination")))
    ).withColumn(
        "layover", array_join(col("layover"), ", ")
    ).withColumn(
        "numStops", when(col("layover") == "", 0).otherwise(size(split(col("layover"), ", ")))
    )

df_final = df_grouped.select(
    "fetch_date", "flightNumber", "formattedPrice", "departure", "duration",
    "marketingCarrier", "operatingCarrier", "origin", "destination", "layover", "numStops"
)

df_final.write.mode("overwrite").csv("s3://flightpriceanalysisproject/flight_prices_test.csv", header=True)
