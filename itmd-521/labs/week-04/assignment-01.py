from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Divvy Trips Analysis") \
    .getOrCreate()

# Read the CSV file with inferred schema
df_inferred = spark.read.csv("Divvy_Trips_2015-Q1.csv", header=True, inferSchema=True)

# Print schema and count of records
print("Schema (Inferred):")
df_inferred.printSchema()
print("Count of Records (Inferred):", df_inferred.count())

# Define schema programmatically
schema = StructType([
    StructField("trip_id", IntegerType(), True),
    StructField("starttime", StringType(), True),
    StructField("stoptime", StringType(), True),
    StructField("bikeid", IntegerType(), True),
    StructField("tripduration", IntegerType(), True),
    StructField("from_station_id", IntegerType(), True),
    StructField("from_station_name", StringType(), True),
    StructField("to_station_id", IntegerType(), True),
    StructField("to_station_name", StringType(), True),
    StructField("usertype", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birthyear", IntegerType(), True)
])

# Read the CSV file with defined schema
df_programmatic = spark.read.csv("Divvy_Trips_2015-Q1.csv", header=True, schema=schema)

# Print schema and count of records
print("Schema (Programmatic):")
df_programmatic.printSchema()
print("Count of Records (Programmatic):", df_programmatic.count())

# Read the CSV file with DDL schema
ddl_schema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"
df_ddl = spark.read.csv("Divvy_Trips_2015-Q1.csv", header=True, schema=ddl_schema)

# Print schema and count of records
print("Schema (DDL):")
df_ddl.printSchema()
print("Count of Records (DDL):", df_ddl.count())

# Perform transformations and actions
selected_gender = df_ddl.select("gender")
filtered_gender = selected_gender.filter((df_ddl["gender"].startswith("A-K") & (df_ddl["gender"] == "Female")) | (df_ddl["gender"].startswith("L-Z") & (df_ddl["gender"] == "Male")))
grouped_station_to = df_ddl.groupBy("to_station_name").count()
grouped_station_to.show(10)

# Stop SparkSession
spark.stop()
