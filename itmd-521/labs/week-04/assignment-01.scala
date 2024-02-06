import org.apache.spark.sql.SparkSession

object DivvyTripsAnalysis {
  def main(args: Array[String]): Unit = {
    // Set master URL to local[*]
    val spark = SparkSession.builder
      .appName("DivvyTripsAnalysis")
      .master("local[*]")  // Set the master URL
      .getOrCreate()

    // Read the CSV file with inferred schema
    val dfInferred = spark.read.option("header", "true").csv("Divvy_Trips_2015-Q1.csv")

    // Print schema and count of records
    println("Schema (Inferred):")
    dfInferred.printSchema()
    println("Count of Records (Inferred):", dfInferred.count())

    // Define schema programmatically
    val schema = new org.apache.spark.sql.types.StructType()
      .add("trip_id", "int")
      .add("starttime", "string")
      .add("stoptime", "string")
      .add("bikeid", "int")
      .add("tripduration", "int")
      .add("from_station_id", "int")
      .add("from_station_name", "string")
      .add("to_station_id", "int")
      .add("to_station_name", "string")
      .add("usertype", "string")
      .add("gender", "string")
      .add("birthyear", "int")

    // Read the CSV file with defined schema
    val dfProgrammatic = spark.read.option("header", "true").schema(schema).csv("Divvy_Trips_2015-Q1.csv")

    // Print schema and count of records
    println("Schema (Programmatic):")
    dfProgrammatic.printSchema()
    println("Count of Records (Programmatic):", dfProgrammatic.count())

    // Read the CSV file with DDL schema
    val ddlSchema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"
    val dfDDL = spark.read.option("header", "true").option("inferSchema", "false").schema(ddlSchema).csv("Divvy_Trips_2015-Q1.csv")

    // Print schema and count of records
    println("Schema (DDL):")
    dfDDL.printSchema()
    println("Count of Records (DDL):", dfDDL.count())

    // Perform transformations and actions
    val selectedGender = dfDDL.select("gender")
    val filteredGender = selectedGender.filter(dfDDL("gender").startsWith("A-K") && dfDDL("gender") === "Female" || dfDDL("gender").startsWith("L-Z") && dfDDL("gender") === "Male")
    val groupedStationTo = dfDDL.groupBy("to_station_name").count()
    groupedStationTo.show(10)

    // Stop SparkSession
    spark.stop()
  }
}
