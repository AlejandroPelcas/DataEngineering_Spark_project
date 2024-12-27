from pyspark.sql import SparkSession
from pyspark.sql.types import LongType

spark = (SparkSession
         .builder
         .appName("SparkSQLExampleApp")
         .getOrCreate())

csv_file = "departuredelays.csv"

df = (spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csv_file))
# createOrReplaceTempView function in Apache Spark is used to register a DataFrame
# as a temporary view within the Spark session.
# This allows you to query the DataFrame using SQL syntax, just like querying a table in a database.
df.createOrReplaceTempView("us_delay_flights_tbl")

# Simple Query
spark.sql("""SELECT distance, origin, destination
FROM us_delay_flights_tbl WHERE distance > 1000
ORDER BY distance DESC""").show(10)
# Complex Query
spark.sql("""SELECT delay, origin, destination,
             CASE
                WHEN delay > 360 THEN 'Very Long Delays'
                WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
                WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
                WHEN delay = 0 THEN 'No Delays'
                ELSE 'Early'
            END AS Flight_Delays
            FROM us_delay_flights_tbl
            ORDER BY origin, delay DESC""").show(20)
# Simple and Complex Query with simpler syntax
(df.select("distance", "origin", "destination")
 .where("distance > 1000")
 .orderBy("distance", ascending=False)).show(2)

# You can create your on User Defined Functions as well (UDF)
def cubed(s):
    return s**3
# Register the UDF and then SQL query from a temporary view [You could also use pandas 'pd' to speed up]
spark.udf.register("cubed", cubed, LongType())
spark.range(1,9).createOrReplaceTempView("udf_test")
spark.sql("""SELECT id, cubed(id) AS id_cubed FROM udf_test""").show()

# [TODO:] Learn to use SQL in command-line. Might need to configure $SPARK_HOME

