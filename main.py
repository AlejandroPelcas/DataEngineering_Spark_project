from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
import os
import sys
from pyspark.sql.functions import *
from pyspark.sql.functions import count, col, desc

os.environ["JAVA_HOME"] = "/Users/alejandropelcastre/Library/Java/JavaVirtualMachines/openjdk-22.0.1/Contents/Home"


# Create a spark context
spark_conf = SparkConf()\
  .setAppName("YourTest")\
  .setMaster("local[*]")
sc = SparkContext.getOrCreate(spark_conf)

# SparkSession is the entry point for programming with the DataFrame and Dataset APIs.
spark = SparkSession(sc)
df = spark.read.csv("mnm_dataset.csv" , header=True, inferSchema=True)
#  Data looks like:  (Size = 100,000 Rows)
#  [ State | Color | Count ]
#  [ State | Color | Count ]
#  ...

# Show the 60 highest counts of pairs (State, Color) in descending order
count_mnm_df = (df
                .select("State", "Color", "Count")
                .groupby("State", "Color")
                .agg(count("Count").alias("Total"))
                .orderBy("Total",ascending=False))
count_mnm_df.show(n=60, truncate=False)

# Use high level spark API to create a simple agg table:
df_ages = spark.createDataFrame([("Ale", 28), ("Abby", 26), ("Bobby", 61),
                                   ("Ale", 42), ("Jules", 30)],
                                  ["Name", "Age"])
avg_df = df_ages.groupby("Name").agg(avg("Age").alias("Avg"))
avg_df.show()

# Use SQL commands on df
(avg_df.select("Name")
 .where("Avg > 30")).show()
