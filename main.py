from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
import os
import sys
from pyspark.sql.functions import *

os.environ["JAVA_HOME"] = "/Users/alejandropelcastre/Library/Java/JavaVirtualMachines/openjdk-22.0.1/Contents/Home"


# Create a spark context
spark_conf = SparkConf()\
  .setAppName("YourTest")\
  .setMaster("local[*]")
sc = SparkContext.getOrCreate(spark_conf)

# SparkSession is the entry point for programming with the DataFrame and Dataset APIs.
spark = SparkSession(sc)
df = spark.read.csv("mnm_dataset.csv" , header=True, inferSchema=True)
x = df.take(3)
print(x)