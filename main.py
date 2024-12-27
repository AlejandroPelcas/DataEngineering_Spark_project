from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
import os
import sys
from pyspark.sql.functions import *

# Create a spark context
spark_conf = SparkConf()\
  .setAppName("YourTest")\
  .setMaster("local[*]")
sc = SparkContext.getOrCreate(spark_conf)

# SparkSession is the entry point for programming with the DataFrame and Dataset APIs.
spark = SparkSession(sc)

