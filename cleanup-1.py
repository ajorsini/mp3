import csv
from pyspark.sql.types import StructType
from pyspark.sql.types import *
# from StringIO import StringIO
from datetime import datetime
from collections import namedtuple
from operator import add, itemgetter
from pyspark import SparkConf, SparkContext


# Constants
APP_NAME = "Flight Delay Analysis"
DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H%M"

schema = StructType() \
      .add("Year", IntegerType(), False) \
      .add("Quarter", IntegerType(), False) \
      .add("Month", IntegerType(), False) \
      .add("DayofMonth", IntegerType(), False) \
      .add("DayOfWeek", IntegerType(), False) \
      .add("FlightDate", DateType(), False) \
      .add("UniqueCarrier", StringType(), False) \
      .add("AirlineID", IntegerType(), False) \
      .add("Carrier", StringType(), False) \
      .add("TailNum", StringType(), False) \
      .add("FlightNum", IntegerType(), False) \
      .add("Origin", StringType(), False) \
      .add("Dest", StringType(), False) \
      .add("CRSDepTime", IntegerType(), False) \
      .add("DepTime", IntegerType(), False) \
      .add("DepDelay", IntegerType(), False) \
      .add("DepDelayMinutes", IntegerType(), False) \
      .add("CRSArrTime", IntegerType(), False) \
      .add("ArrTime", IntegerType(), False) \
      .add("ArrDelay", IntegerType(), False) \
      .add("ArrDelayMinutes", IntegerType(), False)

df = spark.read.format("csv") \
      .option("header", True) \
      .option("mode", "DROPMALFORMED") \
      .schema(schema) \
      .load("./ontime_sample.csv")

df = df.dropDuplicates()
print("Distinct count: "+str(df.count()))
df.show(truncate=False)
