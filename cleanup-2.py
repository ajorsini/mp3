import csv
from pyspark.sql.functions import col
from pyspark.sql.types import *
# from StringIO import StringIO
from datetime import datetime
from collections import namedtuple
from operator import add, itemgetter
from pyspark import SparkConf, SparkContext


# Constants
APP_NAME = "Flight Analysis"
DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H%M"


df = spark.read.format("csv") \
      .option("header", True) \
      .load("./ontime_sample.csv")

df = df.select( \
             'Year', \
             'Quarter', \
             'Month', \
             'DayofMonth', \
             'DayOfWeek', \
             'FlightDate', \
             'UniqueCarrier', \
             'AirlineID', \
             'Carrier', \
             'TailNum', \
             'FlightNum', \
             'Origin', \
             'Dest', \
             'CRSDepTime', \
             'DepTime', \
             'DepDelay', \
             'DepDelayMinutes', \
             'CRSArrTime', \
             'ArrTime', \
             'ArrDelay', \
             'ArrDelayMinutes')

df = df.dropDuplicates()

hhmmType = lambda x: datetime.strptime(x, TIME_FMT).time()

df = df \
  .withColumn('Year', df.Year.cast(IntegerType())) \
  .withColumn('Quarter', df.Quarter.cast(IntegerType())) \
  .withColumn('Month', df.Month.cast( IntegerType())) \
  .withColumn('DayofMonth', df.DayofMonth.cast(IntegerType())) \
  .withColumn('DayOfWeek', df.DayOfWeek.cast(IntegerType())) \
#  .withColumn('FlightDate', datetime.strptime(df.FlightDate, DATE_FMT).date()) \
  .withColumn('FlightDate', df.FlightDate.cast(DateType())) \
  .withColumn('UniqueCarrier', df.UniqueCarrier.cast(StringType())) \
  .withColumn('AirlineID', df.AirlineID.cast(IntegerType())) \
  .withColumn('Carrier', df.Carrier.cast(StringType())) \
  .withColumn('TailNum', df.TailNum.cast(StringType())) \
  .withColumn('FlightNum', df.FlightNum.cast(IntegerType())) \
  .withColumn('Origin', df.Origin.cast(StringType())) \
  .withColumn('Dest', df.Dest.cast(StringType())) \
  .withColumn('CRSDepTime', df.CRSDepTime.cast(IntegerType())) \
#  .withColumn('DepTime', datetime.strptime(col('DepTime'), TIME_FMT).time()) \
  .withColumn('DepTime', hhmmType(df.DepTime)) \
  .withColumn('DepDelay', df.DepDelay.cast(IntegerType())) \
  .withColumn('DepDelayMinutes', df.DepDelayMinutes.cast(IntegerType())) \
  .withColumn('CRSArrTime', df.CRSArrTime.cast(IntegerType())) \
  .withColumn('ArrTime', datetime.strptime(df.ArrTime, TIME_FMT).time()) \
  .withColumn('ArrDelay', df.ArrDelay.cast(IntegerType())) \
  .withColumn('ArrDelayMinutes', df.ArrDelayMinutes.cast(IntegerType()))

print("Distinct count: "+str(df.count()))
df.show(truncate=False)
