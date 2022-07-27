## Spark Application - execute with spark-submit
## Imports
import csv
from datetime import datetime
from collections import namedtuple
from operator import add, itemgetter
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark import SparkConf, SparkContext

## Module Constants

APP_NAME = "Flight Analysis"
DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H%M"

fields   = ('Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek', 'FlightDate',
            'UniqueCarrier', 'AirlineID', 'Carrier', 'TailNum', 'FlightNum',
            'Origin', 'Dest', 'CRSDepTime', 'DepTime', 'DepDelay', 'DepDelayMinutes',
            'CRSArrTime', 'ArrTime', 'ArrDelay', 'ArrDelayMinutes')

Flight   = namedtuple('Flight', fields)

## Closure Functions
def parse(row):
    try:
        row[0]  = int(row[0])
        row[1]  = int(row[1])
        row[2]  = int(row[2])
        row[3]  = int(row[3])
        row[4]  = int(row[4])
        row[5]  = datetime.strptime(row[5], DATE_FMT).date()
        row[6]  = row[6][1:-1]
        row[7]  = int(row[7])
        row[8]  = row[8][1:-1]
        row[9]  = row[9][1:-1]
        row[10]  = row[10][1:-1]
        row[11]  = row[11][1:-1]
        row[18]  = row[18][1:-1]
        row[25] = datetime.strptime(row[25][1:-1], TIME_FMT).time()
        row[26] = datetime.strptime(row[26][1:-1], TIME_FMT).time()
        row[27] = float(row[27])
        row[28] = float(row[28])
        row[36] = datetime.strptime(row[36][1:-1], TIME_FMT).time()
        row[37] = datetime.strptime(row[37][1:-1], TIME_FMT).time()
        row[38] = float(row[38])
        row[39] = float(row[39])
    except Exception as e:
        row  = [None] * 40
    return Flight(*row[:12], row[18], *row[25:29], *row[36:40])

# conf = SparkConf().setMaster("local[*]").setAppName(APP_NAME)
# sc   = SparkContext(conf=conf)

dfFlights = sc.textFile("./airline_ontime/2008/On_Time_On_Time_Performance_2008_1.csv").zipWithIndex().filter(lambda row_index: row_index[1] > 0).keys() \
             .map(lambda line: line.split(",")) \
             .map(lambda r: parse(r)).toDF().na.drop("all").dropDuplicates()

dfFlights.show(truncate=False)

dfFlights.rdd.foreach(lambda r: print(
            "Year: ", r.Year,
            "Quarter: ", r.Quarter,
            "Month: ", r.Month,
            "DayofMonth: ", r.DayofMonth,
            "DayOfWeek: ", r.DayOfWeek,
            "FlightDate: ", r.FlightDate,
            "UniqueCarrier: ", r.UniqueCarrier,
            "AirlineID: ", r.AirlineID,
            "Carrier: ", r.Carrier,
            "TailNum: ", r.TailNum,
            "FlightNum: ", r.FlightNum,
            "Origin: ", r.Origin,
            "Dest: ", r.Dest,
            "CRSDepTime: ", r.CRSDepTime,
            "DepTime: ", r.DepTime,
            "DepDelay: ", r.DepDelay,
            "DepDelayMinutes: ", r.DepDelayMinutes,
            "CRSArrTime: ", r.CRSArrTime,
            "ArrTime: ", r.ArrTime,
            "ArrDelay: ", r.ArrDelay,
            "ArrDelayMinutes: ", r.ArrDelayMinutes))

airports = dfFlights.rdd.map(lambda x: (x.Origin, 1)) + dfFlights.rdd.map(lambda x: (x.Dest, 1))
airports = airports.reduceByKey(add).collect()
airports  = sorted(airports, key=itemgetter(1), reverse=True)
for a, n in airports:
    print(a, " -> ", n)

airlines_ontime = dfFlights.rdd \
    .map(lambda r: (r.Carrier, (r.ArrDelay, 1))) \
    .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
    .map(lambda x: (x[0], x[1][0]/x[1][1])) \
    .collect()
airlines_ontime = sorted(airlines_ontime, key=itemgetter(1))
for a, n in airlines_ontime:
    print(a, " -> ", n)

WeekDays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
weekdays_ontime = dfFlights.rdd \
    .map(lambda r: (WeekDays[r.DayOfWeek-1], (r.ArrDelay, 1))) \
    .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
    .map(lambda x: (x[0], x[1][0]/x[1][1])) \
    .collect()
weekdays_ontime = sorted(weekdays_ontime, key=itemgetter(1))
for a, n in weekdays_ontime:
    print(a, " -> ", n)

AirPt_OrigDepDly = dfFlights.rdd \
    .map(lambda r: ((r.Origin, r.Carrier), (r.DepDelay, 1))) \
    .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
    .map(lambda x: (x[0], x[1][0]/x[1][1])) \
    .collect()
AirPt_OrigDepDly = sorted(AirPt_OrigDepDly, key=lambda x: (x[0][0], x[1]))
for a, n in AirPt_OrigDepDly:
    print(a, " -> ", n)

OrigDestCarr_ArrDly = dfFlights.rdd \
    .map(lambda r: ((r.Origin, r.Dest, r.Carrier), (r.ArrDelay, 1))) \
    .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
    .map(lambda x: (x[0], x[1][0]/x[1][1])) \
    .collect()
OrigDestCarr_ArrDly = sorted(OrigDestCarr_ArrDly, key=lambda x: (x[0][0], x[0][1], x[1]))
for a, n in OrigDestCarr_ArrDly:
    print(a, " -> ", n)

selCols = ['Origin', 'Dest', 'Carrier', 'FlightNum', 'FlightDate', 'DepTime', 'ArrDelay', 'ArrDelayMinutes']
l1cols = [col(col_name).alias("l1_" + col_name)  for col_name in selCols]
l2cols = [col(col_name).alias("l2_" + col_name)  for col_name in selCols]
w = Window.orderBy("AccArrDlyMts", "AccArrDly")
df1Leg = dfFlights.select(*l1cols).where(dfFlights.DepTime < datetime.strptime("1200", TIME_FMT))
df2Leg = dfFlights.select(*l2cols).where(dfFlights.DepTime >= datetime.strptime("1200", TIME_FMT))
bestOpt = df1Leg.join(df2Leg, df1Leg.l1_Dest ==  df2Leg.l2_Origin, "inner") \
                .withColumn("AccArrDly", df1Leg.l1_ArrDelay+df2Leg.l2_ArrDelay) \
                .withColumn("AccArrDlyMts", df1Leg.l1_ArrDelayMinutes+df2Leg.l2_ArrDelayMinutes) \
                .withColumn("row", row_number().over(w)) \
                .where(df1Leg.l1_FlightDate == df2Leg.l2_FlightDate+2) \
                .filter(col("row") == 1).drop("row") \
                .collect()



def plot(delays):
    """
    Show a bar chart of the total delay per airline
    """
    airlines = [d[0] for d in delays]
    minutes  = [d[1] for d in delays]
    index    = list(xrange(len(airlines)))
    fig, axe = plt.subplots()
    bars = axe.barh(index, minutes)
    # Add the total minutes to the right
    for idx, air, min in zip(index, airlines, minutes):
        if min > 0:
            bars[idx].set_color('#d9230f')
            axe.annotate(" %0.0f min" % min, xy=(min+1, idx+0.5), va='center')
        else:
            bars[idx].set_color('#469408')
            axe.annotate(" %0.0f min" % min, xy=(10, idx+0.5), va='center')
    # Set the ticks
    ticks = plt.yticks([idx+ 0.5 for idx in index], airlines)
    xt = plt.xticks()[0]
    plt.xticks(xt, [' '] * len(xt))
    # minimize chart junk
    plt.grid(axis = 'x', color ='white', linestyle='-')
    plt.title('Total Minutes Delayed per Airline')
    plt.show()

## Main functionality

def main(sc):

    # Load the airlines lookup dictionary

    airlines = dict(sc.textFile("ontime/airlines.csv").map(split).collect())

    # Broadcast the lookup dictionary to the cluster

    airline_lookup = sc.broadcast(airlines)

    # Read the CSV Data into an RDD

    flights = sc.textFile("ontime/flights.csv").map(split).map(parse)

    # Map the total delay to the airline (joined using the broadcast value)

    delays  = flights.map(lambda f: (airline_lookup.value[f.airline],

                                     add(f.dep_delay, f.arv_delay)))

    # Reduce the total delay for the month to the airline

    delays  = delays.reduceByKey(add).collect()

    delays  = sorted(delays, key=itemgetter(1))

    # Provide output from the driver

    for d in delays:

        print "%0.0f minutes delayed\t%s" % (d[1], d[0])

    # Show a bar chart of the delays

    plot(delays)

if __name__ == "__main__":

    # Configure Spark

    conf = SparkConf().setMaster("local[*]")

    conf = conf.setAppName(APP_NAME)

    sc   = SparkContext(conf=conf)

    # Execute Main functionality

    main(sc)
