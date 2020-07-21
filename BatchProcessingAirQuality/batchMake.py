from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, avg, col
import pyspark.sql.functions as SQLFunctions
import os
import time

os.environ["PYSPARK_PYTHON"] = '/usr/bin/python3'
spark = SparkSession.builder.getOrCreate()
# Load data from a CSV
filePath = "/home/varun/PycharmProjects/BatchProcessingAirQuality/weatherAUS.csv"
df = spark.read.format("CSV").option("inferSchema", True).option("header", True).load(filePath)
df = df.withColumn("Date", expr("to_date(Date)"))
print(df.show(5))

# time.sleep(10)
# Average rainfall overall
avgRain = df.filter(SQLFunctions.col('Date') >= '2008-12-01').select(
    SQLFunctions.round(avg('Rainfall'), 2).alias("Avg. Rainfall")).show()
# time.sleep(10)

# Min and Max Temperatures where MaxTemp >= 10
Temp = df.filter(SQLFunctions.col('MaxTemp') >= '10').select('Date', 'MinTemp', 'MaxTemp').dropDuplicates(
    subset=['MaxTemp']).show(5)
# time.sleep(10)

# Average Temperature of the day where Wind Direction is North
meanCols = [col('MaxTemp'), col('MinTemp')]
avgCol = sum(x for x in meanCols) / len(meanCols)
avgTempOfDay = df.filter(SQLFunctions.col('WindGustDir') == 'N').select('Date', 'MinTemp', 'MaxTemp', 'RainTomorrow',
                                                                        avgCol.alias('Avg(Temp)'))
# time.sleep(10)
# Writing the dataframe
avgTempOfDay.write.save("AvgTempOfDay")
# time.sleep(10)
avgTempOfDay.show(5)
# time.sleep(60)