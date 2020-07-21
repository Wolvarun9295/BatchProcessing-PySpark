import re
from pyspark.sql.types import Row
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

df = spark.read.csv("Employees_Position_Salaries.csv")
time.sleep(10)
# reading our csv file, we can check if it has been correctly with:
df.head(5)
time.sleep(10)

# then it is parsed to a row object
def analyse(file):
    return Row(name=file[0], position=file[1], department=file[2], salary=file[3])


analysis = df.rdd.map(analyse)
time.sleep(10)
# we omit the first line, just a header with no data
header = analysis.first()
time.sleep(10)
noHeader = analysis.filter(lambda x: x != header).cache()
time.sleep(10)


# Now we calculate the average salary.
# First we have to filter all the valid fields applying the filter "isSalary" to the not null values.
# Then the "$" is removed and we calculate the average
def isSalary(amount):
    pattern = '\$(\d+)\.(\d+)'
    return re.search(pattern, amount)


avgSalary = noHeader.map(lambda x: x.salary).filter(lambda x: x is not None).filter(isSalary).map(
    lambda x: float(x.replace("$", ""))).mean()
print("The average salary is: ${}".format(round(avgSalary, 2)))
time.sleep(10)

# Now we look for the top-3 popular departments
topDepartment = noHeader.map(lambda x: (x.department, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda (k, v): -v)
print("The top 3 popular departments are: {}".format(topDepartment.take(3)))
time.sleep(10)

# And now we get the top-3 people with the highest salary
topSalaries = noHeader.map(lambda x: (x, x.salary)).filter(lambda x: isSalary(str(x[1]))).map(
    lambda x: (x[0], float(x[1].replace("$", "")))).sortBy(lambda (k, v): -v)
print("The top 3 people with the highest salaries are: {}".format(topSalaries.take(3)))
time.sleep(30)