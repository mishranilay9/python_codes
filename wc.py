from pyspark.sql import SparkSession
from operator import add
def getOrElse(x):
  return x.replace(",","") if x is not None else 0
if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("PythonWordCount").getOrCreate()
    data = spark.read.csv("C:\\Study\\Spark\\Worldbank\\World_Bank_Indicators.csv").rdd
    # result = data.map(lambda x: (x[0], int(getOrElse(x[9])))) \
    #     .reduceByKey(add).sortBy(lambda x: x[1])
    # output = result.take(10)
    # for (cnt,op) in output:
    #     print("{} country has {} no of peoples\n".format(cnt,op))

    # result = data.map(lambda x: (x[0], int(getOrElse(x[9])))) \
    #     .groupByKey().map(lambda x: (x[0],max(x[1])-min(x[1])))
    # output = result.take(10)
    # for (cnt, op) in output:
    #      print("{} country  {} no of peoples\n".format(cnt, op))

    result = data.filter(lambda x: str(x[0])=="Afghanistan") \
            .map(lambda x:(x[0],x[1]))
    output = result.take(10)
    for (cnt, op) in output:
         print("{} country  {} no of peoples\n".format(cnt, op))






    
