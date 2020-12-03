import json
from pyspark.sql.functions import struct
from pyspark.sql.types import *
from pyspark.sql import functions as F,SparkSession
def js():
    def func(row):
        print(row)
        temp=row.asDict()
        headDict = {}
        fieldslist = []
        headDict['record'] = "record"
        headDict['name'] = temp["nm"]
        headDict['grp'] = fieldslist
        fieldslist.append((temp["payload1"]))
        return (json.dumps(headDict,default=str))
    return F.udf(func,StringType())
def js1():
    def func(row):
        temp=row.asDict()
        headDict = {}
        headDict['type'] = temp["nm"]
        smalldict={}
        smalldict['id'] = temp["id"]
        smalldict['txt'] = temp["txt"]
        headDict['val'] = smalldict
        return (json.dumps(headDict,default=str))
    return F.udf(func,StringType())
if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("PythonWordCount").getOrCreate()
    data = spark.createDataFrame(
        [
            (1, "a", 'foo1'),
            (1, "b", 'bar'),
            (2, "a", 'mnc'),
            (2, "b", 'mnc')
        ],
        ['id', 'nm', 'txt']
    )

    data = data.withColumn("payload1", js1()(struct([data[x] for x in data.columns])))
    #data1=data.groupBy("").agg(*[F.collect_list("payload1").alias("payload1")])
    data1=data.groupBy("nm").agg(F.concat_ws(",",F.collect_list("payload1")).alias("payload1"))
    data2 = data1.withColumn("payload2", js()(struct([data1[x] for x in data1.columns])))
    data2.select("payload2").show(2,False)

