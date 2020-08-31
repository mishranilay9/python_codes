import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, udf
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
from datetime import date, datetime
from collections import OrderedDict

def func(row):
    temp=row.asDict()
    headDict = {}
    headDict['type'] = "record"
    headDict['name'] = "source"
    headDict['namespace'] = "com.streaming.event"
    headDict['doc'] = "SCD signals from  source123"
    fieldslist = []
    headDict['fields'] = fieldslist
    for i in temp:
        fieldslist.append({i:temp[i]})
    return (json.dumps(headDict,default=str))
def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))
if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("PythonWordCount").getOrCreate()
    payload=udf(func,StringType())
    data = spark.createDataFrame(
        [
            (1, "a", 'foo1','2020-02-03',-23.0),  # create your data here, be consistent in the types.
            (2, "b", 'bar','2020-02-03',56.789),
            (3, "c", 'mnc','2020-02-03',None)
        ],
        ['id', 'am', 'txt','dt','rate']  # add your columns label here
    )
    l=[]
    for c in data.columns:
        l.append(c)
    print(l)
    data=data.withColumn("dt",F.col("dt").cast('date'))
    df=data.withColumn("payload1",payload(struct([data[x] for x in data.columns])))
    #df=data.withColumn("payload1", payload(struct([F.when(data[x].isNotNull(), data[x]).otherwise(F.lit("")).alias(x) for x in data.columns])))
    df.select("payload1").show(3,False)