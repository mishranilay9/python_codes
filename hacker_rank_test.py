from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import struct
from pyspark.sql.window import Window
from pyspark.sql.types import *
def js(lis,ls):
    def func(row):
        print(ls)
        return lis[int(row.id)-1]
    return F.udf(func,IntegerType())
if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("PythonWordCount").getOrCreate()
    data=spark.createDataFrame(
        [
            (10,1),  # create your data here, be consistent in the types.
            (10,2),
            (10, 3),
            (10,4)
        ],
        ['vl','id']  # add your columns label here
    )
    lis=[]
    s=data.rdd.map(lambda x: x.vl).collect()
    for i in s:
        lis.append(int(i))
    print(lis)
    cnt=0
    cumlis=[]
    for x in lis:
        vln=x+cnt
        cumlis.append(vln)
        cnt=vln
    print(cumlis)
    w = Window.orderBy(F.col('id'))
    data = data.withColumn('maximum', F.row_number().over(w))
    data = data.withColumn("payload1", js(cumlis,lis)(struct([data[x] for x in data.columns])))
    data.show()


