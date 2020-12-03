from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import struct
from pyspark.sql.window import Window
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
    #data=data.withColumn("CumSum",cumlis[int(F.col("maximum"))])
    data.show()
    # data.show(7,False)
    # data=data.withColumn("dt",F.col("dt").cast("date"))
    # data.printSchema()
    # wind=Window.partitionBy("id")
    # data = data.withColumn("maxVersion", F.max("version").over(wind)) \
    #     .where(F.col("version") == F.col("maxVersion")) \
    #     .withColumn("maxDt", F.max("dt").over(wind)) \
    #     .where(F.col("maxDt") == F.col("dt")) \
    #     .drop(F.col("maxVersion")) \
    #     .drop(F.col("maxDt"))
    # data.show(5,False)
    #
    # windowedDF = data.repartition(F.col("id")) \
    #                  .select(F.col("id"),F.col("dt"),F.col("version"),F.col("Name"),
    #                         F.collect_set(struct(F.col("id"), F.col("Name"))).over(wind).alias("test1")
    # )
    # windowedDF.show(2, False)

    # w = Window.partitionBy("id").orderBy(F.col('version').desc(), F.col('dt').desc())
    # data=data.withColumn('maximum', F.row_number().over(w))#.filter('maximum = 1').drop('maximum').show()
    # data.show(8,False)

