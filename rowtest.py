from pyspark import Row
from pyspark.sql import functions as F,SparkSession
from pyspark.sql.functions import md5, to_date


def func(row):
    temp=row.asDict()
    l=[]
    for key, value in temp.iteritems():
        print("{}-->{}".format(key,value))
        #l.append("{} : {}".format(key,value))


def add_md5(target_df):
    row_rdd=target_df.rdd.map(func)
    concat_df=row_rdd.toDF()
    hash_df=concat_df.withColumn("hash_id",md5(F.col("concat_val")))#.drop(F.col("concat_val"))
    return hash_df
if __name__ == "__main__":


    spark = SparkSession\
        .builder.\
        master("local[*]")\
        .appName("PythonWordCount")\
        .getOrCreate()


    data = spark.createDataFrame(
        [
            (1,"a",'foo1'),  # create your data here, be consistent in the types.
            (2,"b",'bar'),
            (3,"c",'mnc')
        ],
        ['id','nm','txt']  # add your columns label here
    )

    data.rdd.map(func).collect()

    #text1 = data.rdd.map(lambda x:x.asDict())
    #text1.foreach(print)

