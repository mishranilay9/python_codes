from pyspark import Row
from pyspark.sql import functions as F,SparkSession
from pyspark.sql.functions import md5, to_date


def func(row):
    """primary_key = "id,nm"  # list of primary KEYS#
    rt = primary_key.split(",")"""
    temp=row.asDict()
    #temp["concat_val"] = "|".join([str(temp[x]) for x in temp])
    #put=Row(**temp)
    return temp
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

    publish_data_before_filter=spark.createDataFrame(
        [
            (1,"a",'foo','2010-08-03','2099-01-01'),  # create your data here, be consistent in the types.
            (2,"b",'bar','2010-09-04','2099-01-01'),
            (4,"d","yui",'2010-10-10','2099-01-01')
        ],
        ['id','nm','txt','from_dt','to_dt']  # add your columns label here
    )
    landing_data = spark.createDataFrame(
        [
            (1,"a",'foo1'),  # create your data here, be consistent in the types.
            (2,"b",'bar'),
            (3,"c",'mnc')
        ],
        ['id','nm','txt']  # add your columns label here
    )
    text=landing_data.rdd.map(func)
    text.foreach(print)
    text1 = landing_data.rdd.map(lambda x:x.asDict())
    text1.foreach(print)

