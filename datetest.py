import datetime
from pyspark import Row
from pyspark.sql import functions as F,SparkSession
from pyspark.sql.functions import md5, to_date


def func(row):
    primary_key = "id,nm"  # list of primary KEYS#
    rt = primary_key.split(",")
    temp=row.asDict()
    temp["concat_val"] = "|".join([str(temp[x]) for x in temp if x not in rt])
    put=Row(**temp)
    return put
def add_md5(target_df):
    row_rdd=target_df.rdd.map(func)
    concat_df=row_rdd.toDF()
    hash_df=concat_df.withColumn("hash_id",md5(F.col("concat_val")))#.drop(F.col("concat_val"))
    return hash_df



def perform_delta_identification(current_df, previous_hash_df, primary_key):

    primary_key_list = primary_key.split(",")
    current_dt = datetime.date.today().strftime('%Y-%m-%d')
    long_gt=datetime.datetime.strptime("2099-01-01",'%Y-%m-%d').strftime('%Y-%m-%d')

    #New inserted records detection
    insert_records = previous_hash_df.alias('source').join(current_df.alias('target'),
                                                       [previous_hash_df[v] == current_df[v]
                                                        for v in primary_key_list], how="right_outer") \
    .filter(previous_hash_df['hash_id'].isNull()) \
    .select([F.col('target.' + xx) for xx in current_df.columns])\
        .withColumn("from_dt",F.lit(current_dt)).withColumn("to_dt",F.lit(long_gt)).withColumn("active_flag",F.lit("A")).drop('hash_id')

    # Updated records detection
    update_records = previous_hash_df.alias('source').join(current_df.alias('target'),
                                                       [current_df[v] == previous_hash_df[v]
                                                        for v in primary_key_list], how="inner") \
    .filter(previous_hash_df['hash_id'] != current_df['hash_id'])
    new_recs=update_records.select([F.col('target.' + xx) for xx in current_df.columns]) \
        .withColumn("from_dt", F.lit(current_dt)).withColumn("to_dt", F.lit(long_gt)).withColumn("active_flag",F.lit("A")).drop('hash_id')

    old_recs = update_records.select([F.col('source.' + xx) for xx in previous_hash_df.columns]) \
        .withColumn("to_dt", F.lit(current_dt)).withColumn("active_flag",F.lit("NA")).drop('hash_id')


    delete_records = previous_hash_df.alias('source').join(current_df.alias('target'),
                                                       [previous_hash_df[v] == current_df[v]
                                                        for v in primary_key_list], how="left_outer"). \
    filter(current_df['hash_id'].isNull()).select([F.col('source.' + xx) for xx in previous_hash_df.columns]) \
        .withColumn("to_dt",F.lit(current_dt)).withColumn("active_flag",F.lit("NA")).drop('hash_id')

    final_df = insert_records.unionAll(new_recs).unionAll(old_recs).unionAll(delete_records)
    return final_df


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
    long_gt = datetime.datetime.strptime("2099-01-01", '%Y-%m-%d').strftime('%Y-%m-%d')
    print(long_gt)
    primary_key = "id,nm" #list of primary KEYS#
    primary_key_list=primary_key.split(",")


    #Filter only records where end_date=2099-01-01
    publish_data1=publish_data_before_filter.filter(to_date('to_dt') == long_gt)
    publish_data=publish_data1.drop('from_dt').drop('to_dt')

    Publish_hashed_df = add_md5(publish_data)
    #Adding again the from_dt
    Publish_hashed_df_final = Publish_hashed_df.alias('source').join(publish_data1.alias('target'),
                                                           [Publish_hashed_df[v] == publish_data1[v]
                                                            for v in primary_key_list], how="inner")\
        .select([F.col('source.' + xx) for xx in Publish_hashed_df.columns]+[F.col('target.from_dt')])


    landing_hased_df = add_md5(landing_data)
    gyu=perform_delta_identification(landing_hased_df,Publish_hashed_df_final,primary_key)
    gyu.show()

