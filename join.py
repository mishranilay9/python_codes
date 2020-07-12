from pyspark.sql import SparkSession
if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("PythonWordCount").getOrCreate()
    df1=spark.createDataFrame(
        [
            (1,"a",'foo','2010-08-03','2099-01-01'),  # create your data here, be consistent in the types.
            (2,"b",'bar','2010-09-04','2099-01-01'),
            (4,"d","yui",'2010-10-10','2099-01-01')
        ],
        ['id','nm','txt','from_dt','to_dt']  # add your columns label here
    )
    df1.createOrReplaceTempView("df1")
    df2= spark.createDataFrame(
        [
            (1,"a",'foo1'),  # create your data here, be consistent in the types.
            (2,"b",'bar'),
            (3,"c",'mnc')
        ],
        ['id','nm','txt']  # add your columns label here
    )
    df2.createOrReplaceTempView("df2")
    query="""SELECT * FROM df1 JOIN df2 ON df1.id=df2.id"""
    spark.sql(query).show()

    df1.join(df2, on=['id'], how='inner').show()

