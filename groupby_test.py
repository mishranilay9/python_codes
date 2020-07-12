from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("PythonWordCount").getOrCreate()
    df = spark.createDataFrame(
        [
            (1,"a",'foo1',4,5),  # create your data here, be consistent in the types.
            (2,"b",'bar',4,6),
            (3,"c",'mnc',4,7),
            (3, "c", 'mnc', 4, 7)
        ],
        ['a','b','c','d','e']
    )# add your columns label here
    df.createOrReplaceTempView("table")
    query="""
    SELECT 
        x.a as a_1,
        x.b as b_1,
        x.c as c_1,
        x.d as d_1,
        x.e as e_1 FROM (SELECT a,b,c,d,e,
    ROW_NUMBER() OVER (PARTITION BY d, e order by d,e) as cnt FROM table  ) x
    WHERE cnt = 1
    """
    query2="""SELECT a,b,c,d,e,
    ROW_NUMBER() OVER (PARTITION BY d, e order by d,e) as cnt FROM table
    """
    query1="""
    SELECT 
        x.a as a_1,
        x.b as b_1,
        x.c as c_1,
        x.d as d_1,
        x.e as e_1 FROM (SELECT a,b,c,d,e,
    ROW_NUMBER() OVER (PARTITION BY a order by a) as cnt FROM table  ) x
    where cnt=1
    """
    df2=spark.sql(query2)
    df2.show()
   # df3=spark.sql(query1)
   # df3.show()





