from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import struct
from pyspark.sql.window import Window
if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("PythonWordCount").getOrCreate()
    data=spark.createDataFrame(
        [
            ("Day",1,"nil"),  # create your data here, be consistent in the types.
            ("Night",1,"ram"),
            ("Day", 2, "nil1"),
            ("Night", 3, "ram"),
            ("Day", 2, "nil1"),
        ],
        ['movie','rating','name']  # add your columns label here
    )

    s=data.groupBy("movie").agg(F.countDistinct('name').alias('name'), F.avg('rating').alias('average_rat'))
    s.show()