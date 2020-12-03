from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import struct
from pyspark.sql.window import Window
from datetest import datetime
from pyspark.sql.functions import mean, sum, max, col
if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("PythonWordCount").getOrCreate()
    df = spark.parallelize([(1, 3.0), (1, 3.0), (2, -5.0)]).toDF(["k", "v"])
    groupBy = ["k"]
    aggregate = ["v"]
    funs = [mean, sum, max]
    exprs = [f(col(c)) for f in funs for c in aggregate]
    # or equivalent df.groupby(groupBy).agg(*exprs)
    df.groupby(*groupBy).agg(*exprs)

    


