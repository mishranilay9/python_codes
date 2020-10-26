import sys
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from pyspark.sql import SparkSession
if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("PythonWordCount").getOrCreate()
    df=spark.createDataFrame(
        [
            ("Thin","Cellphone",6000),  # create your data here, be consistent in the types.
            ("Normal","Tablet",1500),
            ("Mini","Tablet",5500),
            ("Ultra-Thin", "Cellphone", 5000),  # create your data here, be consistent in the types.
            ("Very-Thin", "Cellphone", 6000),
            ("Big", "Tablet", 1500),
            ("Bendable", "Cellphone", 3000),
            ("Foldable", "Cellphone", 3000),  # create your data here, be consistent in the types.
            ("Pro", "Tablet", 4500),
            ("Pro2", "Tablet", 6500)
        ],
        ['product','category','revenue']  # add your columns label here
    )
    windowSpec = Window \
                .partitionBy(df['category']) \
                .orderBy(df['revenue'].desc())
                #.rangeBetween(-sys.maxsize, sys.maxsize)
    dataFrame=df.withColumn("revenue_difference",func.max(df['revenue']).over(windowSpec) - df['revenue'])
    data1 =  dataFrame.select(
            dataFrame['product'],
            dataFrame['category'],
            dataFrame['revenue'],
            dataFrame['revenue_difference'])
    data1.show()