
from pyspark.sql import functions as F,SparkSession


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

    publish_data_before_filter.write.format("avro").save("C:\\Users\\Nilay\\Desktop\\avro")
    #publish_data_before_filter.save()

