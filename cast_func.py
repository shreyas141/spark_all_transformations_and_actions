from pyspark.sql import SparkSession
from pyspark.sql.functions import cast

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [(1,"Shreyas","M"),(2,"Shreya","F"),(3,"Akshay","")]
    schema = ["Id","Name","Gender"]
    df = spark.createDataFrame(data,schema)
    df.printSchema()

    # cast
    df1 = df.withColumn("Id",df.Id.cast("Int"))
    df1.printSchema()