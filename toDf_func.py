from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    lst = [(1,"Shreyas"),(2,"Tejas")]
    rdd = spark.sparkContext.parallelize(lst)

    # toDF used to create DataFrame from rdd
    df = rdd.toDF(["ID","Name"])
    df.show()