from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    lst = [1,2,3]

    # How to perform transformation on rdd
    rdd = spark.sparkContext.parallelize(lst)

    # How to perform action on rdd
    print(rdd.collect())