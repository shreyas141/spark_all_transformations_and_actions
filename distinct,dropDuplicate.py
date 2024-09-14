from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [(1,"shreyas",50000),(2,"Kishor",40000),(1,"shreyas",50000)]
    schema = ["id","name","salary"]
    df = spark.createDataFrame(data = data,schema = schema)
    df.show()

    # distinct()
    df.distinct().show()

    # dropDuplicates()
    df.dropDuplicates(["name","id"]).show()
