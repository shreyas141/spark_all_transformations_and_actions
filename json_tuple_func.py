from pyspark.sql import SparkSession
from pyspark.sql.functions import json_tuple
if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [(1,"Shreyas",'{"hair":"black","eye":"blue"}'),(2,"kishor",'{"hair":"black","eye":"black"}')]

    schema = ["id","name","properties"]
    df = spark.createDataFrame(data,schema)
    df.printSchema()
    df.show()

    # How to access specific element from json string
    df1 = df.select("id","name",json_tuple(df.properties,"hair","eye").alias("hair","eye"))
    df1.show()