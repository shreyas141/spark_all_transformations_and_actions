from pyspark.sql import SparkSession
from pyspark.sql.functions import col,map_keys

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [(1, {"First_name": "Shreyas", "Last_name": "Bugad"}),
            (2, {"First_name": "Kishor", "Last_name": "Kharat"})]
    schema = ["id","info"]

    df = spark.createDataFrame(data,schema)
    df.show(truncate=False)

    # map_keys() (Extract only keys from MapType column)
    df1 = df.withColumn("Keys",map_keys(df.info))
    df1.show(truncate=False)