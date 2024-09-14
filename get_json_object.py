from pyspark.sql import SparkSession
from pyspark.sql.functions import get_json_object
if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [("shreyas",'{"Addr":{"City":"Pune","State":"MH"},"Gender":"M"}'),("Kishor",'{"Addr":{"City":"Miraj","State":"MH"},"Gender":"M"}')]

    schema = ["Name","Props"]

    df = spark.createDataFrame(data,schema)

    df.show(truncate=False)

    # How to access specific obj from json string
    df1 = df.select("Name",get_json_object('Props',"$.Addr.State").alias("State"))
    df1.show()