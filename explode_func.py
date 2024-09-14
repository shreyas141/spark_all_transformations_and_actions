from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # How explode works for Arrytype Column
    #
    # data = [(1,["Shreyas","Bugad"]),(2,["Kishor","Kharat"])]
    # schema = StructType([StructField("id",IntegerType()),StructField("Name",ArrayType(StringType()))])
    #
    # df = spark.createDataFrame(data,schema)
    # df.printSchema()
    # df.show()
    #
    # df1 = df.withColumn("Exploded_Column",explode(col("Name")))
    # df1.show()

    # How explode works for map type Column
    data = [(1,{"Name":{"First_name":"Shreyas","Last_name":"Bugad"}}),(2,{"Name":{"First_name":"Kishor","Last_name":"Kharat"}})]
    schema = ["id","Name"]
    df = spark.createDataFrame(data,schema)
    df.printSchema()
    df.show(truncate=False)

    df1 = df.select(explode(df.Name))
    df1 = df1.select(df1.value.First_name.alias('first_name'))
    df1.show(truncate=False)


