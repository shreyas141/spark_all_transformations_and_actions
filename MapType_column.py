from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,MapType,IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [(1,"Shreyas",{"hair":"black","eye":"black"}),(2,"Shreya",{"hair":"brown","eye":"blue"})]

    # How to define schema for MapType Column
    schema = StructType([\
                         StructField("Id",IntegerType()),\
                         StructField("Name",StringType()),\
                         StructField("Properties",MapType(StringType(),StringType()))\
                         ])

    df = spark.createDataFrame(data,schema)
    # df.printSchema()
    # df.show(truncate=False)

    # How access the elements of MapType column
    df1 = df.withColumn("hair",df.Properties["hair"])
    df1.show(truncate=False)

    # Another Way
    df2 = df1.withColumn("eye",df1.Properties.getItem("eye"))
    df2.show()

