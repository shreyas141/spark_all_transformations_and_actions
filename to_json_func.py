from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json
from pyspark.sql.types import MapType,StructType,StructField,StringType,IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [(1,"shreyas",{"hair":"brown","eye":"blue"}),(2,"kishor",{"hair":"black","eye":"brown"})]

    schema = ["id","name","properties"]

    mapDf = spark.createDataFrame(data,schema)
    mapDf.show()

    # How to convert MapType column into json String
    df1 = mapDf.withColumn("properties",to_json(mapDf.properties))
    df1.printSchema()
    df1.show()

    # How to convert StructType column into json String
    structData = [(1,"shreyas",("brown","blue")),(2,"kishor",("black","brown"))]

    structTypeSchema = StructType([StructField("hair",StringType()),StructField("eye",StringType())])

    structSchema = StructType([StructField("id",IntegerType()),StructField("name",StringType()),StructField("properties",structTypeSchema)])

    structDf = spark.createDataFrame(structData,structSchema)

    structDf.printSchema()
    structDf.show()

    df2 = structDf.withColumn("properties",to_json(structDf.properties))

    df2.printSchema()
    df2.show()

