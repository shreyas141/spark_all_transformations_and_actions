from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType,StructField,IntegerType,MapType,StringType
if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [(1,"Shreyas",'{"hair":"black","eye":"blue"}'),(2,"kishor",'{"hair":"black","eye":"black"}')]

    schema = ["id","name","properties"]
    df = spark.createDataFrame(data,schema)
    df.printSchema()
    df.show()

    # how to convert json string into MapType

    df1 = df.withColumn("Properties",from_json("Properties",MapType(StringType(),StringType())))
    df1.printSchema()
    df1.show()

    # how to convert json string into StructType
    structTypeSchema = StructType([StructField("hair",StringType()),StructField("eye",StringType())])
    df2 = df.withColumn("Properties",from_json("Properties",structTypeSchema))
    df2.printSchema()
    df2.show()