from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # data = [(1,"shreyas",50000),(2,"Kishor",40000)]
    # schema = StructType([
    #     StructField(name="id",IntegerType()),
    #     StructField(name="Name",StringType()),
    #     StructField(name="Salary",StringType())
    # ])
    # df = spark.createDataFrame(data = data,schema = schema)
    # df.show()
    # df.printSchema()

    # Defining Schema For complex data Structure
    # data = [(1, ("shreyas","Bugad"), 50000), (2, ("Kishor","Kharat"), 40000)]
    #
    # stuctvalue = StructType([StructField(name="First_Name",dataType=StringType()),StructField(name="Last_Name",dataType=StringType())])
    #
    # schema = StructType([StructField(name="id", dataType=IntegerType()),StructField(name="Name", dataType=stuctvalue),StructField(name="Salary", dataType=StringType())])
    #
    # df = spark.createDataFrame(data = data,schema=schema)
    # df.show()
    # df.printSchema()