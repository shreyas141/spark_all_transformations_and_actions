from pyspark.sql import SparkSession
from pyspark.sql.functions import array
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType
if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    # How to define ArryType schema and How to give data type to arry elements
    data = [("abc",[1,2]),("pqr",[4,5]),("mno",[7,8])]
    schema = StructType([StructField("Id",StringType()),StructField("Numbers",ArrayType(IntegerType()))])

    df = spark.createDataFrame(data,schema)
    df.show()
    df.printSchema()



    # How to make colums which holds arry type values
    # data = [(1,2),(3,4)]
    # schema = ["num1","num2"]
    # df = spark.createDataFrame(data,schema)
    # df1 = df.withColumn("numbers",array(df.num1,df.num2))
    # df1.show()