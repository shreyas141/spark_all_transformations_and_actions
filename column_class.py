from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import col,lit
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # To use column class object simplest way to use lit
    col1 = lit("abcd")
    print(type(col1))

    # How to assign a value to column
    row1 = Row(Name = "Shreyas",age = 23)
    row2 = Row(Name = "Kishor",age = 19)
    data = [row1,row2]
    df = spark.createDataFrame(data)
    df.show()

    # let's add a new column using withColumn
    df1 = df.withColumn("State",lit("Maharastra"))
    df1.show()

    # How to select specific column
    df1.select(df1.Name,df1.age).show()
    # 2nd way
    df1.select("Name", "age").show()
    # 3rd way
    df1.select(col("Name"),col("age")).show()


    # data = [(1,"Shreyas",("black","black")),(2,"Shreya",("black","brown"))]
    #
    # StructTyp = StructType([StructField("Hair",StringType()),StructField("Eyes",StringType())])
    # schema = StructType([\
    #          StructField("Id",IntegerType()),\
    #          StructField("Name",StringType()),\
    #          StructField("Properties",StructTyp)])
    #
    # df = spark.createDataFrame(data,schema)
    #
    # df.show()
    # df.printSchema()
    #
    # # How to select specific element from struct column
    # df.select(df.Properties.Hair).show()
    # # 2nd way
    # df.select(col("Properties.Hair")).show()
    # # 3rd way
    # df.select("Properties.Hair").show()
    # # 4t way
    # df.select(df["Properties.Hair"]).show()


