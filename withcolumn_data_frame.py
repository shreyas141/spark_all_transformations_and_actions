from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [(1,"shreyas",50000),(2,"Kishor",40000)]
    schema = ["id","name","salary"]
    df = spark.createDataFrame(data = data,schema = schema)
    # df.show()
    # df.printSchema()

    # How to change data type of existing column using withColumn
    # df1 = df.withColumn(colName="salary",col=col("salary").cast("Integer"))
    # df1.printSchema()

    # How to apply for multiple columns
    df2 = df.withColumn("salary",col("salary").cast("Integer")).withColumn("id",col("id").cast("Integer"))
    # df2.printSchema()

    # How to modify tha data of existing column
    df3 = df2.withColumn("salary",col("salary")*2)
    # df3.show()

    # How to create a new column on existing data frame
    df4 = df3.withColumn("Country",lit("India"))
    # df4.show()

    # How to create new column with the help of existing column
    df5 = df4.withColumn("Salary_Hike",col("salary")*2)
    df5.show()
