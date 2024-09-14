from pyspark.sql import SparkSession
from pyspark.sql.functions import when

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [(1,"Shreyas","M"),(2,"Shreya","F"),(3,"Akshay","")]
    schema = ["Id","Name","Gender"]
    df = spark.createDataFrame(data,schema)

    # when() & otherwise() (Using With withColumn() Statement)
    # df1 = df.withColumn("Gender_Full_Form",when(df.Gender=="M","Male").when(df.Gender=="F","Female").otherwise("Unknown"))
    # df1.show()

    # when() & otherwise() (Using With select() Statement)
    df1 = df.select(df["Id"],df["Name"],
                        when(df.Gender == "M", "Male").when(df.Gender == "F", "Female").otherwise("Unknown").alias("Gender_Full_Form"))
    df1.show()