from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [(1, "Shreyas", "M"), (2, "Shreya", "F"), (3, "Akshay", "")]
    schema = ["Id", "Name", "Gender"]
    df = spark.createDataFrame(data, schema)

    # we can select columns in multiple ways
    # 1st
    df.select(df.Id,df.Name).show()
    # 2nd
    df.select("Id", "Name").show()
    # 3rd
    df.select(df["Id"], df["Name"]).show()
    # 4th
    df.select(["Id", "Name"]).show()
    # 5th
    df.select(col("Id"), col("Name")).show()
    # 6th
    df.select([i for i in df.columns]).show()