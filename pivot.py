from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [("Shreyas","Male","Akluj"),("Pranav","Male","Akluj"),("Shreya","FeMale","Ppur"),("Suruchi","FeMale","Ppur"),("Dhanraj","Male","Ppur")]

    schema = ["Name","Gender","City"]

    df = spark.createDataFrame(data, schema)

    df.show()

    # pivot
    df.groupby("City").pivot("Gender").count().show()