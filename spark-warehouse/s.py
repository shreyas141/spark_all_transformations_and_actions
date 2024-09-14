from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()

df = spark.read.csv(path=r"C:\Users\SHREYAS\OneDrive\Desktop\Data Factory\ecdc-data\031 cases-deaths.csv", header=True, inferSchema=True)
# df.show()

df = df.filter(df.country == 'Europe')

df.show()
