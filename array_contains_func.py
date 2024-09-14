from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains,col

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [(1,["python","sql"]),(2,["java","RWD"])]
    schema = ["id","Skills"]

    df = spark.createDataFrame(data,schema)

    # array_contails(col,value)
    # used to check array column contains value True if it is there, Null if array is Null

    df1 = df.withColumn("hasPythonSkills",array_contains(col("Skills"),"python"))
    df1.show()