# from pyspark.sql import SparkSession
#
# if __name__ == '__main__':
#     spark = SparkSession.builder.master("local[*]").getOrCreate()
#
#     data = [(1,"shreyas",50000),(2,"Kishor",40000)]
#     schema = ["id","name","salary"]
#     df = spark.createDataFrame(data = data,schema = schema)
#
#     df1 = df.withColumnRenamed("salary","Salary_amount")
#     df1.show()
