# from pyspark.sql import SparkSession
# from pyspark.sql.functions import udf,col
# from pyspark.sql.types import StringType,IntegerType
# from pyspark.sql.functions import *
# import re
# from pyspark.sql.window import Window
# spark = SparkSession.builder.master("local[*]").getOrCreate()

# if __name__ == '__main__':spark = SparkSession.builder.master("local[*]").getOrCreate()
#
#     data = [("Shrey@as","5000","1000"),("Kish$or","3000","2000")]
#     schema = ["Name","Salary","Bonus"]
#
#     df = spark.createDataFrame(data,schema)
#
#     # udf
#     # 1st way
#     def removeSpecialChar(x):
#         return re.sub("[^a-zA-Z0-9]","",x)
#
#     def totalPay(s,b):
#         return s+b
#
#     reg_repl = udf(lambda x:removeSpecialChar(x),StringType())
#     TotalPay = udf(lambda x,y:totalPay(x,y),IntegerType())
#
#     df1 = df.withColumn("Name",reg_repl(col("Name"))).withColumn("TotalPay",TotalPay(col("Salary"),col("Bonus")))
#
#     df1.show()
#
#     # 2nd way
#     @udf(returnType=StringType())
#     def removeSpecialChar(x):
#         return re.sub("[^a-zA-Z0-9]","",x)
#
#     @udf(returnType=IntegerType())
#     def totalPay(s,b):
#         return s+b
#
#     df.select(removeSpecialChar("Name"),totalPay("Salary","Bonus")).show()
#
#     # How to register our own udf
#     def removeSpecialChar(x):
#         return re.sub("[^a-zA-Z0-9]","",x)
#     spark.udf.register(name="removeSpecialChar", f=removeSpecialChar, returnType=StringType())
#
#     # How to use our function in sql query
#     view = df.createOrReplaceTempView("emp")
#     df1 = spark.sql("SELECT removeSpecialChar(Name) FROM emp")
#     df1.show()

#
# df = spark.read.csv('C:\Users\SHREYAS\OneDrive\Desktop\gen2_data\raw\ecdc\cases_deaths\cases_deaths.csv')
#
# df = df.filter(df.country == 'Europe')
#
# df.show()
