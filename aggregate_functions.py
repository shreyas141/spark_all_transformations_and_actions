from pyspark.sql import SparkSession
from pyspark.sql.functions import count,max,min,avg,sum,approx_count_distinct,collect_list,collect_set,countDistinct

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [(1,"Shreyas","M","It",2000),(2,"kishor","M","Hr",4000),(3,"vaibhav","M","It",1000),(4,"shreya","F","It",5000),(5,"Akshay","M","Accounts",500),(6,"Tejas","M","Accounts",300),(7,"Pratiksha","F","Hr",4000)]

    schema = ["id","name","gender","dept","salary"]

    df = spark.createDataFrame(data,schema)
    df.show()

    # How to use aggregate function with group by
    # df1 = df.groupby("dept").min("salary")
    # df1.show()

    # How to use multiple aggregate function in a single data frame using agg

    # df2 = df.groupby("dept").agg(count("*").alias("No. of emp"),min("salary").alias("minSalary"),max("salary").alias("maxSalary"),sum("salary").alias("sumSalary"),avg("salary").alias("avgSalary"))
    # df2.show(truncate=4)

    # approx_count_distinct()
    df.select(approx_count_distinct("dept")).show()

    # collect_list()
    df.select(collect_list("dept")).show(truncate=False)

    # collect_set()
    df.select(collect_set("dept")).show(truncate=False)

    # countDistinct()
    df.select(countDistinct("dept")).show(truncate=False)




