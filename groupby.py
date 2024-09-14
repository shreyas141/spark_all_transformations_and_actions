from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [(1,"Shreyas","M","It"),(2,"kishor","M","Hr"),(3,"vaibhav","M","It"),(4,"shreya","F","It"),(5,"Akshay","M","Accounts"),(6,"Tejas","M","Accounts"),(7,"Pratiksha","F","Hr")]

    schema = ["id","name","gender","dept"]

    df = spark.createDataFrame(data,schema)
    df.show()

    # groupby()
    df.groupby(df.dept,df.gender).count().show()
