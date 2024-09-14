from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    data = [(1,"Shreyas","male","It"),(2,"Rupesh","male","It"),(3,"Vaibhav","male","Hr"),(4,"Shital","female","Hr"),(5,"Shreya","female","It")]

    schema = ["Id","Name","Gender","Dept"]

    df = spark.createDataFrame(data,schema)

    # partitionBy

    df.write.parquet(path=r"C:\Spark_practice_2_wafa\emp_parquet",mode="overwrite",partitionBy="Dept")

    df1 = spark.read.parquet(r"C:\Spark_practice_2_wafa\emp_parquet\Dept=Hr")

    df2 = spark.read.parquet(r"C:\Spark_practice_2_wafa\emp_parquet\Dept=It")

    df3 = spark.read.parquet(r"C:\Spark_practice_2_wafa\emp_parquet")

    df1.show()
    df2.show()
    df3.show()


    # we can specify multiple column name also
    # df.write.parquet(path=r"C:\Spark_practice_2_wafa\emp_parquet", mode="overwrite", partitionBy=["Dept","Gender"])