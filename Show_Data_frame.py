from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    df = spark.read.csv(path=r"C:\Spark_practice_2_wafa\csv_files_folder\emp_data",header=True,inferSchema=True)

    # show()
    # it is used to show the data in table rows and column format
    # df.show()

    # we can see complete data if we set truncate = False
    # df.show(truncate=False)

    # we can see data upto specified length
    # df.show(truncate=6)

    # we can retrive specific number of rows also
    # df.show(n=2,truncate=False)

    # we can see content vertically too
    # df.show(truncate=False,vertical=True)