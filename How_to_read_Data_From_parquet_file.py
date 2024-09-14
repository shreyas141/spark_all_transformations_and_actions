from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # read single parquet file
    # df = spark.read.parquet(r"C:\Spark_practice_2_wafa\Parquet_files_folder\userdata1.parquet")
    # df.show(1000,truncate=False)
    # print(f"Total Number of Rows: {df.count()}")

    # how to read all parquet file only
    # df = spark.read.parquet(r"C:\Spark_practice_2_wafa\Parquet_files_folder\*.parquet")
    # df.show(2000,truncate=False)
    # print(f"Total Number of Rows: {df.count()}")

    # how to read parquet files from folder which contain only parquet files
    # df = spark.read.parquet(r"C:\Spark_practice_2_wafa\Parquet_files_folder")
    # df.show(2000,truncate=False)
    # print(f"Total Number of Rows: {df.count()}")
