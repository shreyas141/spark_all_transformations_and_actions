from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    data = [(1,"Shreyas"),(2,"Suraj"),(3,"Aniket"),(4,"Kishor")]
    schema = ["Id","Name"]
    df = spark.createDataFrame(data=data,schema=schema)
    # df.show()

    # write
    # df.write.parquet(r"C:\Spark_practice_2_wafa\Parquet_files_folder\parquet_output")
    #
    # df1 = spark.read.parquet(r"C:\Spark_practice_2_wafa\Parquet_files_folder\parquet_output")
    # df1.show()

    # Saving Modes
    # 1.overwrite :- mode is used to overwrite existing file
    # df.write.parquet(path=r"C:\Spark_practice_2_wafa\Parquet_files_folder\parquet_output",mode="overwrite")

    # 2.append :- mode is used to add the data on existing file
    # df.write.parquet(path=r"C:\Spark_practice_2_wafa\Parquet_files_folder\parquet_output",mode="append")
    #
    # # 3.ignore :- ignores write operations when file already exists
    # df.write.parquet(path=r"C:\Spark_practice_2_wafa\Parquet_files_folder\parquet_output", mode="ignore")
    #
    # # 4.error:- this is the default option when the file already exists, it returns an error
    # df.write.parquet(path=r"C:\Spark_practice_2_wafa\Parquet_files_folder\parquet_output", mode="error")