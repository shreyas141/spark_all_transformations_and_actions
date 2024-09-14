from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # Write(used to write into the disk)
    data = [(1,"Shreyas"),(2,"Vaibhav")]
    schema = ["Id","Name"]
    df = spark.createDataFrame(data = data,schema=schema)
    #
    # df.write.json(path=r"C:\Spark_practice_2_wafa\json_files_folder\written_files")
    #
    # df = spark.read.json(path=r"C:\Spark_practice_2_wafa\json_files_folder\written_files").show()

    # Saving Modes
    # 1.overwrite :- mode is used to overwrite existing file
    # df.write.json(path=r"C:\Spark_practice_2_wafa\json_files_folder\written_files",mode="overwrite")

    # 2.append :- mode is used to add the data on existing file
    # df.write.json(path=r"C:\Spark_practice_2_wafa\json_files_folder\written_files",mode="append")

    # 3.ignore :- ignores write operations when file already exists
    # df.write.json(path=r"C:\Spark_practice_2_wafa\json_files_folder\written_files", mode="ignore")

    # 4.error:- this is the default option when the file already exists, it returns an error
    # df.write.json(path=r"C:\Spark_practice_2_wafa\json_files_folder\written_files", mode="error")