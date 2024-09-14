from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("write_to_csv").getOrCreate()

    data = [(1,"Shreyas"),(2,"Suraj")]
    schema = ["Id","Name"]

    df = spark.createDataFrame(data=data,schema=schema)
    # df.show()

    # write (used to write data frame into csv)
    # df.write.csv(path=r"C:\Spark_practice_2_wafa\csv_files_folder\emp_data",header=True)

    # df = spark.read.csv(path = r"C:\Spark_practice_2_wafa\csv_files_folder\emp_data",header=True)
    # df.show()

    # Saving Modes
    # 1.overwrite :- mode is used to overwrite existing file
    df.write.csv(path=r"C:\Spark_practice_2_wafa\csv2\data",header=True)
    #
    # df = spark.read.csv(path=r"C:\Spark_practice_2_wafa\csv_files_folder\emp_data",header=True)
    # df.show()

    # 2.append :- mode is used to add the data on existing file
    # df.write.csv(path=r"C:\Spark_practice_2_wafa\csv_files_folder\emp_data",header=True,mode="append")
    #
    # df = spark.read.csv(path = r"C:\Spark_practice_2_wafa\csv_files_folder\emp_data",header=True)
    # df.show()

    # 3.ignore :- ignores write operations when file already exists
    # df.write.csv(path=r"C:\Spark_practice_2_wafa\csv_files_folder\emp_data",header=True,mode="ignore")
    #
    # df = spark.read.csv(path=r"C:\Spark_practice_2_wafa\csv_files_folder\emp_data",header=True)
    # df.show()

    # 4.error:- this is the default option when the file already exists, it returns an error
    # df.write.csv(path = r"C:\Spark_practice_2_wafa\csv_files_folder\emp_data",header=True,mode="error")
    #
    # df = spark.read.csv(path=r"C:\Spark_practice_2_wafa\csv_files_folder\emp_data",header=True)
    # df.show()
