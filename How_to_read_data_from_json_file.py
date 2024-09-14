from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # How to read data from single file
    # df = spark.read.json(path=r"C:\Spark_practice_2_wafa\json_files_folder\emp.json")
    # df.printSchema()
    # df.show()

    # How to read data from multiple files when both the file are at seprate location
    # df = spark.read.json(path=[r"C:\Spark_Practice\input.JSON",r"C:\Spark_practice_2_wafa\json_files_folder\input2.json"])
    #
    # df.printSchema()
    # df.show()

    # How to read data from multiple json files when those are at same directory
    # df = spark.read.json(path=r"C:\Spark_practice_2_wafa\json_files_folder\student\*.json")
    # df.printSchema()
    # df.show()


    # How to read data from Multi Line Json File
    df = spark.read.json(path=r'C:\Spark_practice_2_wafa\json_files_folder\empML.json',multiLine=True)
    df.printSchema()
    df.show()


