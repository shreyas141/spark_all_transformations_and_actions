from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    # df = spark.read.csv(path = r"C:\Spark_practice_2_wafa\csv_files_folder\emp1.csv",header=True,inferSchema=True)
    # df.show()

    # How To create dataframe from more than one csv file if both file located in diff locations

    # df = spark.read.csv([r"C:\Spark_practice_2_wafa\csv_files_folder\emp1.csv",r"C:\Spark_Practice\emp_info.csv"],header=True)
    # df.show()

    # if my 2 csv files are in same folder then i need to mention path only upto folder

    # df = spark.read.csv(r"C:\Spark_practice_2_wafa\csv_files_folder",header=True)
    # df.show()

    # Another Way
    # df = spark.read.format("csv").load(r"C:\Spark_practice_2_wafa\csv_files_folder\emp1.csv")
    # df.show()

    # if we want to make header = True or inferSchema = True
    # df = spark.read.format("csv").option(key= "header",value=True).option(key = "inferSchema",value=True).load(r"C:\Spark_practice_2_wafa\csv_files_folder\emp1.csv")
    # df.show()

    # if we want to define our own data type
    # schema = StructType().add(field="id",data_type=IntegerType(),nullable=False)\
    #                    .add(field="fname",data_type=StringType())\
    #                    .add(field="lname",data_type=StringType())\
    #                    .add(field="dept_id",data_type=StringType())\
    #                    .add(field="city",data_type=StringType())\
    #                    .add(field="state",data_type=StringType())\
    #                    .add(field="Salary",data_type=IntegerType(),nullable=False)
    #
    # df = spark.read.csv(r"C:\Spark_practice_2_wafa\csv_files_folder\emp1.csv",schema = schema)
    # df.show()




