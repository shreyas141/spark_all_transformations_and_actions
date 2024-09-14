from pyspark.sql import SparkSession
from pyspark.sql.functions import upper,col,when,transform,udf
import re
if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [("Shreyas","Male",["Akl#uj","Pune"]),("Pranav","Male",["A*kluj","Mumbai"]),("Shreya","FeMale",["Ppur","Solap*&ur"])]

    schema = ["Name", "Gender", "City"]

    df = spark.createDataFrame(data,schema)

    def convertArryToUpper(x):
        return upper(x)


    # transform
    df.select("City",transform("City",convertArryToUpper).alias("City_Upper")).show()

    # or
    df.select("*",transform("City",lambda x:upper(x))).show()