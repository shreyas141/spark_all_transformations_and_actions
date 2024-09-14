from pyspark.sql import SparkSession
from pyspark.sql.functions import upper,col,when
from re import sub
if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [("Shreyas","Male","Akl#uj"),("Pranav","Male","A*kluj"),("Shreya","FeMale","Ppur"),("Suruchi","FeMale","Ppur"),("Dhanraj","Male","Pp$ur"),("Nikita","FeMale","Ak&luj")]

    schema = ["Name","Gender","City"]

    df = spark.createDataFrame(data,schema)


    def convertNameToUpper(df):
        return df.withColumn("Name",upper("Name"))

    def convertingMaleToM(df):
        return df.withColumn("Gender",when(col("Gender")=="Male","M").otherwise(col("Gender")))

    df.transform(convertNameToUpper).transform(convertingMaleToM).show()