from pyspark.sql import SparkSession,Row

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # How to access Row Elements
    # person = Row(Name = "Shreyas",age = 23)
    # print(person[0])
    # print(person.Name,person.age)

    # Row() used to represent a row in Data Frame
    # row1 = Row(Name = "Shreyas",age = 23)
    # row2 = Row(Name = "Kishor",age = 19)
    # data = [row1,row2]
    # df = spark.createDataFrame(data)
    # df.show(truncate=False)

    # Another way
    # info = Row("Name","age")
    # info1 = info("Shreyas",23)
    # info2 = info("Kishor",19)
    # data = [info1,info2]
    # df = spark.createDataFrame(data)
    # df.show()

    # How to insert nested structure
    # row1 = Row(Id = 1,Name = Row(First_Name="Shreyas",Last_Name="Bugad"))
    # row2 = Row(Id=2, Name=Row(First_Name="Kishor", Last_Name="Kharat"))
    # data = [row1,row2]
    # df = spark.createDataFrame(data)
    # df.show(truncate=False)
    # df.printSchema()
