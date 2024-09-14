from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data1 = [(1,"Shreyas","M"),(2,"Shreya","F"),(3,"Akshay","")]
    schema1 = ["Id","Name","Gender"]

    data2 = [(4, "Shreyas", "M"), (5, "Shreya", "F"), (6, "Akshay", ""),(1,"Shreyas","M")]
    schema2 = ["Id", "Name", "Gender"]

    df1 = spark.createDataFrame(data1,schema1)
    df2 = spark.createDataFrame(data2, schema2)

    df1.show()
    df2.show()

    # union and unionAll (Both these transformation works exactly same)

    unionDf = df1.union(df2)
    unionDf.show()

    # unionAll
    unionAllDf = df1.unionAll(df2)
    unionAllDf.show()

    # to remove duplicates
    unionDf.distinct().show()