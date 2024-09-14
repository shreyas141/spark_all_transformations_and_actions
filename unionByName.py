from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data1 = [(1,"Shreyas","M"),(2,"Shreya","F"),(3,"Akshay","")]
    schema1 = ["Id","Name","Gender"]

    data2 = [(4, "Shreyas", "M"), (5, "Shreya", "F"), (6, "Akshay", ""),(1,"Shreyas","M")]
    schema2 = ["Id", "Name", "sex"]

    df1 = spark.createDataFrame(data1,schema1)
    df2 = spark.createDataFrame(data2, schema2)

    df1.show()
    df2.show()

    # union by name
    # union by name always merge the columns by their column names
    # in my data frame one column contain gender and other contain sex but both of the column having same schema
    # so, it will throw an error
    # to overcome on that error we have function called allowMissingColumn
    # if we assign Value as True to it, it will all the missing column names

    # using allowMissingColumn

    df1.unionByName(df2, allowMissingColumns=True).show()

    # below query will show an error because both having different column names
    df1.unionByName(df2).show()