from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [("Shreyas","Male",["Akl#uj","Pune"]),("Pranav","Male",["A*kluj","Mumbai"]),("Shreya","FeMale",["Ppur","Solap*&ur"])]

    schema = ["Name", "Gender", "City"]

    df = spark.createDataFrame(data, schema)

    # createOrReplaceTempView()

    df1 = df.createOrReplaceTempView("Student")
    df2 = spark.sql("SELECT * FROM Student")
    df2.show()

    # How to drop TempView
    # spark.catalog.dropTempView("Student")

    # How to current session TempViews
    # print(spark.catalog.listTables(spark.catalog.currentDatabase()))