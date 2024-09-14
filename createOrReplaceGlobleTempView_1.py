from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [("Shreyas","Male",["Akl#uj","Pune"]),("Pranav","Male",["A*kluj","Mumbai"]),("Shreya","FeMale",["Ppur","Solap*&ur"])]

    schema = ["Name", "Gender", "City"]

    df = spark.createDataFrame(data, schema)

    # How to create GlobalTempView
    df1 = df.createOrReplaceGlobalTempView("Global_Student")
    df2 = spark.sql("SELECT * FROM global_temp.Global_Student")
    df2.show()

    # How to drop GlobalTempView
    # spark.catalog.dropGlobalTempView("Global_Student")

    # How to see GlobalTempViews in the cluster
    # print(spark.catalog.listTables("global_temp"))