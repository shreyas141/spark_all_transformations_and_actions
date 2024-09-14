from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [("Shreyas","Male","Akluj"),("Pranav","Male","Akluj"),("Shreya","FeMale","Ppur"),("Suruchi","FeMale","Ppur"),("Dhanraj","Male","Ppur"),("Nikita","FeMale","Akluj")]

    schema = ["Name","Gender","City"]

    df = spark.createDataFrame(data,schema)

    # collect
    collectedList1 = df.collect()
    print(collectedList1)
    print(collectedList1[0])
    print(collectedList1[0][1])
    print(collectedList1[0].Name)