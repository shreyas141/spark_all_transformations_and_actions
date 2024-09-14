from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [(1, "Shreyas", "M"), (2, "Shreya", "F"), (3, "Akshay", "")]
    schema = ["Id", "Name", "Gender"]
    df = spark.createDataFrame(data, schema)
    df.show()

    # filter() (To filter the values on specific conditions)
    df.filter(df.Gender == "M").show()
    # or
    df.filter("Gender == 'M'").show()

    df.filter(("Gender == 'M'") and ("Name == 'Shreyas'")).show()

    # similarly we can use where() instade of filter both works exact same