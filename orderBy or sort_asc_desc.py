from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [(1,"Shreyas","M"),(2,"Shreya","F"),(3,"Akshay","")]
    schema = ["Id","Name","Gender"]
    df = spark.createDataFrame(data,schema)

    # sort,asc,desc
    # asc
    # df.sort(df.Name.asc()).show()

    # desc
    # df.sort(df.Name.desc()).show()

    # orderBy()
    # df.orderBy(df.Name.asc(),df.Id.asc()).show()