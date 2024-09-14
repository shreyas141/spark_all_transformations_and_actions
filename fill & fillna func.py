from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [(1,"male","it"),(2,"female",None),(3,None,"Hr")]
    schema = ["id","gender","dept"]
    df = spark.createDataFrame(data,schema)
    df.show()

    # fill(value,[column_name])
    df.na.fill("unknown",["gender"]).show()

    # fillna(value,[column_name])
    df.fillna("unknown").show()
    
