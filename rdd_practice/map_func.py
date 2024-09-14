from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [(1,("Shreyas","Bugad")),(2,("Vaibhav","kadam")),(3,("Rupesh","Mohite")),(4,("kshitij","Thakare"))]
    rdd = spark.sparkContext.parallelize(data)
    rdd2 = rdd.map(lambda x:(x[0],x[1]+(x[1][0]+' '+x[1][1],)))
    print(rdd2.collect())

    # we can define function and use it into map also
    def fullName(x):
        return (x[0],x[1]+(x[1][0]+' '+x[1][1],))

    rdd3 = rdd.map(lambda x: fullName(x))
    print(rdd3.collect())

    # how to use map function with dataFrame
    data1 = [("Shreyas","Bugad"),("Vaibhav","Kadam")]
    schema = ["First_Name","Last_Name"]
    df = spark.createDataFrame(data1,schema)

    # df.rdd is used to generate radd from dataFrame
    df2 = df.rdd.map(lambda x:x+(x[0]+' '+x[1],))
    df2 = df2.toDF(["First_Name","Last_Name","Full_Name"])
    df2.show()

