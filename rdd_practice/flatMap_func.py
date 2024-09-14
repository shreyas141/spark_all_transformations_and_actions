from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [("SHREYAS BUGAD","SHREYA GODALE"),("KISHOR KHARAT","TEJAS SANKPAL")]
    rdd = spark.sparkContext.parallelize(data)
    rdd2 = rdd.flatMap(lambda x:x)
    print(rdd2.collect())
    # output : ['SHREYAS BUGAD', 'SHREYA GODALE', 'KISHOR KHARAT', 'TEJAS SANKPAL']

    rdd3 = rdd2.map(lambda x:x.split(" "))
    print(rdd3.collect())
    # output [['SHREYAS', 'BUGAD'], ['SHREYA', 'GODALE'], ['KISHOR', 'KHARAT'], ['TEJAS', 'SANKPAL']]

    rdd4 = rdd3.flatMap(lambda x:x)
    print(rdd4.collect())
