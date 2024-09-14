from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    df = spark.range(1,101)
    df.show(100)

    # sample(fraction,seed)
    df1 = df.sample(fraction=0.1)
    df2 = df.sample(fraction=0.1)
    df1.show()
    df2.show()

    # if we set a fraction value it won't retrive exact percentage value but it may be close to it or more than that
    # if we won't set seed value it both data frame retrive diff no. of value with diff count

    df3 = df.sample(fraction=0.1,seed=123)
    df4 = df.sample(fraction=0.1,seed=123)
    df3.show()
    df4.show()