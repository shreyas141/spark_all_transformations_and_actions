from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [(1,"python,sql"),(2,"java,RWD")]
    schema = ["id","Skills"]

    df = spark.createDataFrame(data,schema)

    # split() (returns an array type after spliting the column by delimiter)

    df1 = df.withColumn("Skills_Array",split(col("Skills"),","))
    df1.show()