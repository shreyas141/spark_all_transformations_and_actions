from pyspark.sql import SparkSession
from pyspark.sql.functions import array,col

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [(1,"python","sql"),(2,"java","RWD")]
    schema = ["id","Primary_Skills","Secondary_Skills"]

    df = spark.createDataFrame(data,schema)

    # array() (used to create a column after merging data from diff columns)

    df1 = df.withColumn("Skills_array",array(col("Primary_Skills"),col("Secondary_Skills")))
    df1.show()
    input("please press any key to terminate")