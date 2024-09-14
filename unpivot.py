from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [("It",8,5),("Hr",4,3),("Admin",2,1)]
    schema = ["dept","male","female"]

    df = spark.createDataFrame(data,schema)
    df.cache()
    df.show()

    # expr("stack(no. of column,"value",column_name) as (generated column name)")

    unpivotDf = df.select(df.dept,expr("stack(2,'Male',male,'Female',female) as (gender,count)"))

    unpivotDf.show()
