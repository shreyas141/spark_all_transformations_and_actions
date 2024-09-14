from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank,dense_rank,row_number,lead,lag,desc,asc,col

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data = [(1,"Shreyas","It",75000),(2,"Kishor","Hr",45000),(3,"Shreya","It",80000),(4,"Akshay","Hr",50000)]

    schema = ["Id","Name","Department","Salary"]

    df = spark.createDataFrame(data,schema)
    df.show()

    # row_number()
    # windowSpecification = Window.partitionBy("Department").orderBy(col("Salary").desc())
    #
    # rowNumberDf = df.withColumn("Row_Number",row_number().over(windowSpecification))
    # rowNumberDf.show()

    # rank()
    # windowSpecification = Window.partitionBy("Department").orderBy(col("Salary").desc())
    #
    # rankDf = df.withColumn("Rank",rank().over(windowSpecification))
    # rankDf.show()

    # dense_rank()
    # windowSpecification = Window.partitionBy("Department").orderBy(col("Salary").desc())
    #
    # denseRankDf = df.withColumn("Dense_Rank",dense_rank().over(windowSpecification))
    # denseRankDf.show()

    # lead
    windowSpecification = Window.partitionBy("Department").orderBy(col("Salary"))

    # leadDf = df.withColumn("Lead",lead("Salary").over(windowSpecification))
    # leadDf.show()

    # lag
    lagDf = df.withColumn("Lag",lag("Salary").over(windowSpecification))
    lagDf.show()
