from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp,to_timestamp,lit,hour,minute,second
if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    df = spark.range(3)
    df.show()

    # current_timestamp()
    df1 = df.withColumn("currentTimeStamp",current_timestamp())
    df1.show(truncate=False)
    df1.printSchema()

    ###########################################
    df2 = df1.withColumn("timeStampInString",lit("16-12-2023 11:25:50"))
    df2.printSchema()
    df2.show()
    ###########################################

    # to_timestamp()
    df3 = df2.withColumn("timeStampInString",to_timestamp(df2.timeStampInString,"dd-MM-yyyy HH:mm:ss"))

    df3.printSchema()
    df3.show()

    # hour,minute,second
    df3.select('*',hour(df3.currentTimeStamp).alias("Hour"),minute(df3.currentTimeStamp).alias("Minute"),second(df3.currentTimeStamp).alias("Second")).show(truncate=False)
