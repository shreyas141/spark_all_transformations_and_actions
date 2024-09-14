from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, date_format,to_date,datediff,months_between,add_months,date_add,year,month,dayofmonth,dayofweek,dayofyear
if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    df = spark.range(1,3)
    df.show()

    # dateType default format is yyyy-MM-dd

    # How to use current_date()
    df1 = df.withColumn("currentDate",current_date())
    df1.show()
    df1.printSchema()

    # How to format a date (but the data type will convert into string )
    df1 = df1.withColumn("currentDate",date_format("currentDate","dd|MM|yyyy"))
    df1.show()
    df1.printSchema()

    # How to convert date into dateType
    df1 = df1.withColumn("currentDate",to_date(df1.currentDate,"dd|MM|yyyy"))
    df1.show()
    df1.printSchema()

    # ------------------------------------------------------- #

    data = [("2023-01-09","2023-01-30"),("2021-05-19","2023-06-19")]
    schema = ["date1","date2"]

    newDf = spark.createDataFrame(data,schema)

    newDf.show()

    # datediff()
    df1 = newDf.withColumn("DateDiff",datediff(newDf.date2,newDf.date1))

    df1.show()

    # months_between()
    df2 = df1.withColumn("MonthsDiff",months_between(df1.date2,df1.date1))
    df2.show()

    # add_months() (if we pass -ve value it will subtract months)
    df3 = df2.withColumn("monthAdded",add_months(df2.date1,5))
    df3.show()

    # date_add() (if we pass -ve value it will subtract days)
    df4 = df3.withColumn("daysAdded",date_add(df3.date1,5))
    df4.show()

    # year()
    df5 = df4.withColumn("yearOfDate1",year(df4.date1))
    df5.show()

    # month()
    df6 = df5.withColumn("monthOfDate1",month(df5.date1))
    df6.show()

    # dayofmonth() (return day of the date)
    df7 = df6.withColumn("dayOfMonth",dayofmonth(df6.date1))
    df7.show()

    # dayofweek() (1 for sunday till 7 for saturday)
    df8 = df7.withColumn("dayOfWeek",dayofweek(df7.date1))
    df8.show()

    # dayofyear()
    df9 = df8.withColumn("dayOfYear",dayofyear(df8.date1))
    df9.show()


