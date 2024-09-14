from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    data1 = [(1,"Shreyas","2"),(2,"Kishor","1"),(3,"Suraj","4")]
    schema1 = ["id","name","dept_id"]

    empDf = spark.createDataFrame(data1,schema1)
    empDf.show()

    data2 = [(1,"HR"),(2,"IT"),(3,"Accounts")]
    schema2 = ["dept_id","dept_name"]

    deptDf = spark.createDataFrame(data2,schema2)
    deptDf.show()

    empDf.cache()
    deptDf.cache()

    # inner join
    empDf.join(deptDf,empDf.dept_id == deptDf.dept_id,"inner").show()

    # left join
    empDf.join(deptDf,empDf.dept_id == deptDf.dept_id,"left").show()

    # right join
    empDf.join(deptDf, empDf.dept_id == deptDf.dept_id, "right").show()

    # full outer join
    empDf.join(deptDf, empDf.dept_id == deptDf.dept_id, "full").select(empDf.id,empDf.name,empDf.dept_id,deptDf.dept_name).show()

    # leftsemi join (it will retrive matching column from left table only)
    empDf.join(deptDf, empDf.dept_id == deptDf.dept_id, "leftsemi").show()

    # leftanti join (it will retrive non matching column from left table only)
    empDf.join(deptDf, empDf.dept_id == deptDf.dept_id, "leftanti").show()

    # self join
    data3 = [(1,"shreyas",0),(2,"kishor",1),(3,"suraj",2),(4,"akshay",3)]
    schema3 = ["id","name","manager_id"]

    empDf2 = spark.createDataFrame(data3,schema3)

    empDf2.alias("empInfo").join(empDf2.alias("mgrInfo"),col("empInfo.manager_id")==col("mgrInfo.id"),"left").select(col("empInfo.name").alias("Employee"),col("mgrInfo.name").alias("Manager")).show()

