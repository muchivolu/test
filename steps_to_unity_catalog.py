# Databricks notebook source
# MAGIC %sql
# MAGIC show catalogs

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists tiru_catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog tiru_catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists tiru_catalog.tiru_schema

# COMMAND ----------

# MAGIC %sql
# MAGIC describe schema extended  tiru_schema

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists quickstart_table(columna int, columnb stringtiru_catalog.default.quickstart_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into table quickstart_table values(1,"tiru")

# COMMAND ----------

# MAGIC  %sql
# MAGIC create table if not exists tiru_catalog.tiru_schema.menu( 
# MAGIC recipe_id INT,
# MAGIC app string,
# MAGIC main string,
# MAGIC dessert string
# MAGIC );
# MAGIC
# MAGIC
# MAGIC INSERT into tiru_catalog.tiru_schema.menu (recipe_id,app,main,dessert) values (1,"ceivere","Flan","Tacos"),  (2,"Tomota soup","souff","cream blurree");
# MAGIC
# MAGIC create table  tiru_catalog.tiru_schema.dinner AS Select recipe_id,concat(app,"+",dessert) as full_menu from  tiru_catalog.tiru_schema.menu 

# COMMAND ----------

from pyspark.sql.functions import *

df = spark.range(3).withColumn("price",round(10*rand(seed=42),2)).withColumnRenamed("id","recipe_id")

df.write.mode("overwrite").saveAsTable("tiru_catalog.tiru_schema.price")

dinner = spark.read.table("tiru_catalog.tiru_schema.dinner") 
price = spark.read.table("tiru_catalog.tiru_schema.price")  

dinner_price = dinner.join(price, on="recipe_id")
dinner_price.write.mode("overwrite").saveAsTable("tiru_catalog.tiru_schema.dinner_price")

# COMMAND ----------

list1 = [1,2,3,4,5,6,7]
list2 = [1,6,2,3,4,5,7]

def equal_check(list1,list2):
    return sorted(list1) == sorted(list2)

print(equal_check(list1,list2))





# COMMAND ----------

# remove duplicates and reverse the list

list1 = [1,4,5,3,2,3,4,5,9,0,7]

list2 = list(set(list1))

print(list2[::-1])





# COMMAND ----------

list1 = range(1,151)

list2 = [ i%2 for i in list1   ]

print(list2)

# COMMAND ----------

from pyspark.sql import SparkSession
s=['This is Tiupalu Muchivolu Muchivolu']

spark = SparkSession.builder.appName("tiru").getOrCreate()

rdd1 = sc.parallelize(s)
rdd2 = rdd1.flatMap(lambda x : x.split(' '))
rdd3 = rdd2.map(lambda word: (word,1))
rdd3.collect()
word_count_rdd = rdd3.reduceByKey(lambda x,y : x+y)
word_count_rdd.collect()


# COMMAND ----------

from pyspark.sql.functions import *

data = [("Tirupa&l",37),("Hari",8),("Karthi",2)]
df = spark.createDataFrame(data,["Name","Age"])
#df = df.filter(col('age') > 18)

df = df.withColumn("Name", regexp_replace(col('Name'), '[&!@#$%]', ''))
display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import *
list1= [1,2,4,5,7]
list2= [4,5,6,7,6]

list2 = list2[::-1]

print(list2)

print(zip(list1,list2))

# COMMAND ----------

from pyspark.sql.functions import *

data1=[(100,"Raj",None,1,"01-04-23",50000),
       (200,"Joanne",100,1,"01-04-23",4000),(200,"Joanne",100,1,"13-04-23",4500),(200,"Joanne",100,1,"14-04-23",4020)]

data2=[(1,"IT"),
       (2,"HR")]

emp_df = spark.createDataFrame(data1,["EmpId","EmpName","Mgrid","deptid","salarydt","salary"])

dept_df = spark.createDataFrame(data2,["deptid","deptname"])

display(emp_df)
display(dept_df)


# COMMAND ----------

join_df = emp_df.join(dept_df,"deptid","left")
display(join_df)

# COMMAND ----------


data = [("Alice","Badmentan,Tennies"), ("Bob","Criket,Tennies")]
df = spark.createDataFrame(data,["Name","Hobbies"])

df = df.select("Name", explode(split(df.Hobbies,",")))
display(df)

# COMMAND ----------

data = [('Goa','','AP'),('','AP',None),(None,'','bglr')]
column = ["City1","City2","City3"]

df = spark.createDataFrame(data,column)

df = df.withColumn("City",coalesce(when(col("City1")=="",None).otherwise(col("City1")),when(col("City2")=="",None).otherwise(col("City2")),when(col("City3")=="",None).otherwise(col("City3"))))
display(df)

# COMMAND ----------

data1=[(1,"A",1000,"IT"),(2,"B",1500,"IT"),(3,"C",2500,"IT"),(4,"D",3000,"HR"),(5,"E",2000,"HR"),(6,"F",1000,"HR")
       ,(7,"G",4000,"Sales"),(8,"H",4000,"Sales"),(9,"I",1000,"Sales"),(10,"J",2000,"Sales")]
schema1=["EmpId","EmpName","Salary","DeptName"]
df=spark.createDataFrame(data1,schema1)

df = df.withColumn("dept_salary", rank().over(Window.partitionBy(col("DeptName")).orderBy(col("Salary").desc()))).filter(col("dept_salary") == 1).drop(col("dept_salary"))

display(df)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
data=[(1,'2024-01-01',"I1",10,1000),(2,"2024-01-15","I2",20,2000),(3,"2024-02-01","I3",10,1500),(4,"2024-02-15","I4",20,2500),(5,"2024-03-01","I5",30,3000),(6,"2024-03-10","I6",40,3500),(7,"2024-03-20","I7",20,2500),(8,"2024-03-30","I8",10,1000)]
schema=["SOId","SODate","ItemId","ItemQty","ItemValue"]
df=spark.createDataFrame(data,schema)



# COMMAND ----------


df1 = df.withColumn("SODate_dt",to_date(col("SODate"),'yyyy-MM-dd')).withColumn("Year_Month",date_format(col("SODate_dt"),'MMM-yy')).withColumn("Sales",col("ItemValue")).select(col("Year_Month"),col("Sales")).groupBy(col("Year_Month")).(sum(col("Sales")).alias("Total_Sales"))\
    .lag(col("Total_Sales")).over(Window.orderBy(col("Year_Month")))

#df2 = df1.lag(col("Total_Sales")).over(Window.orderBy(col("Year_Month")))

#df2 = df1.groupBy(col("Year_Month")).agg(sum(col("Sales")).alias("Total_Sales")).withColumn("previews_sales", lag(col("Total_Sales")).over(Window.orderBy(col("Year_Month"))))

#df2 = df1.groupBy(col("Year_Month")).agg(sum(col("Sales")).alias("Total_Sales"))


display(df1)

# COMMAND ----------

from pyspark.sql.types import *
data=[(0,0,'start',0.712),(0,0,'end',1.520),(0,1,'start',3.140),(0,1,'end',4.120),
      (1,0,'start',0.550),(1,0,'end',1.550),(1,1,'start',0.430),(1,1,'end',1.420),
      (2,0,'start',4.100),(2,0,'end',4.512),(2,1,'start',2.500),(2,1,'end',5.000)]
schema=["Machine_id","processid","activityid","timestamp"]
df1=spark.createDataFrame(data,schema)
display(df1)

# COMMAND ----------

df2=df1.select(df1.Machine_id,df1.processid,when(df1.activityid=='start',df1.timestamp).alias('starttime'),when(df1.activityid=='end',df1.timestamp).alias('endtime'))
display(df2)

df3=df2.groupBy(df2.Machine_id,df2.processid).agg(max(df2.starttime).alias('starttime'),max(df2.endtime).alias('endtime'))
display(df3)

# COMMAND ----------

from pyspark.sql.types import *
data=[(1,'John','ADF'),(1,'John','ADB'),(1,'John','PowerBI'),(2,'Joanne','ADF'),(2,'Joanne','SQL'),(2,'Joanne','Crystal Report'),(3,'Vikas','ADF'),(3,'Vikas','SQL'),(3,'Vikas','SSIS'),(4,'Monu','SQL'),(4,'Monu','SSIS'),(4,'Monu','SSAS'),(4,'Monu','ADF')]
schema=["EmpId","EmpName","Skill"]
df1=spark.createDataFrame(data,schema)

df2 = df1.groupBy(col("EmpName")).agg(collect_list(col("Skill")).alias("Skills"))


#df3=df2.select(df2.EmpName,concat_ws(',',df2.Skills).alias('Skills'))
display(df2)

# COMMAND ----------

data = [("IT","M"),("IT","F"),("IT","M"),("IT","M"),
("HR","F"),("HR","F"),("HR","F"),("HR","F"),("HR","F"),
("Sales","M"),("Sales","M"),("Sales","F"),("Sales","M"),("Sales","M"),("Sales","F")
]

df = spark.createDataFrame(data,["Dept","Gender"])
df

# COMMAND ----------

df1 = df.groupBy(col("Dept")).agg(count(col("Gender")).alias("Total_Emp"),sum(when(col("Gender")=='M',1).otherwise(0)).alias("Males_Count"),sum(when(col("Gender")=='F',1).otherwise(0)).alias("Males_Count"))

df1.show()

# COMMAND ----------


