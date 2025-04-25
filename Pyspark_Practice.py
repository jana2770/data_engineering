# Databricks notebook source
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('mysparkprogram').getOrCreate()

# COMMAND ----------

df=spark.read.format('csv').option('header', 'true').load('/FileStore/tables/jana/SalesData.csv')


# COMMAND ----------

df.columns

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/jana

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType
schema=StructType([
StructField ('Name', StringType()),
StructField ('Dept', StringType()),
StructField ('Salary', IntegerType())
])
data22=[('Rahul','Sales',10000),('Bob','Finance',20000),('Ankit','Marketing',20000),('Jack','Reporting',15000)]
df22=spark.createDataFrame(data22,schema=schema)

# COMMAND ----------

df22.show()

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('sparksession').getOrCreate()

schema01=StructType([

StructField ('Name', StringType()),
StructField ('Dept', StringType()),
StructField('Salary', IntegerType())
])

df=spark.createDataFrame(data22,schema=schema01)
df.show()

# COMMAND ----------

# spark.read.format('csv').option('inferschema','false').schema(schema).load('/path/file')


# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import round
df=df.filter(df.Salary<20000).withColumn('bonus',round(df.Salary*0.1,0))
df.withColumn('total_salary',(df.bonus+df.Salary)).show()

df.write.format('parquet').mode('append').save('dbfs:/FileStore/tables/jana/employee.parquet')

# COMMAND ----------

schema03=StructType([

    StructField('firstname',StringType()),
    StructField('lastname',StringType())
])
data33=[('John','Smith'),('Jane','D!oe'),('Bob','Johnson'),('Mike','Pant$on'),('Tom','L#ee')]
df22=spark.createDataFrame(data33,schema=schema03)

# COMMAND ----------

df22.show()

# COMMAND ----------

from pyspark.sql.functions import when,col
df22.withColumn('newlastname',when(df22.lastname.contains('!'), ' ').otherwise(df22.lastname)).show()

# COMMAND ----------

data44 = [("1", "Alice", "123 Main Street, New York, USA"),("2", "Bob", "456 Oak Avenue, San Francisco, USA"),
    ("3", "Carol", "789 Pine Road, Los Angeles, USA"),("4", "David", "321 Elm Lane, Chicago, USA"),
    ("5", "Emily", "654 Maple Drive, Miami, USA")]
columns = ["emp_id", "emp_name", "emp_add"]
df44 = spark.createDataFrame(data44, schema=columns)

# COMMAND ----------

df44.show()

# COMMAND ----------

from pyspark.sql.functions import split
df44.withColumn('Address',split(df44.emp_add, ',').getItem(0))\
    .withColumn('city',split(df44.emp_add, ',').getItem(1))\
        .withColumn('state',split(df44.emp_add, ',').getItem(2)).show()      

# COMMAND ----------

data = [(101, "Alice", "Electronics", 2023, "Q1", 250.0),(102, "Bob", "Fashion", 2023, "Q1", 180.0),
(103, "Charlie", "Electronics", 2023, "Q2", 300.0),(104, "David", "Home Decor", 2023, "Q3", 150.0),
(105, "Eve", "Fashion", 2023, "Q4", 200.0),(106, "Frank", "Electronics", 2024, "Q1", 275.0),
(107, "Grace", "Home Decor", 2024, "Q2", 320.0),(108, "Hank", "Fashion", 2024, "Q3", 90.0),
(109, "Ivy", "Electronics", 2024, "Q4", 400.0),(110, "Jack", "Home Decor", 2024, "Q1", 175.0),]
columns = ["CustomerID", "Name", "Category", "Year", "Quarter", "AmountSpent"]
df = spark.createDataFrame(data, columns)
df.show()

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/jana

# COMMAND ----------

df11=spark.read.format('csv')\
            .option('inferschema','true')\
            .option('header','true')\
            .load('dbfs:/FileStore/tables/jana/SalesData.csv')
df11.display()

# COMMAND ----------

df11.write.format('parquet')\
    .partitionBy('Year')\
    .save('dbfs:/FileStore/tables/jana/SalesData')

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/jana/SalesData

# COMMAND ----------

df12=spark.read.format('parquet').load('dbfs:/FileStore/tables/jana/SalesData/Year=2020')
df12.display()

# COMMAND ----------

df11.write.format('parquet')\
    .bucketBy(4,'Branch_ID')\
    .sortBy('Branch_ID')\
    .saveAsTable('hive_metastore.01jana_schema.SalesDataBucketed')

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/jana

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema hive_metastore.01jana_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC use hive_metastore.01jana_schema

# COMMAND ----------

# MAGIC %fs ls /user/hive/hive_metastore/01jana_schema.db/SalesDataBucketed

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salesdatabucketed;

# COMMAND ----------

spark.sql("DESCRIBE FORMATTED hive_metastore.01jana_schema.SalesDataBucketed").show(truncate=False)


# COMMAND ----------

spark.sql("DESCRIBE EXTENDED SalesDataBucketed").show()

# COMMAND ----------

# MAGIC %fs ls hive_metastore.`01jana_schema`

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `hive_metastore`; select * from `01jana_schema`.`salesdatabucketed` limit 100;

# COMMAND ----------

spark.sql("USE default")

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/01jana_schema.db/salesdatabucketed
# MAGIC

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/jana

# COMMAND ----------

df=spark.read.format('csv').option('header', 'true').option('inferschema', 'true').option('mode', 'dropmalformed').load('dbfs:/FileStore/tables/jana/SalesData.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format('parquet').mode('append').partitionBy('BranchName','DealerName').save('dbfs:/FileStore/tables/jana/partitionedbybrancheswithdealers')

# COMMAND ----------


