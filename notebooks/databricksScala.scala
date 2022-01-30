// Databricks notebook source
val containerName = "test"
val storageAccountName = "databrickslithindl"
val sas = "sp=racwdlmeop&st=2021-12-31T14:52:55Z&se=2021-12-31T22:52:55Z&spr=https&sv=2020-08-04&sr=c&sig=a9eC1PZCiFZ5HKIgxd6cMS%2BThWPQs7kPEfYq60vk9TE%3D"
val config = "fs.azure.sas."  + containerName+ "." + storageAccountName + ".blob.core.windows.net"

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://test@databrickslithindl.blob.core.windows.net",
  mountPoint = "/mnt/myfile",
  extraConfigs = Map(config -> sas))

// COMMAND ----------


val b= dbutils.fs.mounts
// for (name <- dbutils.fs.mounts) if(name.mountPoint.contains("myfile")) dbutils.fs.unmount(name.mountPoint) else println(name.mountPoint)

for (name <- b) println(name.mountPoint)


// COMMAND ----------

val mydf = spark.read
.option("header","true")
.option("inferSchema","true")
.csv("/mnt/myfile/1000_Sales_Records2.csv")



// COMMAND ----------

val fg = mydf.select("region", "Country").withColumnRenamed("region","region12")
display(fg)
fg.createOrReplaceTempView("SalesData")

// COMMAND ----------


val gfh = spark.sql("""select region12 , Country from SalesData group by region12,Country order by region12""")
display(gfh)


// COMMAND ----------

gfh.write
 .option("header", "true")
 .format("json")
 .save("/mnt/myfile/SalesProfitData3.csv")

// COMMAND ----------

gfh.write.saveAsTable("book")

// COMMAND ----------

val er = dbutils.fs.ls("databricks-datasets")
// for ( d <- er) println(dbutils.fs.ls(d.path))
display(er)

// COMMAND ----------

val df = spark.read.json("dbfs:/databricks-datasets/iot/iot_devices.json")
display(df)

// COMMAND ----------

