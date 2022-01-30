// Databricks notebook source
dbutils.fs.mount("gs://testbucketwhite", "/mnt/myfile")


// COMMAND ----------

display(dbutils.fs.ls("/mnt/myfile"))

// COMMAND ----------

df = spark.read.format("parquet").load("gs://<bucket-name>/<path>")

// COMMAND ----------


val sampleDataFilePath = "dbfs:/mnt/myfile/book.csv";

val isHeaderOn = "true"
val isInferSchemaOn = "false"




val sample1DF = spark.read.format("csv")
.option("header", isHeaderOn)
.option("inferSchema", isInferSchemaOn)
.option("treatEmptyValuesAsNulls", "false")
.load(sampleDataFilePath);
display(sample1DF);

// COMMAND ----------

sample1DF.select("deposit").count;

// COMMAND ----------

