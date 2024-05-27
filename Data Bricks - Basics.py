# Databricks notebook source
# MAGIC %md ## Mark Down Cell
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC  #--> Heading1
# MAGIC  ##--> Heading2
# MAGIC  ####### Bold
# MAGIC  1. HTML style  <b>  or two Astricks **
# MAGIC  2. Italic single aastric *
# MAGIC  3. Inline code "` `"  --> `print(k) `
# MAGIC  4. Multy inline code --> ```  This is
# MAGIC   multyline code ``` 
# MAGIC  
# MAGIC  list :
# MAGIC  - one
# MAGIC  - two
# MAGIC  - three
# MAGIC
# MAGIC
# MAGIC To highlight something :
# MAGIC
# MAGIC To highlight something
# MAGIC
# MAGIC <span style="background-color: #FFFF00"> Highlight this </span>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Click here [Profile Pic](https://media.licdn.com/dms/image/D4D03AQExNjLivgJEOg/profile-displayphoto-shrink_800_800/0/1679333627995?e=1722470400&v=beta&t=nLd9CU-GYMK-vWUprdGr7tFVJlIliQOFIMv0sIHnLCc)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Magic commands :
# MAGIC

# COMMAND ----------

# MAGIC %python # python
# MAGIC %scala #  scala
# MAGIC %sql   #  SQL
# MAGIC %md   # Mark down
# MAGIC %r  # R language 
# MAGIC %fs # file system 

# COMMAND ----------

# MAGIC %lsmagic

# COMMAND ----------

# MAGIC %md 
# MAGIC #DB Utils 
# MAGIC
# MAGIC Most commanlyused Db utils are  
# MAGIC - File system utilities
# MAGIC - Widget utilities
# MAGIC - Notebook utilities

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

path = 'dbfs:/FileStore/tables/customers.csv'

dbutils.fs.ls(path)

# COMMAND ----------

# MAGIC %md 
# MAGIC #Widget Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Combo Box

# COMMAND ----------

dbutils.widgets.combobox(name= 'combobox', defaultValue='Employee',choices=["Employee","Developer","Tester","Manager"], label="Department Label")

# COMMAND ----------

dbutils.widgets.get("combobox")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Drop down

# COMMAND ----------

dbutils.widgets.dropdown(name= 'dropdown', defaultValue='Employee',choices=["Employee","Developer","Tester","Manager"], label="Department Label")

# COMMAND ----------

dbutils.widgets.get("dropdown")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Multiselect

# COMMAND ----------

dbutils.widgets.multiselect(name= 'multiselect', defaultValue='Employee',choices=["Employee","Developer","Tester","Manager"], label="Department Label")

# COMMAND ----------

dbutils.widgets.get("multiselect")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Text

# COMMAND ----------

dbutils.widgets.text(name='text_name', defaultValue=' ', label="Please enter Text Label")

# COMMAND ----------

result = dbutils.widgets.get("text_name")

print(f'select * from schema.Table where  year ={result} ')

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Notebook Utilities

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

sFDSABFDBAFDAFDFADNFMDAFDAFA
