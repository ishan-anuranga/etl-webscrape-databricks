# Databricks notebook source
# MAGIC %md
# MAGIC # Install dependencies

# COMMAND ----------

# MAGIC %pip install -r requirements.txt

# COMMAND ----------

# MAGIC %md
# MAGIC # Scrape data using scraper

# COMMAND ----------

from Scraper import Scraper
from datetime import datetime

# COMMAND ----------

query = 'iphone 15'
scraper = Scraper()
data = scraper.scrape(query)

# COMMAND ----------

# MAGIC %md
# MAGIC # Add current date to data

# COMMAND ----------

now = datetime.now()
data['scrape_time'] = now

# COMMAND ----------

# MAGIC %md 
# MAGIC # Save data

# COMMAND ----------

dataset_location = "dbfs:/mnt/web-scrape-data/"
spark.conf.set("scrape.query", f"{query.replace(' ', '-')}")

# COMMAND ----------

dbutils.fs.put(f"{dataset_location}/{query.replace(' ', '-')}-{now.strftime('%m-%d-%Y_%H-%M-%S')}.csv", data.to_csv(index=False), overwrite=True)

# COMMAND ----------

# dbutils.fs.ls(dataset_location)
# dbutils.fs.rm(dataset_location, True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE scrape_raw;
# MAGIC
# MAGIC CREATE TABLE scrape_raw
# MAGIC   (title STRING, 
# MAGIC    link STRING,
# MAGIC    premium_member STRING,
# MAGIC    verified_member STRING,
# MAGIC    location STRING,
# MAGIC    category STRING,
# MAGIC    price STRING,
# MAGIC    top_ad STRING,
# MAGIC    img STRING,
# MAGIC    page_no INT,
# MAGIC    scrape_time TIMESTAMP)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true",
# MAGIC   delimiter = ","
# MAGIC )
# MAGIC LOCATION "dbfs:/mnt/web-scrape-data";

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM scrape_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC   SELECT 
# MAGIC     CASE
# MAGIC       WHEN lower(title) LIKE '%max%' THEN 'pro max'
# MAGIC       WHEN lower(title) LIKE '%pro%' THEN 'pro'
# MAGIC       WHEN lower(title) LIKE '%plus%' THEN 'plus'
# MAGIC       ELSE 'base'
# MAGIC     END AS variant,
# MAGIC     title,
# MAGIC     cast(replace(split_part(price, ' ', 2), ',') AS INT) AS price,
# MAGIC     scrape_time
# MAGIC   FROM scrape_raw
# MAGIC ) raw
# MAGIC ORDER BY raw.variant, raw.price 

# COMMAND ----------


