# Databricks notebook source
import io
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.functions import udf, monotonically_increasing_id

filepath="/FileStore/tables/ds_assignment/corp_pfd.dif"
# read from filepath
read_file_io = open(filepath, "r")
data=read_file_io.read()

# cleaning the data and schema from input source file
clean_data=data.split("START-OF-DATA")[1].split("END-OF-DATA")[0]
clean_data_schema=data.split("START-OF-FIELDS")[1].split("END-OF-FIELDS")[0].strip().split("\n")
clean_data_schema=list(filter(lambda clean_data_schema:  clean_data_schema.find("#")!= 0 and len(clean_data_schema)!=0, clean_data_schema))
# creating spark dataframe from pandas read
df =spark.createDataFrame(pd.read_csv(io.StringIO(clean_data), sep="|", header=None,names=clean_data_schema,index_col=False))
# creating Temp view
df.createOrReplaceTempView("input_data")

# COMMAND ----------

# reading securities file
securities_df = spark.read.csv("/FileStore/tables/ds_assignment/reference_securities.csv",header = True)
reference_fld_df = spark.read.csv("/FileStore/tables/ds_assignment/reference_fileds.csv",header = True)
# creating ref field temp view.
reference_fld_df.createOrReplaceTempView("ref_fld")
# creating securities Temp view.
securities_df.createOrReplaceTempView("securities_data")

# COMMAND ----------

# filtering new securities based on source file and reference securities file
new_securities_df = spark.sql("""select distinct a.id_bb_global,a.id_isin,a.id_cusip,a.id_sedol1,a.ticker,a.name,a.exch_code
,a.issuer,a.market_sector_des from input_data a 
left join securities_data b 
on a.id_bb_global = b.id_bb_global
where b.id_bb_global is null""")

cols = new_securities_df.columns
# storing the resultant data into location
new_securities_df.write.csv("/FileStore/tables/new_securities.csv")

# COMMAND ----------

new_securities_df.createOrReplaceTempView("new_securities")

# COMMAND ----------

# creating securities data with spark dataframe
security_data = (spark.sql("""select * from input_data where id_bb_global in (select id_bb_global from new_securities)""")
                 .withColumn("TSTAMP",lit(current_timestamp()))
                 .withColumn("SOURCE",lit("corp_pfd.dif"))
                 .withColumn("FIELD", udf(lambda id: cols[id])(monotonically_increasing_id()))
                 .withColumn("VALUE",udf(lambda id: new_securities_df.select(cols[id])(monotonically_increasing_id())).select("ID_BB_GLOBAL","FIELD","VALUE","SOURCE","TSTAMP")
                )
# Storing securities data at location
security_data.write.csv("/FileStore/tables/security_data.csv")