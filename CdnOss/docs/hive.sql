#### Hive SQL ####
CREATE TABLE logs.OssCdn(
bucket string,
method string,
delta_datasize bigint,
hitrate string,
ip string,
uri string,
responsesize_bytes bigint,
ua string,
user string
)
PARTITIONED BY(dt STRING,day_type STRING)
STORED AS PARQUET
tblproperties ("orc.compress"="SNAPPY");