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

CREATE TABLE logs.cdn_og(
host STRING,
access_ip STRING,
file_day STRING,
access_url STRING,
file_path STRING,
hitrate STRING,
httpcode STRING,
user_agent STRING,
responsesize_bytes BIGINT,
method STRING,
file_type STRING,
file_uri STRING,
referer_host STRING,
log_date STRING
)
PARTITIONED BY(dt STRING, bucket STRING)
STORED AS PARQUET
tblproperties ("orc.compress"="SNAPPY");


CREATE TABLE logs.oss_og(
remote_ip STRING,
log_timestamp STRING,
method STRING,
access_url STRING,
http_status STRING,
sentbytes BIGINT,
referer STRING,
useragent STRING,
hostname STRING,
bucket STRING,
key STRING,
objectsize BIGINT,
delta_datasize BIGINT,
sync_request STRING
)
PARTITIONED BY(dt STRING)
STORED AS PARQUET
tblproperties ("orc.compress"="SNAPPY");

CREATE TABLE logs.online_og(
uid STRING,
publishtime_str STRING,
utimestr STRING,
newsid STRING,
newsmode STRING,
file_uri STRING,
uri_type STRING
)
PARTITIONED BY(date STRING)
STORED AS PARQUET
tblproperties ("orc.compress"="SNAPPY");

CREATE TABLE logs.cdn_online(
bucket STRING,
access_ip STRING,
file_day STRING,
file_path STRING,
hitrate STRING,
httpcode STRING,
user_agent STRING,
responsesize_bytes BIGINT,
method STRING,
file_type STRING,
file_uri STRING,
uid STRING,
publishtime_str STRING,
utimestr STRING,
newsid STRING,
newsmode STRING,
uri_type STRING
)
PARTITIONED BY(dt STRING)
STORED AS PARQUET
tblproperties ("orc.compress"="SNAPPY");
