CREATE DATABASE IF NOT EXISTS de_spark_utils;

CREATE TABLE IF NOT EXISTS de_spark_utils.mor_smkt_str_shopperonecard
(
shopperid int,
onecardnumber string,
isprimary string,
ss_eff_tmstmp timestamp,
end_date  timestamp,
record_deleted_flag string,
ctl_id  smallint,
process_name  string,
process_id  int,
update_process_name string,
update_process_id int,
start_ts  timestamp,
end_ts  timestamp,
last_update_date  date,
insert_date date,
row_status_ind  string,
_unload_dt  date,
_code_ver string,
_sync_dt_utc  date,
start_date date
)
USING PARQUET
OPTIONS
(
  path '.data/mor_smkt_str_shopperonecard'
)
PARTITIONED BY (start_date);

MSCK REPAIR TABLE de_spark_utils.mor_smkt_str_shopperonecard;
