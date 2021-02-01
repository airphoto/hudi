
-- 读优化视图
CREATE EXTERNAL TABLE `hudi_test1_ro`(
  `_hoodie_commit_time` string,
  `_hoodie_commit_seqno` string,
  `_hoodie_record_key` string,
  `_hoodie_partition_path` string,
  `_hoodie_file_name` string,
  `timestamp` bigint,
  `_row_key` string,
  `rider` string,
  `fare` double,
  `_hoodie_is_deleted` boolean)
PARTITIONED BY (
  `driver` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-171642:9000/test/outoput/hudi/output/demo/base'

-- 实时优化视图
CREATE EXTERNAL TABLE `hudi_test1_rt`(
  `_hoodie_commit_time` string,
  `_hoodie_commit_seqno` string,
  `_hoodie_record_key` string,
  `_hoodie_partition_path` string,
  `_hoodie_file_name` string,
  `timestamp` bigint,
  `_row_key` string,
  `rider` string,
  `fare` double,
  `_hoodie_is_deleted` boolean)
PARTITIONED BY (
  `driver` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://emr-header-1.cluster-171642:9000/test/outoput/hudi/output/demo/base'