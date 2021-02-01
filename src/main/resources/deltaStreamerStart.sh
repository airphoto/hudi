#!/usr/bin/env bash
spark-submit --master yarn \
--driver-memory 1G \
--num-executors 2 \
--executor-memory 1G \
--executor-cores 4 \
--deploy-mode cluster \
--conf spark.yarn.executor.memoryOverhead=512 \
--conf spark.yarn.driver.memoryOverhead=512 \
--class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.11-0.8.0-SNAPSHOT.jar \
--props /test/input/hudi/demo/deltaStreamerStart_kafka.properties \
--schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
--source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
--target-base-path /test/outoput/hudi/output/demo/base \
--op UPSERT \
--target-table hudi_test1 \
--enable-hive-sync \
--table-type MERGE_ON_READ \
--source-ordering-field timestamp \
--source-limit 5000000 \
--continuous


spark-shell --master yarn --jars packaging/hudi-spark-bundle/target/hudi-spark-bundle_2.11-0.8.0-SNAPSHOT.jar,spark-avro_2.11-2.4.3.jar \
--conf spark.sql.hive.convertMetastoreParquet=false \
--num-executors 1 --driver-memory 1g --executor-memory 2g

sh run_sync_tool.sh --user '' --pass '' \
--table hudi_test1 --database hudi \
--jdbc-url jdbc:hive2://10.122.238.97:10000 \
--base-path /test/outoput/hudi/output/demo/base \
--partition-value-extractor org.apache.hudi.hive.MultiPartKeysValueExtractor \
--partitioned-by 'driver'