#!/usr/bin/env bash
spark-submit --master yarn \
--driver-memory 1G \
--num-executors 2 \
--executor-memory 1G \
--executor-cores 4 \
--deploy-mode cluster \
--conf spark.yarn.executor.memoryOverhead=512 \
--conf spark.yarn.driver.memoryOverhead=512 \
--class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer `ls /.../hudi-utilities-bundle_2.11-0.5.2-SNAPSHOT.jar` \
--props hdfs://../kafka.properties \
--schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
--source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
--target-base-path hdfs://../business \
--op UPSERT \
--target-table business \ \\'这里其实并不是hive表的名称，实际表名是在kafka.properties中配置'
--enable-hive-sync \ '开启同步至hive'
--table-type MERGE_ON_READ \
--source-ordering-field create_time \
--source-limit 5000000