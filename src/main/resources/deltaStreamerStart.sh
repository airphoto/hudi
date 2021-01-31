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
--props /test/input/hudi/demo/kafka.properties \
--schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
--source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
--target-base-path /test/outoput/hudi/output/demo/base \
--op UPSERT \
--target-table business \
--enable-hive-sync \
--table-type MERGE_ON_READ \
--source-ordering-field timestamp \
--source-limit 5000000 \
--continuous