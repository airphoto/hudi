package com.lhs.hudi.demo


import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * 测试hudi的例子1
  */
object Demo1 {

  def main(args: Array[String]): Unit = {


  }

  def singlePartition(spark:SparkSession):Unit = {
    val sourceDF: DataFrame = spark.sql("select * from hudi.user_info_demo")

    sourceDF
      .withColumn("bir_date", from_unixtime(unix_timestamp(col("bir_date"), "yyyyMMdd"), "yyyy/MM/dd"))
      .write
      .format("org.apache.hudi")
      // 唯一id列名，可以指定多个字段
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "uuid")
      // 指定更新字段，该字段数值大的会覆盖小的
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "update_date")
      // 指定 partitionpath
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "bir_date")
      // 当前数据的分区变更时，数据的分区目录是否变化
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      // 设置索引类型目前有HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局GLOBAL_BLOOM
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
      // hudi table名
      .option(HoodieWriteConfig.TABLE_NAME, "hudi_hive_sync")
      // 设置并行度
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      // 同步hive参数
      // hive database
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "hudi")
      // hive table
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, "hudi_hive_sync")
      // 设置数据集注册并同步到hive
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true")
      // hive表分区字段
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "bir_year,bir_month,bir_day")
      // hiveserver2 地址
      .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY, "jdbc:hive2://10.122.238.97:10000")
      // 从partitionpath中提取hive分区对应的值，MultiPartKeysValueExtractor使用的是"/"分割
      // 此处可以自己实现，继承PartitionValueExtractor 重写 extractPartitionValuesInPath(partitionPath: String)方法即可
      // 写法可以点进MultiPartKeysValueExtractor类中查看
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, "org.apache.hudi.hive.MultiPartKeysValueExtractor")
      .mode(SaveMode.Append)
      .save("/test/output/hudi/demo/hudi_hive_sync")
  }

}
