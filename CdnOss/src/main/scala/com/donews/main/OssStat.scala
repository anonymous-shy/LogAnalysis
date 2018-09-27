package com.donews.main

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by Shy on 2018/9/26
  */

object OssStat {
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val warehouseLocation = "hdfs://HdfsHA/data/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "240")
      .enableHiveSupport()
      .getOrCreate()

    val ossData = spark.read.textFile("/data/cdn_oss/oss/oss_file/OssData-data")

  }
}
