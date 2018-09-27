package com.donews.main

import org.apache.commons.lang3.StringUtils
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

    spark.udf.register("gen_url", (url: String) => {
      var tmpUrl = url
      if (url.contains("\""))
        tmpUrl = url.replaceAll("\"", "")
      if (url.contains("["))
        tmpUrl = url.replace("[", "")
      if (url.contains("]"))
        tmpUrl = url.replace("]", "")
      tmpUrl
    })
  }
}
