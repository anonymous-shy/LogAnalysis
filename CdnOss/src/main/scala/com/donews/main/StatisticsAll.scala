package com.donews.main

import com.donews.utils.CommonUtils.plusDays
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by Shy on 2018/8/30
  */

object StatisticsAll {
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val today = args(0) // 取执行当天日期,处理的文件日期为today-1
    //判断日期取 当日,3日,7日 日期
    val b1D = (plusDays(today, -1), "currentDay")
    val b3D = (plusDays(today, -3), "before3Days")
    val b7D = (plusDays(today, -7), "before7Days")

    val warehouseLocation = "hdfs://HdfsHA/data/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
  }

  def process(spark: SparkSession, today: String, dayType: (String, String)): Unit = {
    val currentDay = plusDays(today, -1)
    val resConf = ConfigFactory.load()
    val prop = new java.util.Properties()
    prop.put("driver", resConf.getString("db.default.driver"))
    prop.put("user", resConf.getString("db.default.user"))
    prop.put("password", resConf.getString("db.default.password"))
    val dbUrl = resConf.getString("db.default.url")
    val domainMap: Map[String, String] = Map("donewsdata.donews.com" -> "donewsdataoss",
      "niuerdata.g.com.cn" -> "donews-data",
      "donewsdataoss.g.com.cn" -> "donewsdataoss",
      "vdata.g.com.cn" -> "donews-data",
      "donewsdataoss.donews.com" -> "donewsdataoss",
      "niuerdata.donews.com" -> "donews-data")
    val domainBD = spark.sparkContext.broadcast[Map[String, String]](domainMap)
    spark.udf.register("gen_url", (url: String) => StringUtils.substringAfter(url.split("//")(1), "/").split("\\?")(0).toLowerCase)
    spark.udf.register("gen_domain", (host: String) => {
      val domain = domainBD.value.getOrElse(host, "")
      domain
    })
  }
}
