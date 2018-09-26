package com.donews.main

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by Shy on 2018/8/20
  */

object TmpStatistics {
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    // val currdate = "2018-08-18"
    val currDate = args(0)
    val resConf = ConfigFactory.load()
    val prop = new java.util.Properties()
    prop.put("driver", resConf.getString("db.default.driver"))
    prop.put("user", resConf.getString("db.default.user"))
    prop.put("password", resConf.getString("db.default.password"))
    val dbUrl = resConf.getString("db.default.url")
    val warehouseLocation = "hdfs://HdfsHA/data/user/hive/warehouse"
    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "240")
      .enableHiveSupport()
      .getOrCreate()

    //    val sparkConf = new SparkConf()
    //      .setAppName(getClass.getSimpleName)
    //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //      .set("spark.sql.shuffle.partitions", "240")
    //    val sc = new SparkContext(sparkConf)
    //    val sqlContext = new HiveContext(sc)
    import spark.sql

    spark.table("logs.cdn_og").createOrReplaceTempView("cdn_og1")
    sql(
      s"""
         |select file_day,responsesize_bytes
         |from cdn_og1
         |where dt = '$currDate' and bucket = 'niuerdata.g.com.cn'
         """.stripMargin).createOrReplaceTempView("cdn")

    sql(
      """
        | SELECT
        | file_day,SUM(responsesize_bytes) as sum_curr_day,
        | SUM(SUM(responsesize_bytes)) OVER (ORDER BY file_day asc ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS sum2days,
        | SUM(SUM(responsesize_bytes)) OVER (ORDER BY file_day asc ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sum3days,
        | SUM(SUM(responsesize_bytes)) OVER (ORDER BY file_day asc ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS sum4days,
        | SUM(SUM(responsesize_bytes)) OVER (ORDER BY file_day asc ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sum5days,
        | SUM(SUM(responsesize_bytes)) OVER (ORDER BY file_day asc ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS sum6days,
        | SUM(SUM(responsesize_bytes)) OVER (ORDER BY file_day asc ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sum7days,
        | SUM(SUM(responsesize_bytes)) OVER (ORDER BY file_day asc ROWS BETWEEN UNBOUNDED PRECEDING AND unbounded following) AS sumAlldays
        | FROM cdn
        | GROUP BY file_day
        | ORDER BY file_day desc
      """.stripMargin)
      .where(s"file_day = '$currDate'")
      .write.mode(SaveMode.Append).jdbc(dbUrl, "tmp_stat1", prop)
  }
}
