package com.donews.main

import java.time.LocalDate

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
      .config("spark.sql.shuffle.partitions", "2000")
      .enableHiveSupport()
      .getOrCreate()

    //    val ossAll = spark.read.textFile("/data/cdn_oss/oss/oss_file/OssData-data").selectExpr("lower(value) as oss_uriA")
    //    val ossOnline = spark.sql("select file_uri as oss_uriO from logs.online_og")
    //
    //    ossAll.join(ossOnline, ossAll("oss_uriA") === ossOnline("oss_uriO"), "left")
    //      .write.parquet("/data/cdn_oss/output/OssData.parquet")
    val currDate = LocalDate.now.toString

    spark.udf.register("gen_url", (url: String) => {
      var tmpUrl = url
      if (url != null && url.startsWith("."))
        tmpUrl = url.substring(1)
      tmpUrl
    })

    val ossA = spark.read.parquet("/data/cdn_oss/output/OssData.parquet")
      .selectExpr("concat('/',oss_uriA) as oss_uriA", "oss_uriO")

    val img = spark.read.csv("/data/cdn_oss/mongo/article_photo.csv")
      .selectExpr("_c0 as img_url", "_c1 as crawl_date", "_c2 as saved_img_url", "gen_url(lower(_c2)) as url")
    val video = spark.read.csv("/data/cdn_oss/mongo/article_video.csv")
      .selectExpr("_c0 as crawl_date", "_c1 as saved_img_url", "gen_url(lower(_c1)) as url", "_c2 as img_url")

//    val imgJoin = img.join(ossA, img("saved_img_url") === ossA("oss_uriO"), "left_outer")
    val imgJoin = ossA.join(img, ossA("oss_uriA") === img("url"), "left_outer")
    imgJoin.createOrReplaceTempView("img")
    spark.sql(
      s"""
         | SELECT oss_uriA,img_url,crawl_date,saved_img_url,'$currDate' as process_date,
         | case when oss_uriO IS NOT NULL then '1'
         |      when oss_uriO IS NULL then '0'
         | end onlineSign,
         | case when img_url IS NOT NULL then '1'
         |      when img_url IS NULL then '0'
         | end spider_relative
         | FROM img
      """.stripMargin).write.parquet("/data/cdn_oss/output/oss_online/oss_online_img.parquet")

    ossA.join(video, ossA("oss_uriA") === video("url"), "left_outer").createOrReplaceTempView("video")
    spark.sql(
      s"""
         | SELECT oss_uriA,img_url,crawl_date,saved_img_url,'$currDate' as process_date,
         | case when oss_uriO IS NOT NULL then '1'
         |      when oss_uriO IS NULL then '0'
         | end onlineSign,
         | case when img_url IS NOT NULL then '1'
         |      when img_url IS NULL then '0'
         | end spider_relative
         | FROM video
      """.stripMargin).write.parquet("/data/cdn_oss/output/oss_online/oss_online_video.parquet")
  }
}
