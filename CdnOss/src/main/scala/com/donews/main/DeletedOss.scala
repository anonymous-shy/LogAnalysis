package com.donews.main

import org.apache.spark.sql.SparkSession

/**
  * Created by Shy on 2018/10/16
  */

object DeletedOss {

  def main(args: Array[String]): Unit = {
    val currdate = args(0)
    val warehouseLocation = "hdfs://HdfsHA/data/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "2000")
      .enableHiveSupport()
      .getOrCreate()

    spark.udf.register("gen_url", (url: String) => {
      var tmpUrl = url
      if (url != null && url.startsWith("."))
        tmpUrl = url.substring(1)
      tmpUrl
    })

    val img = spark.read.csv(s"/data/cdn_oss/output/oss_online/process/spider_check_img_$currdate")
      .selectExpr("_c0 as crawl_date",
        "_c1 as img_url",
        "_c2 as onlineSign",
        "_c3 as saved_img_url",
        "_c4 as sign_date",
        "_c5 as spider_relative",
        "gen_url(lower(_c2)) as url")

    val online = spark.table("logs.online_og").selectExpr("concat('/',file_uri) as online_url")

    img.join(online, img("url") === online("online_url"), "left_outer").createOrReplaceTempView("del_oss")

    spark.sql(
      s"""
         | SELECT crawl_date,img_url,onlineSign,saved_img_url,sign_date,spider_relative,
         | case when online_url IS NOT NULL then 'N'
         |      when online_url IS NULL then 'Y'
         | end deleteSign
         | FROM del_oss
       """.stripMargin)
      .write.json(s"/data/cdn_oss/output/oss_online/delete/delete_check_img_$currdate")
  }
}
