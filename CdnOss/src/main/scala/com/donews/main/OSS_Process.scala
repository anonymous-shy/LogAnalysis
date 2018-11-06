package com.donews.main

import com.donews.OSS_bean
import com.donews.main.Online_Process.getClass
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object OSS_Process {

  val Log = LoggerFactory.getLogger(OSS_Process.getClass)

  def main(args: Array[String]): Unit = {

    val desTable = args(0)
    val dt = args(1).replaceAll("_", "-")

    val warehouseLocation = "hdfs://HdfsHA/data/user/hive/warehouse"
    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "240")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

      val file = s"/data/cdn_oss/oss/$dt/*${dt}*"
      println("=================================================================================================")
      println(s"===========================================$file======================================================")

      val oss_rdd = spark.sparkContext.textFile(file)
      val oss_bean = oss_rdd.map(log => logProcess(log)).filter(!_.access_url.equals("")).toDF().createOrReplaceTempView("logDF")

      println(s"===========================================写hive======================================================")


      spark.sql(s"insert overwrite table ${desTable} partition(dt='${dt}' ) select * from logDF")


  }
  def logProcess(log:String): OSS_bean= {

    try{
      val DATA = "(.*?)"
      //--------------------------------------------------------------------------s------------r---------u---------h---------------------------------b---------k--------o----s-----e---r----u------d-------s----------
      val pattern = s"${DATA} - - \\[${DATA}\\] #${DATA} ${DATA} .*?# ${DATA} ${DATA} .*? #${DATA}# #${DATA}# #${DATA}# #.*?# #.*?# #.*?# #.*?# #${DATA}# #${DATA}# ${DATA} .*? #.*?# .*? #.*?# ${DATA} #${DATA}# #.*?#".r
      var pattern(remote_IP, log_timestamp, method, access_url, hTTP_Status, sentBytes, referer, userAgent, hostName, bucket, key, objectSize, delta_DataSize, sync_Request) = log.replaceAll("\"", "#")

      //    val map = Map(
      //      "remote_ip" -> remote_ip,
      //      "log_timestamp" -> log_timestamp,
      //      "method" -> method,
      //      "access_url" -> access_url,
      //      "http_status" -> http_status,
      //      "sentbytes" -> sentbytes,
      //      "referer" -> referer,
      //      "useragent" -> useragent,
      //      "hostname" -> hostname,
      //      "bucket" -> bucket,
      //      "key" -> key,
      //      "objectsize" -> objectsize,
      //      "delta_datasize" -> delta_datasize,
      //      "sync_request" -> sync_request
      //    )

      if(sentBytes.equals("-")) {sentBytes = "0"}
      if(objectSize.equals("-")) {
        objectSize = "0"}
      if(delta_DataSize.equals("-")) {
        delta_DataSize = "0"}

      access_url = access_url.replaceAll("%2F","/").toLowerCase()
      val saved_img_url = key.replaceAll("%2F", "/")
      key = key.replaceAll("%2F", "/").toLowerCase()
      OSS_bean(remote_IP, log_timestamp, method, access_url, hTTP_Status, sentBytes.toLong, referer, userAgent, hostName, bucket, key, objectSize.toLong, delta_DataSize.toLong, sync_Request, saved_img_url)

    }
    catch {
      case e:Exception => OSS_bean("", "", "", "", "", 0, "", "", "", "", "",0, 0, "","")
    }
      }
}
