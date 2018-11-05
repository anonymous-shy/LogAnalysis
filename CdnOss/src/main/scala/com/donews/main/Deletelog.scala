package com.donews.main

import com.donews.Deletelog_bean
import com.donews.main.CDN_Process.getClass
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Deletelog {

  val Log = LoggerFactory.getLogger(Deletelog.getClass)

  def main(args: Array[String]): Unit = {

//    val desTable = args(0)
//    val dt = args(1).replaceAll("_", "-")

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

      val file = s"/data/cdn_oss/deletelog/ylzx-delete-data20181026/*"
      println("=================================================================================================")
      println(s"===========================================$file======================================================")

      val deletelog_rdd = spark.sparkContext.textFile(file)
      val deletelog_bean = deletelog_rdd.map(log => logProcess(log)).toDF()//.registerTempTable("logDF")

      println(s"===========================================写hive======================================================")

    deletelog_bean.write.parquet("/data/cdn_oss/deletelog/ylzx-delete-data26.parquet")
      //sqlContext.sql(s"insert overwrite table ${desTable} partition(dt='${dt}' ) select log_timestamp, method, bucket, key,delta_DataSize from logDF")


  }
  def logProcess(log:String): Deletelog_bean= {

    try{
      val DATA = "(.*?)"
      //--------------------------------------------------------------------------s------------r---------u---------h---------------------------------b---------k--------o----s-----e---r----u------d-------s----------
      val pattern = s"${DATA} - - \\[${DATA}\\] #${DATA} ${DATA} .*?# ${DATA} ${DATA} .*? #${DATA}# #${DATA}# #.*?# #.*?# #.*?# #.*?# #${DATA}# #${DATA}# #${DATA}# ${DATA} .*? #.*?# .*? #.*?# ${DATA} #${DATA}# #.*?#".r
      var pattern(remote_IP, log_timestamp, method1, access_url, hTTP_Status, sentBytes, referer, userAgent, method, bucket, key, objectSize, delta_DataSize, sync_Request) = log.replaceAll("\"", "#")

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



      if(delta_DataSize.equals("-")) {
        delta_DataSize = "0"}

      access_url = access_url.replaceAll("%2F","/").toLowerCase()
      val saved_img_url = key.replaceAll("%2F", "/")
      key = key.replaceAll("%2F", "/").toLowerCase()
      Deletelog_bean(log_timestamp, method, bucket, key,delta_DataSize.toLong)

    }
    catch {
      case e:Exception => Deletelog_bean("",  "", "", "", 0)
    }
      }
}
