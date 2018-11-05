package com.donews.main

import com.donews.main.Online_Process.getClass
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Online_Statistics {
  def main(args: Array[String]): Unit = {

    val desTable = args(0)
    var date  = args(1)

    val conf = new SparkConf()
      .setAppName("Log")

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

    val online = spark.table("logs.online_og")

   // val online_broadcast = sc.broadcast(online)
    date = date.replaceAll("_","-")
    val cdn = spark.sql(s"select file_day,file_uri,sum(responsesize_bytes) as responsesize_bytes,host,access_ip,access_url,file_path,hitrate,httpcode,user_agent,method,file_type from logs.cdn_og where dt='${date}' group by file_day,file_uri,host,access_ip,access_url,file_path,hitrate,httpcode,user_agent,method,file_type")//.rdd

    online.join(cdn, Seq("file_uri"), "rightouter").createOrReplaceTempView("online_statistics")


    spark.sql(s"insert overwrite table ${desTable} partition(dt='${date}' ) select host,access_ip,file_day,access_url,file_path,hitrate,httpcode,user_agent,responsesize_bytes,method,file_type,file_uri,uid,publishtime_str,utimestr,newsid,newsmode,uri_type from online_statistics")


  }

  def findstr(str: String,s:String):Int = {

    (str.length - str.replaceAll(s,"").length)/s.length
  }
}
