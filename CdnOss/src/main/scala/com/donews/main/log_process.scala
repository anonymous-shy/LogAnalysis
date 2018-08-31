package com.donews.main

import java.text.SimpleDateFormat
import java.util.Locale

import com.alibaba.fastjson.JSON
import com.donews.{CDN_bean, OSS_bean, Online_bean}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object log_process {

  def cdn_logProcess2hive(sc :SparkContext, spark:SparkSession, dt:String, desTable:String): Unit ={
    val fileArr = Array(
      s"/data/cdn_oss/cdn/donewsdataoss.g.com.cn_${dt}*",
      s"/data/cdn_oss/cdn/www.cdn.g.com.cn_${dt}*",
      s"/data/cdn_oss/cdn/wap.cdn.g.com.cn_${dt}*",
      s"/data/cdn_oss/cdn/niuerdata.g.com.cn_${dt}*",
      s"/data/cdn_oss/cdn/niuerdata.donews.com_${dt}*",
      s"/data/cdn_oss/cdn/donewsdataoss.g.com.cn_${dt}*",
      s"/data/cdn_oss/cdn/donewsdata.donews.com_${dt}*"
    )
    fileArr.foreach(file => {

      println("=================================================================================================")
      val bucket = StringUtils.substringAfterLast(file, "/").split("_")(0)
      println(s"===========================================$file======================================================")
      import spark.implicits._

      val cdn_rdd = sc.textFile(file)
      val cdn_bean = cdn_rdd.map(log => cdn_logProcess2bean(log)).filter(!_.access_url.equals("")).toDF().createOrReplaceTempView("logDF")

      println(s"===========================================写hive======================================================")


      spark.sql(s"insert overwrite table ${desTable} partition(dt='${dt.replaceAll("_", "-")}' , bucket ='${bucket}' ) select * from logDF")
    })

  }

  def cdn_logProcess2bean(log: String): CDN_bean = {

    try {

      val map: java.util.Map[String, String] = cdn_logProcess2Map(log)

      val method = map.getOrElse("method", "-").toString
      var responsesize_bytes = map.getOrElse("responsesize_bytes", "0").toString
      val log_timestamp = map.getOrElse("log_timestamp", "-").toString
      val user_agent = map.getOrElse("user_agent", "-").toString
      val httpcode = map.getOrElse("httpcode", "-").toString
      val hitrate = map.getOrElse("hitrate", "-").toString
      val file_path = map.getOrElse("file_path", "-").toString
      val access_url = map.getOrElse("access_url", "-").toString.toLowerCase
      val access_ip = map.getOrElse("access_ip", "-").toString
      val referer = map.getOrElse("referer", "-").toString


      var referer_host: String = null
      if (referer.split("/").length >= 2) {
        referer_host = referer.split("/")(2)
      }


      val file_type = file_path.split("/")(0)
      val arr = access_url.split("/")

      var file_day = "-"
      if (arr.length == 10) {
        file_day = arr(arr.length - 4) + "-" + arr(arr.length - 3) + "-" + arr(arr.length - 2)
      }
      val host = arr(2)


      val smp = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
      val smp2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
      val log_date = smp2.format(smp.parse(log_timestamp))

      val file_uri = access_url.toString.split(host + "/")(1).split("\\?")(0).toLowerCase
      if (responsesize_bytes.equals("-")) {
        responsesize_bytes = "0"
      }
      CDN_bean(host, access_ip, file_day, access_url, file_path, hitrate, httpcode, user_agent, responsesize_bytes.toLong, method, file_type, file_uri, log_date, referer.split("/")(2))

    } catch {

      case e: Exception =>
        println(s"==============================解析错误 ： ${log}==================================")
        CDN_bean("", "", "", "", "", "", "", "", 0, "", "", "", "", "")
    }

  }

  def cdn_logProcess2Map(log: String): java.util.Map[String, String] = {
    val DATA = "(.*?)"

    val pattern = s"\\[${DATA}\\] ${DATA} ${DATA} ${DATA} #${DATA}# #${DATA} ${DATA}# ${DATA} ${DATA} ${DATA} ${DATA} #${DATA}# #${DATA}#".r
    var pattern(log_timestamp, access_ip, temp, responsetime, referer, method, access_url, httpcode, requestsize_bytes, responsesize_bytes, hitrate, user_agent, file_path) = log.replaceAll("\"", "#")

    val map = Map(
      "log_timestamp" -> log_timestamp,
      "access_ip" -> access_ip,
      "responsetime" -> responsetime,
      "referer" -> referer,
      "method" -> method,
      "access_url" -> access_url,
      "httpcode" -> httpcode,
      "requestsize_bytes" -> requestsize_bytes,
      "responsesize_bytes" -> responsesize_bytes,
      "hitrate" -> hitrate,
      "user_agent" -> user_agent,
      "hitrate" -> hitrate,
      "file_path" -> file_path
    )
    map
  }

  def oss_logProcess2hive(sc: SparkContext, spark: SparkSession, dt: String, desTable: String): Unit ={

    import spark.implicits._
    val file = s"/data/cdn_oss/oss/niuer-donews-data${dt}*"
    println("=================================================================================================")
    println(s"===========================================$file======================================================")

    val oss_rdd = sc.textFile(file)
    val oss_bean = oss_rdd.map(log => oss_logProcess2bean(log)).filter(!_.access_url.equals("")).toDF().createOrReplaceTempView("logDF")

    println(s"===========================================写hive======================================================")


    spark.sql(s"insert overwrite table ${desTable} partition(dt='${dt}' ) select * from logDF")

  }

  def oss_logProcess2bean(log: String): OSS_bean = {

    try {
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

      if (sentBytes.equals("-")) {
        sentBytes = "0"
      }
      if (objectSize.equals("-")) {
        objectSize = "0"
      }
      if (delta_DataSize.equals("-")) {
        delta_DataSize = "0"
      }

      access_url = access_url.replaceAll("%2F", "/").toLowerCase()
      key = key.replaceAll("%2F", "/").toLowerCase()
      OSS_bean(remote_IP, log_timestamp, method, access_url, hTTP_Status, sentBytes.toLong, referer, userAgent, hostName, bucket, key, objectSize.toLong, delta_DataSize.toLong, sync_Request)

    }
    catch {
      case e: Exception => OSS_bean("", "", "", "", "", 0, "", "", "", "", "", 0, 0, "")
    }
  }

  def online_logProcess2hive(sc: SparkContext, spark: SparkSession, dt: String, desTable: String): Unit ={
    import spark.implicits._

    //yinli_201705.json.gz
      val filePath = s"/data/cdn_oss/online/*${dt}*"
      val online = sc.textFile(filePath)
      online.flatMap(log => {
        val objs: ArrayBuffer[Online_bean] = ArrayBuffer[Online_bean]()
        val obj = JSON.parseObject(log)
        val uid = obj.getString("uid")
        val publishtime_str = obj.getString("publishtime_str")
        val utimestr = obj.getString("utimestr")
        val newsid = obj.getString("newsid")
        val newsmode = obj.getString("newsmode")
        var content_list = obj.getString("content_list")
        var coverthumbnailimglists = obj.getString("coverthumbnailimglists")
        var videolists = obj.getString("videolists")


        //       content_list
        if (content_list == null || content_list.isEmpty) {
        } else {
          val content_lists = content_list.split(",").map(str => {
            if (str.contains("http")) {
              try {
                val host = str.split("/")(2)
                str.split(host + "/")(1).toLowerCase
              } catch {
                case e: Exception => str.toLowerCase
              }

            } else {
              str.toLowerCase
            }
          })
          content_lists.foreach(file_uri => {
            val uri_type = "content_list"
            objs += Online_bean(uid, publishtime_str, utimestr, newsid, newsmode, file_uri, uri_type)
          })
        }
        //       coverthumbnailimglists
        if (coverthumbnailimglists == null || coverthumbnailimglists.isEmpty) {
        } else {
          coverthumbnailimglists.toLowerCase().split(",").foreach(file_uri => {
            val uri_type = "coverthumbnailimglist"
            objs += Online_bean(uid, publishtime_str, utimestr, newsid, newsmode, file_uri.replaceFirst("/", ""), uri_type)
          })
        }


        if (videolists == null || videolists.isEmpty) {

        } else {
          videolists.toLowerCase().split(",").foreach(file_uri => {
            val uri_type = "videolist"
            objs += Online_bean(uid, publishtime_str, utimestr, newsid, newsmode, file_uri.replaceFirst("/", ""), uri_type)
          })
        }

        objs.iterator
      }).toDF().createOrReplaceTempView("online")

      spark.sql(s"insert overwrite table ${desTable} partition(date='${dt}' ) select * from online")



  }


}
