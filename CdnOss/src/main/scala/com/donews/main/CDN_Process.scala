package com.donews.main

import java.text.SimpleDateFormat
import java.util.Locale

import com.donews.CDN_bean
import com.donews.main.Cdn_Online.getClass
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._


object CDN_Process {

  val Log = LoggerFactory.getLogger(CDN_Process.getClass)
  def main(args: Array[String]): Unit = {

    val desTable = args(0)
    val dt = args(1)

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

    val fileArr = Array(
      s"/data/cdn_oss/cdn/$dt/donewsdataoss.g.com.cn_${dt}*",
      s"/data/cdn_oss/cdn/$dt/www.cdn.g.com.cn_${dt}*",
      s"/data/cdn_oss/cdn/$dt/wap.cdn.g.com.cn_${dt}*",
      s"/data/cdn_oss/cdn/$dt/niuerdata.g.com.cn_${dt}*",
      s"/data/cdn_oss/cdn/$dt/niuerdata.donews.com_${dt}*",
      s"/data/cdn_oss/cdn/$dt/donewsdataoss.g.com.cn_${dt}*",
      s"/data/cdn_oss/cdn/$dt/donewsdata.donews.com_${dt}*",
      s"/data/cdn_oss/cdn/$dt/content.g.com.cn_${dt}*",
      s"/data/cdn_oss/cdn/$dt/vdta.g.com.cn_${dt}*"
    )

    fileArr.foreach(file => {

      try{
        println("=================================================================================================")
        val bucket = StringUtils.substringAfterLast(file, "/").split("_")(0)
        println(s"===========================================$file======================================================")

        val cdn_rdd = spark.sparkContext.textFile(file)
        val cdn_bean = cdn_rdd.map(log => log_process(log)).filter(!_.access_url.equals("")).toDF().createOrReplaceTempView("logDF")

        println(s"===========================================写hive======================================================")


        spark.sql(s"insert overwrite table ${desTable} partition(dt='${dt.replaceAll("_", "-")}' , bucket ='${bucket}' ) select host,access_ip,file_day,access_url,file_path,hitrate,httpcode,user_agent,responsesize_bytes,method,file_type,file_uri,referer_host,log_date from logDF")

      }catch {
        case e:Exception => println(s"==================================没有这个文件 $file =================================")
      }



    })


  }

  def log_process(log:String): CDN_bean ={

    try {

      val map: java.util.Map[String, String] = logProcess(log)

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


      var referer_host:String = null
      if (referer.split("/").length >=2){
        referer_host = referer.split("/")(2)
      }


      val file_type = file_path.split("/")(0)
      val arr = access_url.split("/")

      var file_day = "-"
      if (arr.length ==10 ){
         file_day = arr(arr.length - 4) + "-" + arr(arr.length - 3) + "-" + arr(arr.length - 2)
      }
      val host = arr(2)


      val smp = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
      val smp2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
      val log_date = smp2.format(smp.parse(log_timestamp))

      val file_uri = access_url.toString.split(host + "/")(1)
      if (responsesize_bytes.equals("-")) {
        responsesize_bytes = "0"
      }
      CDN_bean(host, access_ip, file_day, access_url, file_path, hitrate, httpcode, user_agent, responsesize_bytes.toLong, method, file_type, file_uri, log_date, referer_host)

    }catch {

      case e :Exception =>
        Log.info(s"============================解析错误 ： ${log}===================================")
        println(s"==============================解析错误 ： ${log}==================================")
        CDN_bean("","","", "", "", "", "", "", 0, "", "", "", "","")
    }

      }



  def logProcess(log:String):java.util.Map[String,String] =  {
    val DATA = "(.*?)"

    val pattern = s"\\[${DATA}\\] ${DATA} ${DATA} ${DATA} #${DATA}# #${DATA} ${DATA}# ${DATA} ${DATA} ${DATA} ${DATA} #${DATA}# #${DATA}#".r
    var pattern(log_timestamp, access_ip,temp, responsetime, referer, method, access_url, httpcode, requestsize_bytes, responsesize_bytes, hitrate, user_agent, file_path) = log.replaceAll("\"", "#")

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

}
