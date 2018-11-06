package com.donews.main

import java.text.SimpleDateFormat

import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object Cdn_Online {
  val LOG: Logger = LoggerFactory.getLogger(getClass)
  def main(args: Array[String]): Unit = {

    //-----------------------------------------------------------------------------

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

    spark.sql("use logs")

    val domainMap: Map[String, String] = Map(
      "donewsdata.donews.com" -> "donewsdataoss",
      "niuerdata.g.com.cn" -> "donews-data",
      "donewsdataoss.g.com.cn" -> "donewsdataoss",
      "vdata.g.com.cn" -> "donews-data",
      "donewsdataoss.donews.com" -> "donewsdataoss",
      "niuerdata.donews.com" -> "donews-data",
      "www.cdn.g.com.cn" -> "donews-data",
      "wap.cdn.g.com.cn" -> "donews-data",
      "content.g.com.cn" -> "donews-data"
    )

    val domainBD = spark.sparkContext.broadcast[Map[String, String]](domainMap)
    spark.udf.register("gen_url", (url: String) => {
      var tmp = StringUtils.substringAfter(url.split("//")(1), "/").split("\\?")(0).toLowerCase
      val gen_url = if (tmp == null || tmp == "") "---" else tmp
      gen_url
    })
    spark.udf.register("gen_domain", (host: String) => {
      val domain = domainBD.value.getOrElse(host, "")
      domain
    })
    spark.udf.register("getDay", (time: String) => {
      if (time != null) {
        time.split(" ")(0)
      }else{
        "-"
      }
    })
    spark.udf.register("get_time", (time: String) => {
      if (time != null) {
        val smp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
        val date = smp.parse(time)
        date.getTime
      }else{
        0l
      }
    })


    val currentDay = args(0)

    import spark.sql

    /**
      * =======================================注册原始表===========================
      */

    sql(
      s"""
         SELECT host,referer_host,
         |gen_domain(host) as cdn_bucket,
         |file_day,
         |access_ip as ip,
         |hitrate,
         |httpcode,
         |user_agent,
         |method,
         |gen_url(access_url) as uri,
         |responsesize_bytes,
         |md5(concat(access_ip,user_agent)) as user,
         |file_path,
         |file_type
         |FROM logs.cdn_og
         |WHERE dt='${currentDay}'
         """.stripMargin).createOrReplaceTempView("cdn")


    /**
      *  线上数据
      */
    val online = spark.table("logs.online_og")
    val cdn = sql(s"select file_day,uri,sum(responsesize_bytes) as responsesize_bytes,cdn_bucket as bucket,ip as access_ip,file_path,hitrate,httpcode,user_agent,method,file_type from cdn  group by file_day,uri,cdn_bucket,ip,file_path,hitrate,httpcode,user_agent,method,file_type") //.rdd
    cdn.join(online, online("file_uri") === cdn("uri"), "left").createOrReplaceTempView("online_statistics")

    //将线上数据写入hive & 去cdn中uri
    sql(s"insert overwrite table logs.cdn_online partition(dt='${currentDay}' ) " +
      s"select bucket,access_ip,file_day,file_path,hitrate,httpcode,user_agent,responsesize_bytes,method,file_type,uri," +
      s"uid,publishtime_str,utimestr,newsid,newsmode,uri_type " +
      s"from online_statistics")


  }
}
