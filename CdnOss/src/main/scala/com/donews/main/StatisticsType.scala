package com.donews.main

import com.donews.utils.CommonUtils
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

object StatisticsType {
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
      "niuerdata.donews.com" -> "donews-data")

    val domainBD = spark.sparkContext.broadcast[Map[String, String]](domainMap)
    spark.udf.register("gen_url", (url: String) => StringUtils.substringAfter(url.split("//")(1), "/").split("\\?")(0).toLowerCase)
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

    sql(
      s"""
         |SELECT hostname,
         |bucket as oss_bucket,
         |method,
         |key,
         |delta_datasize,
         |lower(key) as uri,
         |sync_request
         |FROM logs.oss_og
         |WHERE dt = '${currentDay}' and (method = 'PUT' or method = 'DELETE')
       """.stripMargin)
      //      .persist(StorageLevel.MEMORY_ONLY_SER)
      .createOrReplaceTempView("oss")

    /**
      * tab2 Statistics_Type 按分类聚合
      * 统计 CDN 维度用量,去重总量,回源量
      */
    val cdnRes2 = sql(
      s"""
         | SELECT '${currentDay}' as dt,a.cdn_bucket,a.file_type,a.file_path,
         | sum_cdn_size,mean_cdn_size,
         | url_cnts,url_dis_cnts,
         | sum_cdn_bake_size,url_bake_cnts
         | FROM
         | (SELECT cdn_bucket,file_type,file_path,
         | SUM(responsesize_bytes) AS sum_cdn_size,
         | MEAN(responsesize_bytes) AS mean_cdn_size,
         | COUNT(uri) AS url_cnts,
         | COUNT(DISTINCT uri) AS url_dis_cnts
         | FROM cdn
         | GROUP BY cdn_bucket,file_type,file_path) a
         | inner join
         | (SELECT cdn_bucket,file_type,file_path,SUM(responsesize_bytes) AS sum_cdn_bake_size,COUNT(DISTINCT uri) AS url_bake_cnts
         | FROM cdn
         | WHERE hitrate = 'MISS'
         | GROUP by cdn_bucket,file_type,file_path) b
         | on a.cdn_bucket = b.cdn_bucket and a.file_type = b.file_type and a.file_path = b.file_path
      """.stripMargin).withColumnRenamed("cdn_bucket", "bucket")


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


    val online_in_7 = sql(
      s"""
         |select bucket,file_type,file_path,
         |SUM(responsesize_bytes) AS sum_cdn_online_in7_size,
         |MEAN(responsesize_bytes) AS mean_cdn_online_in7_size,
         |COUNT(uri) AS url_online_in7_cnts,
         |COUNT(DISTINCT uri) AS url_online_in7_dis_cnts
         |from online_statistics
         |where publishtime_str is not null and publishtime_str >= date_sub('${currentDay}','7')
         |GROUP by bucket,file_type,file_path
      """.stripMargin)

    val online_out_7 = sql(
      s"""
         |select bucket,file_type,file_path,
         |SUM(responsesize_bytes) AS sum_cdn_online_out7_size,
         |MEAN(responsesize_bytes) AS mean_cdn_online_out7_size,
         |COUNT(uri) AS url_online_out7_cnts,
         |COUNT(DISTINCT uri) AS url_online_out7_dis_cnts
         |from online_statistics
         |where publishtime_str is not null and publishtime_str < date_sub('${currentDay}','7')
         |GROUP by bucket,file_type,file_path
      """.stripMargin)

    //
    val offline = sql(
      s"""
         |select bucket,file_type,file_path,
         |SUM(responsesize_bytes) AS sum_cdn_offline_size,
         |MEAN(responsesize_bytes) AS mean_cdn_offline_size,
         |COUNT(uri) AS url_offline_cnts,
         |COUNT(DISTINCT uri) AS url_offline_dis_cnts
         |from online_statistics
         |where publishtime_str is null
         |GROUP by bucket,file_type,file_path
      """.stripMargin)

    // OSS中数据需要从cdn中进行类型获取
    val ossTmp2 = sql(
      """
        | SELECT a.oss_bucket,a.method,a.Delta_DataSize,b.file_type,b.file_path
        | FROM oss a
        | inner join cdn b
        | on a.uri = b.uri
        | WHERE a.method = 'PUT'
      """.stripMargin)
    val pvtSum = ossTmp2.groupBy("oss_bucket", "file_type", "file_path").pivot("method").sum("Delta_DataSize").na.fill(0)
      .withColumnRenamed("PUT", "PUT_SIZE")
    val pvtCnt = ossTmp2.groupBy("oss_bucket", "file_type", "file_path").pivot("method").count().na.fill(0).withColumnRenamed("PUT", "PUT_CNT")
    val ossRes2 = pvtSum.join(pvtCnt, Seq("oss_bucket", "file_type", "file_path")).withColumnRenamed("oss_bucket", "bucket")

    //结果数据写mysql
    cdnRes2.join(online_in_7, Seq("bucket", "file_type", "file_path"), "inner")
      .join(online_out_7, Seq("bucket", "file_type", "file_path"), "inner")
      .join(offline, Seq("bucket", "file_type", "file_path"), "inner")
      .join(ossRes2, Seq("bucket", "file_type", "file_path"), "outer")
      .write.mode(SaveMode.Append).jdbc(dbUrl, "Statistics_Type", prop)


//    sql("select user_agent,count(1) as ua_cnt ,SUM(responsesize_bytes) as ua_sum from cdn group by user_agent").write.mode(SaveMode.Append).jdbc(dbUrl, s"Cdn_ua_cnt${currentDay.replaceAll("-","")}", prop)
//    sql("select referer_host,count(1) as refhost_cnt ,SUM(responsesize_bytes) as refhost_sum from cdn group by referer_host").write.mode(SaveMode.Append).jdbc(dbUrl, s"Cdn_referHost_cnt${currentDay.replaceAll("-", "")}", prop)
//
//    val ip_cnt = sql("select ip,count(1) as ua_cnt ,SUM(responsesize_bytes) as ua_sum from cdn group by ip")//.write.mode(SaveMode.Append).jdbc(dbUrl, s"Cdn_ip_cnt${currentDay.replaceAll("-", "")}", prop)
//
//    ip_cnt.rdd.coalesce(1).saveAsTextFile(s"/data/cdn_oss/output/ip_stat-${currentDay}")
  }
}
