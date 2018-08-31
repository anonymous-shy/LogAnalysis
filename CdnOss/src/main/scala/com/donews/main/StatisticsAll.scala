package com.donews.main

import com.donews.utils.CommonUtils.plusDays
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by Shy on 2018/8/30
  */

object StatisticsAll {
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val today = args(0) // 取执行当天日期,处理的文件日期为today-1
    //判断日期取 当日,3日,7日 日期
    val b1D = (plusDays(today, -1), "currentDay")
    val b3D = (plusDays(today, -3), "before3Days")
    val b7D = (plusDays(today, -7), "before7Days")

    val warehouseLocation = "hdfs://HdfsHA/data/user/hive/warehouse"
    //val warehouseLocation = "hdfs://HdfsHA/data/user/spark/warehouse"

    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()


    process(spark, today, b1D)
    process(spark, today, b3D)
    process(spark, today, b7D)
  }

  def process(spark: SparkSession, today: String, dayType: (String, String)): Unit = {
    import spark.sql
    spark.sql("use logs")
    val currentDay = plusDays(today, -1)
    val resConf = ConfigFactory.load()
    val prop = new java.util.Properties()
    prop.put("driver", resConf.getString("db.default.driver"))
    prop.put("user", resConf.getString("db.default.user"))
    prop.put("password", resConf.getString("db.default.password"))
    val dbUrl = resConf.getString("db.default.url")

    // cdn_online table
    sql(
      s"""
         |SELECT
         |bucket as cdn_bucket,
         |access_ip as ip,
         |hitrate,
         |file_uri as uri,
         |responsesize_bytes,
         |user_agent as ua,
         |md5(concat(access_ip,user_agent)) as user,
         |file_path,
         |file_type
         |FROM logs.cdn_online
         |WHERE dt = '$currentDay'
         """.stripMargin)
      .createOrReplaceTempView("cdn")
    // oss table
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
         |WHERE dt >= '${dayType._1}' and dt < '$today' and (method = 'PUT' or method = 'DELETE')
       """.stripMargin)
      .createOrReplaceTempView("oss")

    //// ========================================================================================================== ////
    LOG.info(s">>>>> $currentDay : 统计Statistics_All")
    // tab1 : Statistics_All 根据域名 聚合总量
    // 统计 CDN 维度用量,去重总量,回源量
    val cdnRes1 = sql(
      """
        | SELECT a.cdn_bucket,sum_cdn_size,url_cnts,sum_cdn_bake_size,url_bake_cnts
        | FROM
        | (SELECT cdn_bucket,SUM(responsesize_bytes) AS sum_cdn_size,COUNT(DISTINCT uri) AS url_cnts
        | FROM cdn
        | GROUP BY cdn_bucket) a
        | inner join
        | (SELECT cdn_bucket,SUM(responsesize_bytes) AS sum_cdn_bake_size,COUNT(DISTINCT uri) AS url_bake_cnts
        | FROM cdn
        | WHERE hitrate = 'MISS'
        | GROUP by cdn_bucket) b
        | on a.cdn_bucket = b.cdn_bucket
      """.stripMargin)
    // 统计OSS 取 PUT,DELETE 作为增量与删除统计
    val ossTmp1 = sql("SELECT oss_bucket,method,Delta_DataSize FROM oss")
    // 行转列得到oss结果表
    val pvt1sum = ossTmp1.groupBy("oss_bucket").pivot("method").sum("Delta_DataSize").na.fill(0)
      .withColumnRenamed("PUT", "PUT_SIZE")
      .withColumnRenamed("DELETE", "DELETE_SIZE")
    val pvt1Res = pvt1sum.withColumn("All_SIZE", pvt1sum("PUT_SIZE") + pvt1sum("DELETE_SIZE"))
    val pvt1cnt = ossTmp1.groupBy("oss_bucket").pivot("method").count().na.fill(0)
      .withColumnRenamed("PUT", "PUT_CNT")
      .withColumnRenamed("DELETE", "DELETE_CNT")

    //// 判断 cdn 中有 oss数据 ////
    val ossCdn = sql(
      s"""
         | SELECT a.oss_bucket,a.method,a.Delta_DataSize,
         | b.hitrate,b.ip,b.uri,b.responsesize_bytes,b.ua,b.user
         | FROM cdn b
         | INNER JOIN oss a
         | ON a.oss_bucket = b.cdn_bucket and a.uri = b.uri
         | WHERE a.method = 'PUT'
      """.stripMargin).persist(StorageLevel.MEMORY_ONLY_SER)

    // 回源数据 统计cdn中数据
    val ossCdnsum = ossCdn.where("hitrate = 'MISS'").groupBy("oss_bucket").pivot("method").sum("responsesize_bytes").na.fill(0)
      .withColumnRenamed("PUT", "OSSCDN_BAKE_PUT_SIZE")
    val ossCdncnt = ossCdn.where("hitrate = 'MISS'").groupBy("oss_bucket").pivot("method").count().na.fill(0)
      .withColumnRenamed("PUT", "OSSCDN_BAKE_PUT_CNT")
    // 全量 统计cdn中数据
    val ossCdnsumAll = ossCdn.groupBy("oss_bucket").pivot("method").sum("responsesize_bytes").na.fill(0)
      .withColumnRenamed("PUT", "OSSCDN_PUT_SIZE")
    val ossCdncntAll = ossCdn.groupBy("oss_bucket").pivot("method").count().na.fill(0)
      .withColumnRenamed("PUT", "OSSCDN_PUT_CNT")

    val ossRes1 = pvt1Res.join(pvt1cnt, "oss_bucket").join(ossCdnsum, "oss_bucket").join(ossCdncnt, "oss_bucket")
      .join(ossCdnsumAll, "oss_bucket").join(ossCdncntAll, "oss_bucket")

    // tab1 result
    cdnRes1.join(ossRes1, cdnRes1("cdn_bucket") === ossRes1("oss_bucket"), "outer")
      .createOrReplaceTempView("tab1")
    sql(
      s"""
         |SELECT '$currentDay' as dt,
         |cdn_bucket,sum_cdn_size,sum_cdn_bake_size,url_cnts,url_bake_cnts,
         |oss_bucket,PUT_SIZE,PUT_CNT,DELETE_SIZE,DELETE_CNT,All_SIZE,
         |OSSCDN_PUT_SIZE,OSSCDN_PUT_CNT,OSSCDN_BAKE_PUT_SIZE,OSSCDN_BAKE_PUT_CNT,
         |'${dayType._2}' as day_type
         |FROM tab1
        """.stripMargin)
    //      .write.mode(SaveMode.Append).jdbc(dbUrl, "Statistics_All", prop) // 重刷数据

    // 子表
    ossCdn.withColumnRenamed("oss_bucket", "bucket").createOrReplaceTempView("OssCdn")
    // 中间表存入hive
    //    sql(s"insert overwrite table logs.OssCdn partition(dt='$currentDay', day_type = '${dayType._2}') select * from OssCdn")
    sql(
      s"""
         |insert overwrite table logs.OssCdn partition(dt='$currentDay', day_type = '${dayType._2}')
         |select
         |  bucket,method,delta_datasize,hitrate,ip,uri,responsesize_bytes,ua,user
         |from OssCdn
       """.stripMargin)
    // 下拉子表
    val joinRes = sql(
      s"""
         |SELECT a.bucket,sum_cdn_size,cnd_cnts,sum_cdn_bake_size,cnd_bake_cnts
         | FROM
         | (SELECT bucket,SUM(responsesize_bytes) AS sum_cdn_size,COUNT(1) AS cnd_cnts
         | FROM OssCdn
         | GROUP BY bucket) a
         | inner join
         | (SELECT bucket,SUM(responsesize_bytes) AS sum_cdn_bake_size,COUNT(1) AS cnd_bake_cnts
         | FROM OssCdn
         | WHERE hitrate = 'MISS'
         | GROUP by bucket) b
         | on a.bucket = b.bucket
       """.stripMargin).persist(StorageLevel.MEMORY_ONLY_SER)
    // 1.全量
    // 1.ip
    sql(
      s"""
         | SELECT '$currentDay' as dt,bucket,ip,ip_sum_size,ip_cnt,'${dayType._2}' as day_type,'N' as BAKE
         | FROM
         |  (SELECT bucket,ip,ip_sum_size,ip_cnt,
         |    row_number() OVER (PARTITION BY bucket ORDER BY ip_sum_size DESC) AS rank
         |   FROM (SELECT bucket,ip,sum(responsesize_bytes) as ip_sum_size,count(1) as ip_cnt FROM OssCdn GROUP BY bucket,ip) t) tt
         | WHERE tt.rank <= 100
       """.stripMargin)
      .join(joinRes, "bucket")
      .write.mode(SaveMode.Append).jdbc(dbUrl, "Statistics_ip", prop)

    sql(
      s"""
         | SELECT '$currentDay' as dt,bucket,ua,ua_sum_size,ua_cnt,'${dayType._2}' as day_type,'N' as BAKE
         | FROM
         |  (SELECT bucket,ua,ua_sum_size,ua_cnt,
         |    row_number() OVER (PARTITION BY bucket ORDER BY ua_sum_size DESC) AS rank
         |   FROM (SELECT bucket,ua,sum(responsesize_bytes) as ua_sum_size,count(1) as ua_cnt FROM OssCdn GROUP BY bucket,ua) t) tt
         | WHERE tt.rank <= 100
       """.stripMargin)
      .join(joinRes, "bucket")
      .write.mode(SaveMode.Append).jdbc(dbUrl, "Statistics_ua", prop)

    sql(
      s"""
         | SELECT '$currentDay' as dt,bucket,user,user_sum_size,user_cnt,'${dayType._2}' as day_type,'N' as BAKE
         | FROM
         |  (SELECT bucket,user,user_sum_size,user_cnt,
         |    row_number() OVER (PARTITION BY bucket ORDER BY user_sum_size DESC) AS rank
         |   FROM (SELECT bucket,user,sum(responsesize_bytes) as user_sum_size,count(1) as user_cnt FROM OssCdn GROUP BY bucket,user) t) tt
         | WHERE tt.rank <= 100
       """.stripMargin)
      .join(joinRes, "bucket")
      .write.mode(SaveMode.Append).jdbc(dbUrl, "Statistics_user", prop)
    // 2.回源
    // 1.ip
    sql(
      s"""
         | SELECT '$currentDay' as dt,bucket,ip,ip_sum_size,ip_cnt,'${dayType._2}' as day_type,'Y' as BAKE
         | FROM
         |  (SELECT bucket,ip,ip_sum_size,ip_cnt,
         |    row_number() OVER (PARTITION BY bucket ORDER BY ip_sum_size DESC) AS rank
         |   FROM (SELECT bucket,ip,sum(responsesize_bytes) as ip_sum_size,count(1) as ip_cnt FROM OssCdn WHERE hitrate = 'MISS' GROUP BY bucket,ip) t) tt
         | WHERE tt.rank <= 100
       """.stripMargin)
      .join(joinRes, "bucket")
      .write.mode(SaveMode.Append).jdbc(dbUrl, "Statistics_ip", prop)

    sql(
      s"""
         | SELECT '$currentDay' as dt,bucket,ua,ua_sum_size,ua_cnt,'${dayType._2}' as day_type,'Y' as BAKE
         | FROM
         |  (SELECT bucket,ua,ua_sum_size,ua_cnt,
         |    row_number() OVER (PARTITION BY bucket ORDER BY ua_sum_size DESC) AS rank
         |   FROM (SELECT bucket,ua,sum(responsesize_bytes) as ua_sum_size,count(1) as ua_cnt FROM OssCdn WHERE hitrate = 'MISS' GROUP BY bucket,ua) t) tt
         | WHERE tt.rank <= 100
       """.stripMargin)
      .join(joinRes, "bucket")
      .write.mode(SaveMode.Append).jdbc(dbUrl, "Statistics_ua", prop)

    sql(
      s"""
         | SELECT '$currentDay' as dt,bucket,user,user_sum_size,user_cnt,'${dayType._2}' as day_type,'Y' as BAKE
         | FROM
         |  (SELECT bucket,user,user_sum_size,user_cnt,
         |    row_number() OVER (PARTITION BY bucket ORDER BY user_sum_size DESC) AS rank
         |   FROM (SELECT bucket,user,sum(responsesize_bytes) as user_sum_size,count(1) as user_cnt FROM OssCdn WHERE hitrate = 'MISS' GROUP BY bucket,user) t) tt
         | WHERE tt.rank <= 100
       """.stripMargin)
      .join(joinRes, "bucket")
      .write.mode(SaveMode.Append).jdbc(dbUrl, "Statistics_user", prop)
  }
}
