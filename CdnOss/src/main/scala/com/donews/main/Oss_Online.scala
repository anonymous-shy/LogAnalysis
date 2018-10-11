package com.donews.main

import com.donews.main.Cdn_Online.getClass
import com.donews.utils.CommonUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object Oss_Online {
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val currDate = args(0)
    val day = CommonUtils.plusDays(currDate,-7)

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
    spark.udf.register("gen_url", (file_uri: String) => {
      var uri = file_uri
      if (file_uri.startsWith("/")) {
        uri = file_uri.replaceFirst("/", "")
      }
      uri
    })

    spark.udf.register("onlineSign",(str:String) =>{
      if(str != null){
        1
      }else
        0
    })

    spark.udf.register("getType", (key: String) => {
      val fileType = key.split("\\.")
      if (fileType.length >1){
        fileType(1)
      }else{
        "none"
      }
    })

    import spark.sql

    val oss = sql(s"select key,getType(key) as file_type from logs.oss_og where dt='$day'")
    val online = sql("select gen_url(file_uri) as file_uri,uri_type from logs.online_og")
    val online_oss = oss.join(online, oss("key") === online("file_uri"), "left").where("file_uri is null")

    online_oss.createOrReplaceTempView("online_oss")
    sql("select concat('/',key) as saved_img_url,max(onlineSign(file_uri)) as onlineSign from online_oss where file_type != 'mp4' group by key").write.json(s"/data/cdn_oss/output/oss_online/oss_online_img_$currDate")
    sql("select concat('/',key) as saved_img_url,max(onlineSign(file_uri)) as onlineSign from online_oss where file_type = 'mp4' group by key").write.json(s"/data/cdn_oss/output/oss_online/oss_online_video_$currDate")

  }
}
