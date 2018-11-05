package com.donews.main

import com.alibaba.fastjson.JSON
import com.donews.Online_bean
import com.donews.main.CDN_Process.getClass
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object Online_Process {

  def main(args: Array[String]): Unit = {

    val desTable = args(0)
    val dateArr = args(1).split(",")

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

    //yinli_201705.json.gz
    dateArr.foreach(date => {
      val filePath = s"/data/cdn_oss/online/*${date}*"
      val online = spark.sparkContext.textFile(filePath)
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
        if (content_list == null || content_list.isEmpty ) {
          objs += Online_bean(uid, publishtime_str, utimestr, newsid, newsmode, "", "content_list", "")

        }else{
          val content_lists = content_list.replaceAll("\\[", "").replaceAll("]", "").split(",").map(str => {
            if (str.contains("http")) {
              try{
                val host = str.split("/")(2)
                str.split(host)(1)
              }catch {
                case e:Exception => str
              }

            } else {
              str
            }
          })
          content_lists.foreach(file_uri => {
            if (!file_uri.isEmpty){
              val uri_type = "content_list"
              var uri = ""
              try{
                 uri = file_uri.replaceAll("\"", "").split("\\?")(0)
              }catch {
                case e:Exception => uri = file_uri
              }

              var fileUri_og = uri

              if (uri.startsWith("/")) {
                uri = uri.replaceFirst("/", "")
                if (uri.startsWith("/")) {
                  fileUri_og = uri
                  uri = uri.replaceFirst("/", "")
                }
              }

              objs += Online_bean(uid, publishtime_str, utimestr, newsid, newsmode, uri.toLowerCase(), uri_type, fileUri_og)
            }
          })
        }
        //       coverthumbnailimglists
        if (coverthumbnailimglists == null || coverthumbnailimglists.isEmpty ) {
          objs += Online_bean(uid, publishtime_str, utimestr, newsid, newsmode, "", "coverthumbnailimglists", "")
        }else{
          coverthumbnailimglists.replaceAll("\\[", "").replaceAll("]", "").split(",").foreach(file_uri =>{

            val uri_type = "coverthumbnailimglist"
            var uri = file_uri.replaceAll("\"", "").split("\\?")(0)
            if(uri.contains("http")){
              try {
                val host = uri.split("/")(2)
                uri = uri.split(host)(1)
              } catch {
                case e: Exception => uri
              }
            }


            var fileUri_og = uri

            if (uri.startsWith("/")) {
              uri = uri.replaceFirst("/", "")
              if (uri.startsWith("/")) {
                fileUri_og = uri
                uri = uri.replaceFirst("/", "")
              }
            }

            //else {
//              fileUri_og = "/" + uri
//            }

            objs += Online_bean(uid, publishtime_str, utimestr, newsid, newsmode, uri.toLowerCase(), uri_type,fileUri_og)
          })
        }



        if (videolists == null || videolists.isEmpty ) {
          objs += Online_bean(uid, publishtime_str, utimestr, newsid, newsmode, "", "videolists", "")

        }else{
          videolists.replaceAll("\\[", "").replaceAll("]", "").split(",").foreach(file_uri => {
            val uri_type = "videolist"
            var uri = file_uri.replaceAll("\"", "").split("\\?")(0)
            var fileUri_og = uri

            if (uri.startsWith("/")) {
              uri = uri.replaceFirst("/", "")
              if (uri.startsWith("/")) {
                fileUri_og = uri
                uri = uri.replaceFirst("/", "")
              }
            }

            objs += Online_bean(uid, publishtime_str, utimestr, newsid, newsmode, uri.toLowerCase() , uri_type,fileUri_og)
          })
        }

        objs.iterator
      }).toDF().createOrReplaceTempView("online")

      spark.sql(s"insert overwrite table ${desTable} partition(date='${date}' ) select uid,publishtime_str,utimestr,newsid,newsmode,file_uri,uri_type,file_uri_og from online")

    })


    //sqlContext.sql("select uid,publishtime_str,utimestr,videolists,newsid,coverthumbnailimglists,newsmode,getPath(content_list)  from online").printSchema()


  }

}
