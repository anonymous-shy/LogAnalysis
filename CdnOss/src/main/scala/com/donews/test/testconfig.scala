package com.donews.test

import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._


object testconfig {

  def main(args: Array[String]): Unit = {
    val resConf = ConfigFactory.load()
    val prop = new java.util.Properties()
    prop.put("driver", resConf.getString("db.default.driver"))
    prop.put("user", resConf.getString("db.default.user"))
    prop.put("password", resConf.getString("db.default.password"))
    val dbUrl = resConf.getString("db.default.url")

    prop.foreach(println(_))
  }
}
