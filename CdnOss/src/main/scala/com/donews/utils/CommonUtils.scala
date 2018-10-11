package com.donews.utils

import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
  * Created by Shy on 2018/8/24
  */

object CommonUtils {

  def parseLocalDate(dateString: String): LocalDate = LocalDate.parse(dateString, DateTimeFormatter.ISO_LOCAL_DATE)

  def plusDays(date: String, days: Int): String = {
    val localDate = parseLocalDate(date)
    localDate.plusDays(days).format(DateTimeFormatter.ISO_LOCAL_DATE)
  }

  def plusDays(date:LocalDate ,days:Int): String = {
    date.plusDays(days).format(DateTimeFormatter.ISO_LOCAL_DATE)
  }
  def main(args: Array[String]): Unit = {
    println(plusDays("2018-10-08", -7))


  }
}
