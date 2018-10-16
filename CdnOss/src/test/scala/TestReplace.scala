import org.apache.commons.codec.digest.DigestUtils

/**
  * Created by Shy on 2018/9/26
  */


object TestReplace extends App {
  //println(getUrl("[\"data/shareimg_oss/new_thumb/ylzx-fh-2/thumb_84837a5e0298bf30cdcf5f8c9e07fe0f.png\""))
  println(genUrl2("./big_media_img/2017/03/23/4f04088f-2574-11e7-8e6e-f45c89baa8c7.jpeg"))
  println(DigestUtils.md5Hex("./big_media_img/2017/03/23/4f04088f-2574-11e7-8e6e-f45c89baa8c7.jpeg"))

  println(url3("/big_media_img/2017/03/23/4f04088f-2574-11e7-8e6e-f45c89baa8c7.jpeg"))

  def getUrl(url: String): String = {
    //    var tmpUrl = url
    //    if (url.contains("\"")) {
    //      tmpUrl = url.replaceAll("\"", "")
    //      println("\":    " + tmpUrl)
    //    }
    //    if (url.contains("[")) {
    //      tmpUrl = url.replace("[", "")
    //      println("[:    " + tmpUrl)
    //    }
    //
    //    if (url.contains("]")) {
    //      tmpUrl = url.replace("]", "")
    //      println("]:    " + tmpUrl)
    //    }
    val tmpUrl = url match {
      case url if (url.contains("[\"") && url.contains("\"]")) => url.replace("[\"", "").replace("\"]", "")
    }

    tmpUrl
  }

  def genUrl2(url: String): String = {
    var tmpUrl = url
    if (url.startsWith("."))
      tmpUrl = url.substring(1)
    tmpUrl
  }

  def url3(url: String): String = {
    val urlSp = url.split("/")
    println(urlSp(1))
    println(urlSp(2))
    s"/${urlSp(1)}/${urlSp(2)}/"
  }
}
