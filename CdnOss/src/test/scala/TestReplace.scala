
/**
  * Created by Shy on 2018/9/26
  */


object TestReplace extends App {
  println(getUrl("[\"data/shareimg_oss/new_thumb/ylzx-fh-2/thumb_84837a5e0298bf30cdcf5f8c9e07fe0f.png\""))


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
      case url if (url.contains("[\"") && url.contains("\"]")) => url.replace("[\"","").replace("\"]","")
    }

    tmpUrl
  }
}
