package com.donews

case class CDN_bean(host: String, access_ip: String, file_day: String, access_url: String, file_path: String, hitrate: String, httpcode: String, user_agent: String, responsesize_bytes: Long, method: String, file_type: String, file_uri: String, log_date: String, referer_host: String)

case class OSS_bean(remote_ip: String, log_timestamp: String, method: String, access_url: String, http_status: String, sentbytes: Long, referer: String, useragent: String, hostname: String, bucket: String, key: String, objectsize: Long, delta_datasize: Long, sync_request: String)

case class Online_bean(uid: String, publishtime_str: String, utimestr: String, newsid: String, newsmode: String, file_uri: String, uri_type: String)

class bean {

}
