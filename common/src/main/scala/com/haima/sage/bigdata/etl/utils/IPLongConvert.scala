package com.haima.sage.bigdata.etl.utils

/**
  * Created by zhhuiyan on 15/12/29.
  */
object IPLongConvert {
  /**
    * IP转数字
    *
    * @param ip
    * @return
    */
  def to(ip: String): Long = {
    val old_parts=ip.split("\\.", 4)
    if(old_parts.length!=4 || old_parts.exists(!_.matches("\\d+"))){
      -1l
    }else{
      val parts =old_parts
        .map(_.toLong)
      (parts(0) << 24) + (parts(1) << 16) + (parts(2) << 8) + parts(3)
    }

  }

  /**
    * 数字转IP
    *
    * @param ip
    * @return
    */
  def from(ip: Long): String = {

    s"""${ip >>> 24}.${(ip & 0x00FFFFFF) >>> 16}.${(ip & 0x0000FFFF) >>> 8}.${ip & 0x000000FF}"""

  }


}
