package com.haima.sage.bigdata.etl.utils

/**
  * Created by chengji on 17-12-27.
  */
object ExceptionUtils {
  def getMessage(t: Throwable): String ={
    if(t.getCause==null){
      t.getMessage
    }else{
      s"${t.getMessage}\n${getMessage(t.getCause)}"
    }
  }
}
