package com.haima.sage.bigdata.analyzer.streaming.flink.utils

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.commons.io.FileUtils

import scala.util.Random
/**
  * Created by ChengJi on 2017/11/06.
  */
object StreamDataGenerator {
  def main(args: Array[String]): Unit = {
    mockupStreamData()
//    mockupStreamOrdernessData()
  }
  def mockupStreamData(): Unit ={
    val file = new File("/Users/chengji/test/stream.txt")
    val ONYDAY = 24*60*60*1000
    if(!file.exists()){
      file.createNewFile()
    }
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z")
    var i = 0
    while (true){
      TimeUnit.MILLISECONDS.sleep(2000)
      if(i%3==0){
        FileUtils.writeStringToFile(file,s"${format.format(new Date(System.currentTimeMillis()-ONYDAY))},你好世界,中华人民共和国\r\n",true)
      }else if(i%2==0){
        FileUtils.writeStringToFile(file,s"${format.format(new Date(System.currentTimeMillis()-ONYDAY))},你好,中华人民共和国\r\n",true)
      }else{
        FileUtils.writeStringToFile(file,s"${format.format(new Date(System.currentTimeMillis()-ONYDAY))},wo men de Chengji shi ge dashuaibi,中华人民共和国\r\n",true)
      }
      i += 1
    }
//    println(System.getProperty("os.name"))
  }

  def mockupStreamOrdernessData(): Unit ={
    val file = new File("d:/test/stream.txt")
    if(!file.exists()){
      file.createNewFile()
    }
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z")
    var i = 0
    while (true){
      TimeUnit.MILLISECONDS.sleep(2000)
      if(i%3==0){
        FileUtils.writeStringToFile(file,s"${format.format(new Date(System.currentTimeMillis()+Random.nextInt(10000)))},你好世界,中华人民共和国\r\n",true)
      }else if(i%2==0){
        FileUtils.writeStringToFile(file,s"${format.format(new Date(System.currentTimeMillis()+Random.nextInt(10000)))},你好,中华人民共和国\r\n",true)
      }else{
        FileUtils.writeStringToFile(file,s"${format.format(new Date(System.currentTimeMillis()+Random.nextInt(10000)))},wo men de Chengji shi ge dashuaibi,中华人民共和国\r\n",true)
      }
      i += 1
    }
  }

}