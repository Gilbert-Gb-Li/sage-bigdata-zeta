package com.haima.sage.bigdata.etl.utils

import java.util.concurrent.{Executors, ThreadPoolExecutor}

import org.junit.Test

class UUIDUtilsTest {

  @Test
 def  uuid(){
    println(UUIDUtils.id("asdajsdlajd;lasdk;asldjawlkdhalkshdakshdakshd;kashdakshda;ksdhaskdaskdk"))
  }
  @Test
  def  threadUUID(){
    val start=System.currentTimeMillis()
    (0 to 1000).map(_=>{
      val t1=new Thread(){
        override def run(): Unit = {
          (0 to 10000).foreach(i=>{
            UUIDUtils.id(s"asdajsdlajd;lasdk;asldjawlkdhalkshdakshdakshd;kashdakshda;ksdhaskdaskdk$i")

          })
         // println(Thread.currentThread().getName+":finished")
        }
      }

      t1.start()
      t1
    }).foreach(_.join())
println(s"${1000*10000} id make take time ${System.currentTimeMillis()-start} ms")
  }

}

