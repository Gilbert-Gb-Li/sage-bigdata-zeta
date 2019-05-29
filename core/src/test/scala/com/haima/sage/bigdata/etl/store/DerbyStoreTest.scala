package com.haima.sage.bigdata.etl.store

import org.junit.Test

class DerbyStoreTest {

  @Test
  def read(): Unit ={
    val store=new DBParserStore()
    store.init

    (1 to 10).foreach(i=>{
     val t= new  Thread(){
       override def run(): Unit = {
         store.all().foreach(t=>
         {
           Thread.sleep(100)
           println(t)

         })
       }
     }
      t.start()
      t.join()
    })




  }

}
