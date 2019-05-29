package com.haima.sage.bigdata.etl.utils

import scala.util.Random

/**
 * Created by zhhuiyan on 15/6/18.
 */
object Guess extends App{

  val data= (0 to 100000000).map{
    i=>
      Random.nextInt(10)
  }.toArray

  guess(data,11)


  def guess(data:Array[Int],time:Int ): Unit ={
    var i=0 ;
    var even=0 ;
    var odd=0 ;
    var big=0 ;
    var small=0 ;
    var evens:Map[Int,Int]=Map()
    var odds:Map[Int,Int]=Map()
    var bigs:Map[Int,Int]=Map()
    var smalls:Map[Int,Int]=Map()
    while (i<data.length){
     if(data(i)%2 ==0){
       even+=1
       if(odd>=5){
         odds+=(i-odd->odd)
       }
       odd=0
     } else{
       odd+=1
       if(even>=time){
         evens+=(i-even->even)
       }
       even=0
     }
      if(data(i)>=5){
        big+=1
        if(small>=time){
          smalls+=(i-small->small)
        }
        small=0
      } else{
      small+=1
        if(big>=time){
          bigs+=(i-big->big)
        }
        big=0
      }
        i+=1
    }
    println(s"evens:{size:${evens.size},max:${evens.values.max}")
    println(s"odds:{size:${odds.size},max:${odds.values.max}")
    println(s"bigs:{size:${bigs.size},max:${bigs.values.max}")
    println(s"smalls:{size:${smalls.size},max:${smalls.values.max}")
  }
}
