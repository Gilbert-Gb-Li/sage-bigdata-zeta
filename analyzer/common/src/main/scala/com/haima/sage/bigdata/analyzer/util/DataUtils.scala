package com.haima.sage.bigdata.analyzer.util

import org.apache.flink.api.scala._

/**
  * Created by lenovo on 2017/11/14.
  */
object DataUtils {

  /**
    * 补值的数据处理算法，现在实现空缺补0和补均值，后续可以根据需要增加补median（中值），Mode(众数）等
    * @param ds 源数据，元数据需要对数据向量化后的
    * @param compensationValue 补值类型
    * @return 结果数据
    */
  def DataPreprocessing(ds:DataSet[Map[String, Double]], compensationValue:String = "zero"):DataSet[Map[String, Double]]={
    val keySet = ds.map(map=>("key",map.keySet))
      .reduce((t1, t2)=>{
      (t1._1,t1._2++t2._2)
    }).map(_._2).collect()(0)
    val s: DataSet[Map[String, Double]] = compensationValue match {
      case "zero"=>{
        val result: DataSet[Map[String, Double]] = ds.map(map=>{

          var m = map
          keySet.foreach(key=>{
            if(map.get(key).isEmpty){
                m += (key->0)
            }
          })
          m
        })
        println("result:"+result)
        result
      }
      case "avg"=>{
        val dsList = ds.map(map=>{
          val list = map.toList.map(t=>{
            (t._1,t._2,1)
          })
          list
        })
        val dsKv: DataSet[(String, Double, Int)] = dsList.flatMap(list=>list)
        val keyAvg: Map[String, Double] = dsKv.groupBy(0).reduce((t1, t2)=>{
          (t1._1,t1._2+t2._2,t1._3+t2._3)
        }).map(t=>{
          (t._1,t._2/t._3.toDouble)
        }).collect().toMap
        val result = ds.map(map=>{
          var m=map
          keyAvg.foreach(avg =>{
            if(m.get(avg._1)==None){
              m += avg
            }
          })
          m
        })
        result
      }
    }
    s
  }
}
