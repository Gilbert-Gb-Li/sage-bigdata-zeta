package com.haima.sage.bigdata.etl.lexer.filter

import java.util.Date

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter.Script
import org.junit.Test

class ScriptTest {
  @Test
  def replace(): Unit ={
    val rule=Script("""var fans_num=event.get("fans_num");
                      |
                      |    if(fans_num ){
                      |        if(fans_num.indexOf("百万")!=-1){
                      |           var fans=   fans_num.replace(/\s*(粉丝)?\s*(\d+\.?\d*?)\s*百万\s*(粉丝)?\s*/g,'$2')*1000000;
                      |            event.put("fans_num",fans);
                      |        } else if(fans_num.indexOf("万")!=-1){
                      |         var fans=    fans_num.replace(/\s*(粉丝)?\s*(\d+)\s*万\s*(粉丝)?\s*/g,'$2')*10000;
                      |            event.put("fans_num",fans);
                      |        }else{
                      |         var fans=   fans_num.replace(/\s*(粉丝)?\s*(\d+\.?\d*)\s*(粉丝)?\s*/g,'$2');
                      |
                      |
                      |           event.put("fans_num",fans);
                      |        }
                      |
                      |    }
                      |    var follow_num=event.get("follow_num");
                      |
                      |    if(follow_num ){
                      |        if(follow_num.indexOf("百万")!=-1){
                      |           var fans=   follow_num.replace(/\s*(关注)?\s*(\d+\.?\d*?)\s*百万\s*(关注)?\s*/g,'$2')*1000000;
                      |            event.put("follow_num",fans);
                      |        } else if(follow_num.indexOf("万")!=-1){
                      |         var fans=    follow_num.replace(/\s*(关注)?\s*(\d+)\s*万\s*(关注)?\s*/g,'$2')*10000;
                      |            event.put("follow_num",fans);
                      |        }else{
                      |         var fans=   follow_num.replace(/\s*(关注)?\s*(\d+\.?\d*?)\s*(关注)?\s*/g,'$2');
                      |           event.put("follow_num",fans);
                      |        }
                      |
                      |    }""".stripMargin)

    val process=new ScriptProcessor(rule)
    val list=List(RichMap(Map("fans_num"->"9241 粉丝","follow_num"->"0 关注")),
      RichMap(Map("fans_num"->"9241 粉丝","follow_num"->"3"))
    )

    list.map(t=>process.process(t)).foreach(println)
  }


  @Test
  def date(): Unit ={
    println(new Date(1535079631616l))
  }
}
