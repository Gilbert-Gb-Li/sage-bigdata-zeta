package com.haima.sage.bigdata.etl.utils

import com.haima.sage.bigdata.etl.common.base.Lexer
import com.haima.sage.bigdata.etl.common.model.DataAnalyzer
import com.haima.sage.bigdata.etl.filter.RuleProcessor
import com.haima.sage.bigdata.etl.lexer.DefaultLexer
import org.junit.Test

class ClassUtilsTest {

  @Test
  def list(): Unit ={
   // ClassUtils.subClass(classOf[RuleProcessor[_, _, _]], name = "com.haima.sage.bigdata.analyzer.modeling").foreach(println)
   // ClassUtils.subClass(classOf[RuleProcessor[_, _, _]]).foreach(println)
  //   ClassUtils.subClass(classOf[DataAnalyzer[_, _, _]],name = "com.haima.sage.bigdata.analyzer.modeling").foreach(println)
//    ClassUtils.subClass(classOf[DataAnalyzer[_, _, _]],name = "com.haima.sage.bigdata.analyzer.streaming").foreach(println)
    ClassUtils.subClass(classOf[Lexer[_,_]],params = 2).foreach(println)

  }

}
