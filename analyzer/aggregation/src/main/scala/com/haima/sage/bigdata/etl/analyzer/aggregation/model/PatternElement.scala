package com.haima.sage.bigdata.analyzer.aggregation.model

import java.io.Serializable

/**
  * Created by wxn on 2017/10/31.
  *
  */
trait PatternElement extends Serializable {

}

case class PatternParameter(param: String = "<PARAM>") extends PatternElement {


  override def toString: String = param
}

case class PatternText(text: String) extends PatternElement {

  override def toString: String = text
}
