package com.haima.sage.bigdata.etl.store

import java.util.{Date, UUID}

import com.haima.sage.bigdata.etl.common.model.{AnalyzerType, AnalyzerWrapper, DataSourceWrapper, ModelingWrapper}
import com.haima.sage.bigdata.etl.utils.Mapper

/**
  * Created: 2016-05-16 18:30.
  * Author:zhhuiyan
  * Created: 2016-05-16 18:30.
  *
  *
  */
trait AnalyzerStore  extends DBStore[AnalyzerWrapper, String] with Mapper{
  override val TABLE_NAME = "ANALYZER"
  def queryByType(`type`: AnalyzerType.Type): List[AnalyzerWrapper]
}