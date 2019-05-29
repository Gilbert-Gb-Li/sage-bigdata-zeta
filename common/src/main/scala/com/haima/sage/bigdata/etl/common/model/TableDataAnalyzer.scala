package com.haima.sage.bigdata.etl.common.model

/**
  * Created by zhhuiyan on 2017/4/25.
  */
trait TableDataAnalyzer[E, T, R] {

  def getEnvironment(data: T): E

  def register(data: T, first: Table)(implicit environment: E): R

  @throws[Exception]
  def execute(environment: E, sql: Option[String] = None, fieldsDic: Option[Map[String, String]] = None): List[T] = {
    filter(analyze(environment, sql, fieldsDic))
  }

  def filter(data: T): List[T]

  def analyze(environment: E, sql: Option[String] = None, fieldsDic: Option[Map[String, String]] = None): T
}
