package com.haima.sage.bigdata.etl.knowledge

import com.haima.sage.bigdata.etl.common.model.DataSource

trait KnowledgeLoader[D <: DataSource, T] {

  def source: D

  /**
    *
    * @return 数据
    */
  def load(): Iterable[T]

  /**
    * 分页数据大小
    *
    * @return (数据,是否还有)
    */
  def byPage(size: Int): (Iterable[T], Boolean)
}