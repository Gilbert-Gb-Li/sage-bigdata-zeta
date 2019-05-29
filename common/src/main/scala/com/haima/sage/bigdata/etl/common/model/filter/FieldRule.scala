package com.haima.sage.bigdata.etl.common.model.filter

import com.haima.sage.bigdata.etl.common.model.RichMap

trait FieldRule extends WithGet {

  def field: String

  override def get(event: RichMap): Option[Any] = {
    event.get(field)
  }
}