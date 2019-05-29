package com.haima.sage.bigdata.etl.common.model.filter

import com.haima.sage.bigdata.etl.common.model.RichMap

trait WithGet {
  def get(event: RichMap): Option[Any]
}
