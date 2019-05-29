package com.haima.sage.bigdata.etl.store.resolver

import com.haima.sage.bigdata.etl.common.model.ParserWrapper
import com.haima.sage.bigdata.etl.store.BaseStore

/**
  * Created: 2016-05-16 18:30.
  * Author:zhhuiyan
  * Created: 2016-05-16 18:30.
  *
  *
  */
trait ParserStore extends AutoCloseable with BaseStore[ParserWrapper, String] {

  def bySub(id: String): List[ParserWrapper]
  def byKnowledge(knowledgeId: String): List[ParserWrapper]
}