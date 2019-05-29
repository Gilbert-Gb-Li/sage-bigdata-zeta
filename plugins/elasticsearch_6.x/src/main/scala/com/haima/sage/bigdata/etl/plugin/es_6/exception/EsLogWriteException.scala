package com.haima.sage.bigdata.etl.plugin.es_6.exception

import com.haima.sage.bigdata.etl.common.exception.LogWriteException
import org.elasticsearch.action.bulk.BulkRequestBuilder

/**
  * Created by zhhuiyan on 2017/4/17.
  */
case class EsLogWriteException(builder: BulkRequestBuilder, cause: Throwable) extends LogWriteException("es.connect.error", cause)