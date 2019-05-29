package com.haima.sage.bigdata.etl.utils

import java.text.SimpleDateFormat

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.haima.sage.bigdata.etl.common.model.filter._


/**
  * Created by zhhuiyan on 2016/10/18.
  */
trait Mapper extends Serializable {

  final val mapper = {
    val _mapper = new ObjectMapper() with ScalaObjectMapper
    _mapper.setVisibility(PropertyAccessor.FIELD, Visibility.PUBLIC_ONLY)
    // mapper.registerModule(DefaultScalaModule).setSerializationInclusion(Include.NON_NULL)
    _mapper.registerModule(DefaultScalaModule).setSerializationInclusion(Include.NON_ABSENT)
    _mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)


    import com.fasterxml.jackson.databind.SerializationFeature

    _mapper.configure(SerializationFeature.INDENT_OUTPUT, false)
    _mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    _mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"))
    _mapper.getSerializerFactory.withSerializerModifier(new SageBeanSerializerModifier)

    val module = new SimpleModule()
    module.addDeserializer(classOf[ReAnalyzer], new ReAnalyzerDeserializer(classOf[ReAnalyzer]))
    module.addDeserializer(classOf[AnalyzerParser], new AnalyzerParserDeserializer(classOf[AnalyzerParser]))
    module.addDeserializer(classOf[ReParser], new ReParserDeserializer(classOf[ReParser]))
    _mapper.registerModule(module)
    _mapper
  }

}

