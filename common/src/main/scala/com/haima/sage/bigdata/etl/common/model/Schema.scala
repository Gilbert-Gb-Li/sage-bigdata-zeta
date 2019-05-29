package com.haima.sage.bigdata.etl.common.model

/**
  * Derby数据库支持的数据库类型
  */
object FieldType extends Message {
  type FieldType = Value
  val BOOLEAN, CLOB, TIMESTAMP, DOUBLE = Value
}

case class DECIMAL(length: Long = 16, decimals: Long = 2) extends FieldType.FieldType {
  override def id: Int = 5
}

case class NUMERIC(length: Long = 16, decimals: Long = 0) extends FieldType.FieldType {
  override def id: Int = 6
}

case class VARCHAR(length: Long = 200) extends FieldType.FieldType {
  override def id: Int = 7
}

/**
  * fieldName String 字段名称
  * fieldType String 字段类型
  * PK Option[String] 是否是主键  None：否  Some("Y") :是
  * default Option[Any] 默认值  None：不配置默认值， Some(_):_ 默认值
  * allowNull Option[String] 是否允许为空 None：是  Some("N") : 否
  * length Option[Long] 长度 None :默认不配置 已库为标准 ，字符类型默认是250
  * decimals Option[Long] 精度
  */

case class Field(name: String,
                 `type`: FieldType.FieldType,
                 default: Option[Any] = None,
                 pk: Option[String] = None,
                 nullable: Option[String] = None)


case class Schema(name: String, fields: Array[Field])