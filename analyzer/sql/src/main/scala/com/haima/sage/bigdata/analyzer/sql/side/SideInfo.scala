package com.haima.sage.bigdata.analyzer.sql.side

import org.apache.calcite.sql.JoinType

case class SideInfo(
                     /**
                       * Join value index
                       */
                     equalValIndex: List[Int],

                     /**
                       * Join field name List
                       */
                     equalFieldList: List[String],


                     /**
                       * Join field 与原始字段对照表，key为JoinField，value为原始字段名
                       */
                     equalFieldRawMap: Map[String,String],

                     /**
                       * Join后的字段index与维表字段index的对应信息
                       */
                     sideFieldIndex: Map[Int, Int],

                     /**
                       * Join后的字段index与Left表Index对应信息
                       */
                     inFieldIndex: Map[Int, Int],

                     /**
                       *
                       */
                     sideFieldNameIndex: Map[Int, String],

                     /**
                       *
                       */
                     fields: List[String],

                     /**
                       * join info
                       */
                     joinType: JoinType
                   )
