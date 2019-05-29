package com.haima.sage.bigdata.analyzer.sql.utils


import com.dtstack.flink.sql.side.{FieldInfo, JoinInfo}
import com.haima.sage.bigdata.analyzer.sql.side.SideInfo
import com.haima.sage.bigdata.etl.common.model._
import org.apache.calcite.sql.{SqlBasicCall, SqlIdentifier, SqlKind, SqlNode}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.Types

import scala.collection.mutable


object TableUtils {
  /**
    *
    * @param table
    * @return (原字段名称, 大些字段名称, TypeInformation) eg (name, NAME, TypeInformation)
    */
  def toTypeInfo(table: Table): Array[(String, String, TypeInformation[_])] = {
    table.fields.map {
      case (key, value) =>
        (key, key.toUpperCase, value match {
          case "integer" =>
            Types.INT
          case "short" =>
            Types.SHORT
          case "long" =>
            Types.LONG
          case "float" =>
            Types.FLOAT
          case "double" =>
            Types.DOUBLE
          case "string" =>
            Types.STRING
          case "boolean" =>
            Types.BOOLEAN
          case "byte" =>
            Types.BYTE
          case "date" =>
            Types.SQL_DATE
          case "time" =>
            Types.SQL_TIME
          case "datetime" =>
            Types.SQL_TIMESTAMP
          case _ =>
            Types.STRING
        })
    }
  }

  def getSideInfo(joinInfo: JoinInfo, rowTypeInfo: RowTypeInfo, outFieldInfoList: List[FieldInfo]): SideInfo = {
    val conditionNode = joinInfo.getCondition
    val sqlNodeList = {
      if (conditionNode.getKind eq SqlKind.AND)
        conditionNode.asInstanceOf[SqlBasicCall].getOperands.toList
      else
        List(conditionNode)
    }

    val equalFieldList = mutable.ListBuffer[String]()
    val equalFieldRawMap = mutable.Map[String, String]()
    val equalValIndex = mutable.ListBuffer[Int]()

    def dealOneEqualCon(sqlNode: SqlNode): Unit = {
      if (sqlNode.getKind == SqlKind.EQUALS) {
        val sideTableName = joinInfo.getSideTableName
        val left = sqlNode.asInstanceOf[SqlBasicCall].getOperands.apply(0).asInstanceOf[SqlIdentifier]
        val right = sqlNode.asInstanceOf[SqlBasicCall].getOperands.apply(1).asInstanceOf[SqlIdentifier]

        val leftTableName = left.getComponent(0).getSimple
        val leftField = left.getComponent(1).getSimple

        val rightTableName = right.getComponent(0).getSimple
        val rightField = right.getComponent(1).getSimple

        if (leftTableName.equalsIgnoreCase(sideTableName)) {
          equalFieldList.append(leftField)
          var equalFieldIndex = -1
          var i = 0
          while ( {
            i < rowTypeInfo.getFieldNames.length
          }) {
            val fieldName = rowTypeInfo.getFieldNames.apply(i)
            if (fieldName.equalsIgnoreCase(rightField) || fieldName.toUpperCase.equalsIgnoreCase(rightField)) equalFieldIndex = i

            {
              i += 1
              i - 1
            }
          }
          if (equalFieldIndex == -1) throw new RuntimeException("can't find equal field " + rightField)

          equalValIndex.append(equalFieldIndex)
        } else if (rightTableName.equalsIgnoreCase(sideTableName)) {
          equalFieldList.append(rightField)
          var equalFieldIndex = -1
          var i = 0
          while ( {
            i < rowTypeInfo.getFieldNames.length
          }) {
            val fieldName = rowTypeInfo.getFieldNames.apply(i)
            if (fieldName.equalsIgnoreCase(leftField) || fieldName.toUpperCase.equalsIgnoreCase(leftField)) equalFieldIndex = i

            {
              i += 1
              i - 1
            }
          }
          if (equalFieldIndex == -1) throw new RuntimeException("can't find equal field " + leftField)

          equalValIndex.append(equalFieldIndex)
        } else throw new RuntimeException("resolve equalFieldList error:" + sqlNode.toString)

      }
    }

    sqlNodeList.foreach(dealOneEqualCon)


    // parseSelectFields start
    val sideFieldIndex = mutable.Map[Int, Int]()
    val inFieldIndex = mutable.Map[Int, Int]()
    val sideFieldNameIndex = mutable.Map[Int, String]()
    val fields = mutable.ListBuffer[String]()

    val sideTableName = joinInfo.getSideTableName
    val nonSideTableName = joinInfo.getNonSideTable

    outFieldInfoList.zipWithIndex.foreach {
      case (fieldInfo, index) =>
        if (fieldInfo.getTable.equalsIgnoreCase(sideTableName)) {
          fields.append(fieldInfo.getFieldName)
          sideFieldIndex.put(index, fields.length - 1)
          val name = if (fieldInfo.getRawFieldName == null) fieldInfo.getFieldName else fieldInfo.getRawFieldName
          sideFieldNameIndex.put(index, name)
          if (equalFieldList.contains(fieldInfo.getFieldName)) equalFieldRawMap.put(fieldInfo.getFieldName, name)
        } else if (fieldInfo.getTable.equalsIgnoreCase(nonSideTableName)) {
          val nonSideIndex = rowTypeInfo.getFieldIndex(fieldInfo.getFieldName)
          inFieldIndex.put(index, nonSideIndex)
        } else throw new RuntimeException("unknown table " + fieldInfo.getTable)
    }
    // parseSelectFields end


    SideInfo(
      equalValIndex.toList,
      equalFieldList.toList,
      equalFieldRawMap.toMap,
      sideFieldIndex.toMap,
      inFieldIndex.toMap,
      sideFieldNameIndex.toMap,
      fields.toList,
      joinInfo.getJoinType
    )
  }
}
