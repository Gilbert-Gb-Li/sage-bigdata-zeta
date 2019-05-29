package com.haima.sage.bigdata.analyzer.utils.preprocessing

import com.haima.sage.bigdata.analyzer.ml.IDFModel
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.knowledge.{KnowledgeSingle, KnowledgeUser}
import org.apache.flink.ml.math.SparseVector

object ModelHelper {
  //加载SameWeight
  def getSameWeight(id: Option[String]): RichMap = {
    val helper: Option[KnowledgeUser] = id.map(KnowledgeSingle(_))
    helper.map(_.getAll()) match {
      case Some(idf) =>
        RichMap(idf.head)

      case _ =>
        throw new NotImplementedError("SameWeight model is not set")
    }


  }

  def toSWModel(data: Map[String, Any]): Map[String, SparseVector] = {
    data.get("data").map(d => {

      d.toString.split(":").map(d => {
        val kv = d.split(",")
        (kv(0), SparseVector(kv(1).toInt, Array(kv(2).toInt), Array(1)))
      }).toMap
    }).orNull
  }

  def toTFIDFModel(data: Map[String, Any]): IDFModel = {
    data.get("data").map(d => {

      val d2 = d.toString.split(" ")
      val size = d2(0).toInt
      val indices = d2(1).split(",").map(d => {
        d.toInt
      })
      val data = d2(1).split(",").map(d => {
        d.toDouble
      })
      new IDFModel(SparseVector(size, indices, data))
    }).orNull
  }

  //加载IDFMODEL
  def getIDF(id: Option[String]): RichMap = {
    val helper: Option[KnowledgeUser] = id.map(KnowledgeSingle(_))
    helper.map(_.getAll()) match {
      case Some(idf) =>
        RichMap(idf.head)
      case _ =>
        throw new NotImplementedError("tf/idf model is not set")
    }


  }
}
