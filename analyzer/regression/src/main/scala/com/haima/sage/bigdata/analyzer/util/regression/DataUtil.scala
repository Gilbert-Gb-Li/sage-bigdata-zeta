package com.haima.sage.bigdata.analyzer.util.regression

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.knowledge.{KnowledgeSingle, KnowledgeUser}
import org.apache.flink.ml.common.WeightVector
import org.apache.flink.ml.math.{DenseVector, SparseVector}

object DataUtil extends Serializable {


  def model(knowledgeId: Option[String]): Option[WeightVector] = {
    getModel(knowledgeId) match {
      case Some(m) =>
        toModel(m)
      case None =>
        None
    }
  }

  def getModel(knowledgeId: Option[String]): Option[RichMap] = {
    knowledgeId match {
      case Some(id: String) =>
        if (!id.isEmpty) {
          val helper: KnowledgeUser = KnowledgeSingle(id)
          //id对应就是模型库的表名
          val data: Iterable[Map[String, Any]] = helper.getAll()
          data.find(item => {
            val t = item.get("type")
            t.isDefined && t.exists(_ != null)
          }).map(RichMap(_))
        } else None
      case _ =>
        None
    }
  }

  def toModel(data: RichMap): Option[WeightVector] = {
    val t = data.get("type")
    if (t.isDefined && t.exists(_ != null)) {
      t match {
        case Some("sparse") =>
          Some(WeightVector(SparseVector(data("size").toString.toInt, data("indices").toString.split(",").map(_.toInt),
            data("data").toString.split(",").map(_.toDouble)), data("intercept").toString.toDouble))
        case Some("dense") =>
          Some(WeightVector(DenseVector(data("data").toString.split(",").map(_.toDouble)), data("intercept").toString.toDouble))
        case _ =>
          None
      }
    }
    else

      None
  }

}
