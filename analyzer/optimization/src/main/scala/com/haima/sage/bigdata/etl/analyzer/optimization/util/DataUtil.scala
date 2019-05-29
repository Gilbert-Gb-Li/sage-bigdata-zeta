package com.haima.sage.bigdata.analyzer.optimization.util

import breeze.linalg.{DenseMatrix => BDM}
import com.haima.sage.bigdata.analyzer.optimization.model.PCAModel
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.knowledge.{KnowledgeSingle, KnowledgeUser}

/**
  * Created by evan on 18-5-2.
  */
object DataUtil extends Serializable {
  def getPCAModel(knowledgeId: String): RichMap = {
    val helper: KnowledgeUser = KnowledgeSingle(knowledgeId)
    //id对应就是模型库的表名
    RichMap(helper.getAll().head)
  }

  def toPCAModel(model: RichMap): PCAModel = {
    val e = model.get("eigenvalues").get
    val eigenvalues: breeze.linalg.DenseVector[Double] = new breeze.linalg.DenseVector(model.get("eigenvalues").get.toString.split(",").map(x =>
      x.toDouble
    ))
    val row = model.get("row").get.asInstanceOf[Double].toInt
    val col = model.get("col").get.asInstanceOf[Double].toInt
    val k = model.get("k").get.asInstanceOf[Double].toInt
    val matrixArray = model.get("eigenvecor").get.toString.split(",").map(_.toDouble)
    val eigenvecor: BDM[Double] = BDM.zeros(row, col)
    var i, j = 0
    var x = 0
    while (i < row) {

      while (j < col) {
        eigenvecor(i, j) = matrixArray(x)
        j += 1
        x += 1
      }
      i += 1
    }
    new PCAModel(k, eigenvalues, eigenvecor)
  }
}
