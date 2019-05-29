package com.haima.sage.bigdata.analyzer.optimization.model

import breeze.linalg.{eigSym, DenseMatrix => BreezeMatrix}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.api.scala.utils._
import org.apache.flink.ml.math.{DenseMatrix, DenseVector}

class PrincipalComponentAnalysis {

}

object PrincipalComponentAnalysis {
  def apply() = new PrincipalComponentAnalysis()

  def pca(dimensions: Int, inputDS: DataSet[DenseVector], env: ExecutionEnvironment) = {
    val featureCount = inputDS.count().toInt

    require(dimensions > 0 && dimensions < featureCount,
      s"Dimensions must be between 0 and ${featureCount}")

    val adjustedDS = inputDS.map { feature =>
      feature.data.map(dataPoint =>
        dataPoint - feature.data.sum / feature.size
      )
    }

    // Want to do:
    // zipWithIndex without converting the vector to an Array
    val meanAdjustedArrays: DataSet[(Long, Array[Double])] = adjustedDS.zipWithIndex

    case class VectorTuple(id: Long, vector: DenseVector)

    val meanAdjustedVectors: DataSet[VectorTuple] =
      meanAdjustedArrays.map(
        x => VectorTuple(x._1, new DenseVector(x._2))
      )

    val cross = meanAdjustedVectors.cross(meanAdjustedVectors)

    case class Coordinate(x: Int, y: Int)
    case class CovarianceEntry(coordinate: Coordinate, covariance: Double)

    val covarianceMap = new MapFunction[(VectorTuple, VectorTuple), (CovarianceEntry)] {
      override def map(x: (VectorTuple, VectorTuple)): CovarianceEntry = {
        val coordinateTuple = Coordinate(x._1.id.toInt, x._2.id.toInt)
        val covariance = x._1.vector.dot(x._2.vector) / (x._1.vector.size - 1)

        return CovarianceEntry(coordinateTuple, covariance)
      }
    }

    val covariances: DataSet[CovarianceEntry] = cross.map(covarianceMap)
    val covarianceMatrix = DenseMatrix.zeros(featureCount, featureCount)

    // Want to do:
    // Commented code below.
    // afaik can't do this because creating and populating the matrix requires data locality?
    //    val covToMatrixMap = new MapFunction[CovarianceEntry, Unit] {
    //      override def map(entry: CovarianceEntry) = {
    //        covarianceMatrix.update(entry.coordinate.x, entry.coordinate.y, entry.covariance)
    //        covarianceMatrix.update(entry.coordinate.y, entry.coordinate.x, entry.covariance)
    //      }
    //    }
    //    covariances.map(covToMatrixMap)

    covariances.collect().foreach { (entry: CovarianceEntry) =>
      covarianceMatrix.update(entry.coordinate.x, entry.coordinate.y, entry.covariance)
      covarianceMatrix.update(entry.coordinate.y, entry.coordinate.x, entry.covariance)
    }

    // Want to do:
    // val breezeCovarianceMatrix = Breeze.Matrix2BreezeConverter(covarianceMatrix).asBreeze
    // getting an implicit type error...
    val bcv = new BreezeMatrix(featureCount, featureCount, covarianceMatrix.data)
    val eigenSet = eigSym(bcv)


    val eigenTupleList: Array[(Double, DenseVector)] =
      eigenSet.eigenvalues.data.zipWithIndex map { eigenvalueWithIndex =>
        val eigenvalue = eigenvalueWithIndex._1
        val index = eigenvalueWithIndex._2
        // Want to do:
        // val flinkEigenvector = Breeze.Breeze2VectorConverter(breezeEigenvector).fromBreeze
        // doesn't seem to obey the stride/length properties on BreezeVector?

        val breezeVector = eigenSet.eigenvectors.t(::, index).toDenseVector
        val flinkVector = DenseVector(breezeVector.data)
        (eigenvalue, flinkVector)
      }

    val eigenTuplesSorted = eigenTupleList sortBy {
      _._1
    }
    val topEigenTuples = eigenTuplesSorted takeRight dimensions

    val out: DataSet[(Double, DenseVector)] = env.fromCollection(topEigenTuples)
    out
  }
}