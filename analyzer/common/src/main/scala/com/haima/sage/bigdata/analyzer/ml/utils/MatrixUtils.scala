package com.haima.sage.bigdata.analyzer.ml.utils

import breeze.linalg._
import org.apache.flink.ml.math.Breeze._
import org.apache.flink.ml.math.{DenseMatrix => SDM, DenseVector => SDV, Matrix => SM, SparseMatrix => SSM, SparseVector => SSV, Vector => SV}

import scala.reflect.ClassTag

object MatrixUtils {
  def transpose(arr: Array[Array[Double]]): Array[Array[Double]] = {
    val mat = new Array[Array[Double]](arr.head.length)
    for (i <- arr.head.indices) {
      mat(i) = arr.map(_ (i))
    }
    mat
  }

  def matToRowArrs(mat: SM): Array[Array[Double]] = {
    val arrs = new Array[Array[Double]](mat.rows)
    for (r <- 0 until mat.rows) {
      arrs(r) = toBreeze(mat)(r to r, 0 to mat.cols - 1).toDenseMatrix.toArray
    }
    arrs
  }
  /**
    * Converts a matrix to an array of rows.
    * @param mat Input matrix.
    * @return Array of rows.
    */
  def matrixToRowArray[T : ClassTag](mat: DenseMatrix[T]): Array[DenseVector[T]] = {
    val matT = mat.t
    // The explicit copy of the vector is necessary because otherwise Breeze slices
    // lazily, leading to inflated serialization size (A serious issue w/ spark)
    (0 until mat.rows).toArray.map(x => DenseVector(matT(::, x).toArray))
  }

  def matToRowArrs(mat: Matrix[Double]): Array[Array[Double]] = {
    val arrs = new Array[Array[Double]](mat.rows)
    for (r <- 0 until mat.rows) {
      arrs(r) = mat(r to r, 0 to mat.cols - 1).toDenseMatrix.toArray
    }
    arrs
  }

  /**
    * Converts an array of DenseVector to a matrix where each vector is a row.
    *
    * @param inArr Array of DenseVectors (rows)
    * @return A row matrix.
    */
  def rowsToMatrix[T : ClassTag](inArr: Array[DenseVector[T]]): DenseMatrix[T] = {
    val nRows = inArr.length
    val nCols = inArr(0).length
    val outArr = new Array[T](nRows * nCols)
    var i = 0
    while (i < nRows) {
      var j = 0
      val row = inArr(i)
      while (j < nCols) {
        outArr(i + nRows * j) = row(j)
        j = j + 1
      }
      i = i + 1
    }
    val outMat = new DenseMatrix[T](nRows, nCols, outArr)
    outMat
  }

  def arrsToMat(arrs: Iterator[Array[Double]]): DenseMatrix[Double] = {
    val dd = arrs.toArray

    println( "wqweqeqw:"+dd.length)

    if(dd.nonEmpty){
      new DenseMatrix[Double](dd.length, dd.head.length, dd.reduce[Array[Double]](_ ++ _))
    }else{
      DenseMatrix.zeros(0,0)
    }


  }

  def vecArrsToMats(vecArrs: Iterator[Array[Double]], chunkSize: Int)
  : Iterator[DenseMatrix[Double]] = {
    new Iterator[DenseMatrix[Double]] {
      def hasNext: Boolean = vecArrs.hasNext

      def next(): DenseMatrix[Double] = {
        val firstVec = vecArrs.next()
        val vecLen = firstVec.length
        val arr = new Array[Double](chunkSize * vecLen)
        System.arraycopy(firstVec, 0, arr, 0, vecLen)

        var i = 1
        var offs = 0
        while (i < chunkSize && vecArrs.hasNext) {
          val vec = vecArrs.next()
          System.arraycopy(vec, 0, arr, offs, vecLen)
          i += 1
          offs += vecLen
        }

        new DenseMatrix[Double](i, firstVec.length, arr)
      }
    }
  }

  /**
    * Creates a spark-mllib matrix instance from a breeze matrix.
    *
    * @param breeze a breeze matrix
    * @return a spark-mllib Matrix instance
    */
  def fromBreeze(breeze: Matrix[Double]): SM = {
    breeze.fromBreeze
    //    breeze match {
    //      case dm: DenseMatrix[Double] =>
    //        new SDM(dm.rows, dm.cols, dm.data)//没有dm.isTranspose这个参数
    //      case sm: CSCMatrix[Double] =>
    //        // There is no isTranspose flag for sparse matrices in Breeze
    //        new SSM(sm.rows, sm.cols, sm.colPtrs, sm.rowIndices, sm.data)
    //      case _ =>
    //        throw new UnsupportedOperationException(
    //          s"Do not support conversion from type ${breeze.getClass.getName}.")
    //    }
  }

  /**
    * Creates a breeze matrix instance from a spark-mllib matrix.
    *
    * @param sparkMatrix a breeze matrix
    * @return a spark-mllib Matrix instance
    */
  def toBreeze(sparkMatrix: SM): Matrix[Double] = {

    sparkMatrix.asBreeze
    //    sparkMatrix match {
    //      case dm: SDM =>
    //        if (!dm.isTranspose) {
    //          new DenseMatrix[Double](dm.numRows, dm.numCols, dm.data)
    //        } else {
    //          val breezeMatrix = new DenseMatrix[Double](dm.numCols, dm.numRows, dm.data)
    //          breezeMatrix.t
    //        }
    //      case sm: SSM =>
    //        new CSCMatrix[Double](sm.data, sm.numRows, sm.numCols, sm.colPtrs, sm.rowIndices)
    //      case _ =>
    //        throw new UnsupportedOperationException(
    //          s"Do not support conversion from type ${sparkMatrix.getClass.getName}.")
    //    }
  }

  /**
    * Creates a spark-mllib vector instance from a breeze vector.
    *
    * @param breezeVector a breeze vector
    * @return a spark-mllib Vector instance
    */
  def fromBreeze(breezeVector: Vector[Double]): SV = {
    breezeVector match {
      case v: DenseVector[Double] =>
        if (v.offset == 0 && v.stride == 1 && v.length == v.data.length) {
          new SDV(v.data)
        } else {
          new SDV(v.toArray) // Can't use underlying array directly, so make a new one
        }
      case v: SparseVector[Double] =>
        if (v.index.length == v.used) {
          new SSV(v.length, v.index, v.data)
        } else {
          new SSV(v.length, v.index.slice(0, v.used), v.data.slice(0, v.used))
        }
      case v: SliceVector[_, Double] =>
        new SDV(v.toArray)
      case v: Vector[_] =>
        sys.error("Unsupported Breeze vector type: " + v.getClass.getName)
    }
  }

  /**
    * Creates a breeze vector instance from a spark-mllib vector.
    *
    * @param sparkVector a spark-mllib vector
    * @return a breeze vector instance
    */
  def toBreeze(sparkVector: SV): Vector[Double] = {
    sparkVector match {
      case v: SDV =>
        new DenseVector[Double](v.data)
      case v: SSV =>
        new SparseVector[Double](v.indices, v.data, v.size)
    }
  }

  implicit def mSparkToBreeze(sparkMatrix: SM): Matrix[Double] =
    toBreeze(sparkMatrix)

  implicit def dmSparkToBreeze(sparkMatrix: SDM): DenseMatrix[Double] =
    toBreeze(sparkMatrix).asInstanceOf[DenseMatrix[Double]]

  implicit def mBreezeToSpark(breezeMatrix: Matrix[Double]): SM =
    fromBreeze(breezeMatrix)

  implicit def dmBreezeToSpark(breezeMatrix: DenseMatrix[Double]): SDM =
    fromBreeze(breezeMatrix).asInstanceOf[SDM]

  implicit def vSparkToBreeze(sparkVector: SV): Vector[Double] =
    toBreeze(sparkVector)

  implicit def dvSparkToBreeze(sparkVector: SDV): DenseVector[Double] =
    toBreeze(sparkVector).asInstanceOf[DenseVector[Double]]

  implicit def vBreezeToSpark(breezeVector: Vector[Double]): SV =
    fromBreeze(breezeVector)

  implicit def dvBreezeToSpark(breezeVector: DenseVector[Double]): SDV =
    fromBreeze(breezeVector).asInstanceOf[SDV]

  implicit def fvtovBreezeToSpark(f: (Vector[Double]) => Vector[Double])
  : (SV) => SV = {
    v: SV => f(v)
  }

}