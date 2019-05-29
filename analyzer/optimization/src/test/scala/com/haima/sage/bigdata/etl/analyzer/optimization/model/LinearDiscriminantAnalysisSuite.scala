package com.haima.sage.bigdata.analyzer.optimization.model


import breeze.linalg.{DenseMatrix, DenseVector => BDV}
import breeze.numerics.abs
import breeze.stats.distributions.{Gaussian, Multinomial}
import com.haima.sage.bigdata.analyzer.ml.utils.MatrixUtils
import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.preprocessing.StandardScaler
import org.junit.Test

class LinearDiscriminantAnalysisSuite {
  val env = ExecutionEnvironment.getExecutionEnvironment

  @Test
  def testIrisData(): Unit = {


    // Uses the Iris flower dataset


    val irisData = env.readTextFile("/Users/zhhuiyan/workspace/sage-bigdata-etl/analyzer/optimization/src/test/resources/iris.data")
    val trainData = irisData.map(
      item => {
        val data = item.split(",").dropRight(1).map(_.toDouble)
        val lable = item match {
          case x if x.endsWith("Iris-setosa") => 1
          case x if x.endsWith("Iris-versicolor") => 2
          case x if x.endsWith("Iris-virginica") => 3
        }
        LabeledVector(lable, DenseVector(data))
      })

    val scaler = new StandardScaler()
    scaler.fit[LabeledVector](trainData)
    val scalerData = scaler.transform[LabeledVector, LabeledVector](trainData)
    val lda = new LinearDiscriminantAnalysis(2)
    val out = lda.fit(scalerData).collect().head

    // Correct output taken from http://sebastianraschka.com/Articles/2014_python_lda.html#introduction
    println(s"\n${out.eigen}")
    val majorVector = BDV(-0.1498, -0.1482, 0.8511, 0.4808)
    val minorVector = BDV(0.0095, 0.3272, -0.5748, 0.75)

    // Note that because eigenvectors can be reversed and still valid, we allow either direction
    assert(aboutEq(out.eigen(::, 0), majorVector, 1E-4) || aboutEq(out.eigen(::, 0), majorVector * -1.0, 1E-4))
    assert(aboutEq(out.eigen(::, 1), minorVector, 1E-4) || aboutEq(out.eigen(::, 1), minorVector * -1.0, 1E-4))

  }

  /**
    * Compares two arrays for approximate equality. Modify margin by setting Stats.thresh.
    *
    * @param as     A array of numbers.
    * @param bs     A second array of numbers.
    * @param thresh equality threshold
    * @return True if the two numbers are within `thresh` of each other.
    */
  def aboutEq(as: BDV[Double], bs: BDV[Double], thresh: Double): Boolean = {
    require(as.length == bs.length, "Vectors must be the same size")
    abs(as - bs).toArray.forall(_ < thresh)
  }

  //test("Solve Linear Discriminant Analysis on the Iris Dataset") { }

  //  test("Check LDA output for a diagonal covariance") {
  //
  //    val matRows = 1000
  //    val matCols = 10
  //    val dimRed = 5
  //
  //    // Generate a random Gaussian matrix.
  //    val gau = new Gaussian(0.0, 1.0)
  //    val randMatrix = new DenseMatrix(matRows, matCols, gau.sample(matRows*matCols).toArray)
  //
  //    // Parallelize and estimate the LDA.
  //    val data = sc.parallelize(MatrixUtils.matrixToRowArray(randMatrix))
  //    val labels = data.map(x => Multinomial(DenseVector(0.2, 0.2, 0.2, 0.2, 0.2)).draw(): Int)
  //    val lda = new LinearDiscriminantAnalysis(dimRed).fit(data, labels)
  //
  //    // Apply LDA to the input data.
  //    val redData = lda(data)
  //    val redMat = MatrixUtils.rowsToMatrix(redData.collect)
  //
  //    // Compute its covariance.
  //    val redCov = cov(redMat)
  //    log.info(s"Covar\n$redCov")
  //
  //    // The covariance of the dimensionality reduced matrix should be diagonal.
  //    for (
  //      x <- 0 until dimRed;
  //      y <- 0 until dimRed if x != y
  //    ) {
  //      assert(Stats.aboutEq(redCov(x,y), 0.0, 1e-6), s"LDA Matrix should be 0 off-diagonal. $x,$y = ${redCov(x,y)}")
  //    }
  //  }

}
