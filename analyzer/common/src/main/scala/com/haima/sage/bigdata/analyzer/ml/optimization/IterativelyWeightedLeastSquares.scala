/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.haima.sage.bigdata.analyzer.ml.optimization

import com.haima.sage.bigdata.analyzer.ml.linalg.BLAS
import com.haima.sage.bigdata.analyzer.ml.model.{Instance, OffsetInstance}
import com.haima.sage.bigdata.etl.utils.Logger
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.utils._
import org.apache.flink.api.scala.{DataSet, createTypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.math.DenseVector

/**
  * Model fitted by [[IterativelyWeightedLeastSquares]].
  *
  * @param coefficients  model coefficients
  * @param intercept     model intercept
  * @param diagInvAtWA   diagonal of matrix (A^T * W * A)^-1 in the last iteration
  * @param numIterations number of iterations
  */
private[ml] class IterativelyWeightedLeastSquaresModel(
                                                        val coefficients: DenseVector,
                                                        val intercept: Double,
                                                        val diagInvAtWA: DenseVector,
                                                        val numIterations: Int) extends Serializable

/**
  * Implements the method of iteratively reweighted least squares (IRLS) which is used to solve
  * certain optimization problems by an iterative method. In each step of the iterations, it
  * involves solving a weighted least squares (WLS) problem by [[WeightedLeastSquares]].
  * It can be used to find maximum likelihood estimates of a generalized linear model (GLM),
  * find M-estimator in robust regression and other optimization problems.
  *
  * @param initialModel the initial guess model.
  * @param weightFunc   the reweight function which is used to update working labels and weights
  *                     at each iteration.
  * @param fitIntercept whether to fit intercept.
  * @param regParam     L2 regularization parameter used by WLS.
  * @param maxIter      maximum number of iterations.
  * @param tol          the convergence tolerance.
  * @see <a href="http://www.jstor.org/stable/2345503">P. J. Green, Iteratively
  *      Weighted Least Squares for Maximum Likelihood Estimation, and some Robust
  *      and Resistant Alternatives, Journal of the Royal Statistical Society.
  *      Series B, 1984.</a>
  */
private[ml] class IterativelyWeightedLeastSquares(
                                                   val initialModel: WeightedLeastSquaresModel,
                                                   val weightFunc: (OffsetInstance, WeightedLeastSquaresModel) => (Double, Double),
                                                   val fitIntercept: Boolean,
                                                   val regParam: Double,
                                                   val maxIter: Int,
                                                   val tol: Double) extends Logger with Serializable {

  def weightCal: RichMapFunction[OffsetInstance, Instance] = new RichMapFunction[OffsetInstance, Instance] {
    var oldModel: WeightedLeastSquaresModel = _

    override def open(parameters: Configuration): Unit = {
      oldModel = getRuntimeContext.getBroadcastVariable[WeightedLeastSquaresModel]("model").get(0)
    }

    override def map(instance: OffsetInstance): Instance = {
      val (newLabel, newWeight) = weightFunc(instance, oldModel)
      Instance(newLabel, newWeight, instance.features)
    }
  }

  def fit(instances: DataSet[OffsetInstance]): DataSet[WeightedLeastSquaresModel] = {

    val inits = instances.getExecutionEnvironment.fromElements(initialModel)


    inits.iterateWithTermination(maxIter) {
      model => {
        val newInstances = instances.map(weightCal).withBroadcastSet(model, "model")
        val newModel = new WeightedLeastSquares(fitIntercept, regParam, elasticNetParam = 0.0,
          standardizeFeatures = false, standardizeLabel = false).fit(newInstances)

        (newModel, model.zipWithIndex.join(newModel.zipWithIndex).where(0).equalTo(0).filter(d => {
          // Check convergence
          val oldCoefficients = d._1._2.coefficients
          val coefficients = d._2._2.coefficients
          BLAS.axpy(-1.0, coefficients, oldCoefficients)
          val maxTolOfCoefficients = oldCoefficients.data.foldLeft(0.0) { (x, y) =>
            math.max(math.abs(x), math.abs(y))
          }
          val maxTol = math.max(maxTolOfCoefficients, math.abs(d._1._2.intercept - d._2._2.intercept))
          maxTol >= tol
        }).map(_._2._2))

      }
    }
  }
}
