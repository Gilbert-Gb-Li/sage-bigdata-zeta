package com.haima.sage.bigdata.analyzer.ml.optimization

import org.apache.flink.ml.optimization.PartialLossFunction

/** Logistic loss function which can be used with the [[org.apache.flink.ml.optimization.GenericLossFunction]]
  *
  *
  * The [[LogisticLoss]] function implements `-y*log(g(prediction))-(1-y)*log(g(prediction))`
  * for binary classification with label in {0, 1}.Where function g is the sigmoid function.
  * The sigmoid function is defined as:
  *
  * `g(z) = 1 / (1+exp(-z))`
  */
object LogisticLoss extends PartialLossFunction {

  /** Calculates the loss depending on the label and the prediction
    *
    * @param prediction The predicted value
    * @param label The true value
    * @return The loss
    */
  override def loss(prediction: Double, label: Double): Double = {
    if (label > 0) {
      math.log(1 + math.exp(prediction))
    } else {
      math.log(1 + math.exp(prediction)) - prediction
    }
  }

  /** Calculates the derivative of the loss function with respect to the prediction
    *
    * @param prediction The predicted value
    * @param label The true value
    * @return The derivative of the loss function
    */
  override def derivative(prediction: Double, label: Double): Double = {
    (1.0 / (1.0 + math.exp(-prediction))) - label
  }
}
