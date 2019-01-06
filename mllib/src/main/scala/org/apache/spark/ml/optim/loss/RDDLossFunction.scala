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
package org.apache.spark.ml.optim.loss

import scala.reflect.ClassTag

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.DiffFunction

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.ml.optim.aggregator.DifferentiableLossAggregator
import org.apache.spark.rdd.RDD

/**
 * This class computes the gradient and loss of a differentiable loss function by mapping a
 * [[DifferentiableLossAggregator]] over an [[RDD]]. The loss function is the
 * sum of the loss computed on a single instance across all points in the RDD. Therefore, the actual
 * analytical form of the loss function is specified by the aggregator, which computes each points
 * contribution to the overall loss.
 *
 * A differentiable regularization component can also be added by providing a
 * [[DifferentiableRegularization]] loss function.
 *
 * @param instances RDD containing the data to compute the loss function over.
 * @param getAggregator A function which gets a new loss aggregator in every tree aggregate step.
 * @param regularization An option representing the regularization loss function to apply to the
 *                       coefficients.
 * @param aggregationDepth The aggregation depth of the tree aggregation step.
 * @tparam Agg Specialization of [[DifferentiableLossAggregator]], representing the concrete type
 *             of the aggregator.
 */
private[ml] class RDDLossFunction[
    T: ClassTag,
    Agg <: DifferentiableLossAggregator[T, Agg]: ClassTag](
    instances: RDD[T],
    getAggregator: (Broadcast[Vector] => Agg),
    regularization: Option[DifferentiableRegularization[Vector]],
    aggregationDepth: Int = 2)
  extends DiffFunction[BDV[Double]] {

  override def calculate(coefficients: BDV[Double]): (Double, BDV[Double]) = {
    val bcCoefficients = instances.context.broadcast(Vectors.fromBreeze(coefficients))
    val thisAgg = getAggregator(bcCoefficients)
    val seqOp = (agg: Agg, x: T) => agg.add(x)
    val combOp = (agg1: Agg, agg2: Agg) => agg1.merge(agg2)
    val newAgg = instances.treeAggregate(thisAgg)(seqOp, combOp, aggregationDepth)
    val gradient = newAgg.gradient
    val regLoss = regularization.map { regFun =>
      val (regLoss, regGradient) = regFun.calculate(Vectors.fromBreeze(coefficients))
      BLAS.axpy(1.0, regGradient, gradient)
      regLoss
    }.getOrElse(0.0)
    bcCoefficients.destroy(blocking = false)
    (newAgg.loss + regLoss, gradient.asBreeze.toDenseVector)
  }
}
