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

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.DiffFunction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.optim.aggregator.DifferentiableLossAggregator
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

private[ml] class RDDLossFunction[Agg <: DifferentiableLossAggregator[Instance, Agg]: ClassTag](
    instances: RDD[Instance],
    getAggregator: (Broadcast[Vector] => Agg),
    regularization: Option[DifferentiableRegularization[Array[Double]]],
    aggregationDepth: Int = 2)
  extends DiffFunction[BDV[Double]] {

  override def calculate(coefficients: BDV[Double]): (Double, BDV[Double]) = {
    val bcCoefficients = instances.context.broadcast(Vectors.dense(coefficients.data))
//    val thisAgg = aggregator.create(bcCoefficients)
    val thisAgg = getAggregator(bcCoefficients)
    val seqOp = (agg: Agg, x: Instance) => agg.add(x)
    val combOp = (agg1: Agg, agg2: Agg) => agg1.merge(agg2)
    val newAgg = instances.treeAggregate(thisAgg)(seqOp, combOp, aggregationDepth)
    val gradient = newAgg.gradient
    val regLoss = regularization.map { regFun =>
      val (regLoss, regGradient) = regFun.calculate(coefficients.data)
      BLAS.axpy(1.0, Vectors.dense(regGradient), gradient)
      regLoss
    }.getOrElse(0.0)
    bcCoefficients.destroy(blocking = false)
    (newAgg.loss + regLoss, gradient.asBreeze.toDenseVector)
  }
}



