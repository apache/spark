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

package org.apache.spark.mllib.grouped

import org.apache.spark.mllib.optimization.{Updater, Gradient}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import scala.collection.mutable.ArrayBuffer
import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.SparkContext._
import scala.reflect.ClassTag

class GroupedGradientDescent[K](private var gradient: Gradient, private var updater: Updater)
                               (implicit tag:ClassTag[K])
  extends GroupedOptimizer[K] with Logging {
  /**
   * Solve the provided convex optimization problem.
   */

    private var stepSize: Double = 1.0
    private var numIterations: Int = 100
    private var regParam: Double = 0.0
    private var miniBatchFraction: Double = 1.0


  /**
   * Set the initial step size of SGD for the first step. Default 1.0.
   * In subsequent steps, the step size will decrease with stepSize/sqrt(t)
   */
  def setStepSize(step: Double): this.type = {
    this.stepSize = step
    this
  }

  /**
   * :: Experimental ::
   * Set fraction of data to be used for each SGD iteration.
   * Default 1.0 (corresponding to deterministic/classical gradient descent)
   */
  @Experimental
  def setMiniBatchFraction(fraction: Double): this.type = {
    this.miniBatchFraction = fraction
    this
  }

  /**
   * Set the number of iterations for SGD. Default 100.
   */
  def setNumIterations(iters: Int): this.type = {
    this.numIterations = iters
    this
  }

  /**
   * Set the regularization parameter. Default 0.0.
   */
  def setRegParam(regParam: Double): this.type = {
    this.regParam = regParam
    this
  }

  /**
   * Set the gradient function (of the loss function of one single data example)
   * to be used for SGD.
   */
  def setGradient(gradient: Gradient): this.type = {
    this.gradient = gradient
    this
  }


  /**
   * Set the updater function to actually perform a gradient step in a given direction.
   * The updater is responsible to perform the update from the regularization term as well,
   * and therefore determines what kind or regularization is used, if any.
   */
  def setUpdater(updater: Updater): this.type = {
    this.updater = updater
    this
  }

  /**
   * :: DeveloperApi ::
   * Runs gradient descent on the given training data.
   * @param data training data
   * @param initialWeights initial weights
   * @return solution vector
   */
  @DeveloperApi
   override def optimize( data: RDD[(K, (Double, linalg.Vector))],
                          initialWeights: Map[K, linalg.Vector]) :
  Map[K, linalg.Vector] = {
      val out = GroupedGradientDescent.runMiniBatchSGD(
        data,
        gradient,
        updater,
        stepSize,
        numIterations,
        regParam,
        miniBatchFraction,
        initialWeights)
      out.map( x => (x._1, x._2._1) )
    }
}


object GroupedGradientDescent extends Logging {

  def runMiniBatchSGD[K](
                       data: RDD[(K, (Double, Vector))],
                       gradient: Gradient,
                       updater: Updater,
                       stepSize: Double,
                       numIterations: Int,
                       regParam: Double,
                       miniBatchFraction: Double,
                       initialWeightsSet: Map[K,Vector])
                        (implicit tag:ClassTag[K]) :
  Map[K,(Vector, Array[Double])] = {

    val stochasticLossHistory = data.keys.collect().map( x =>
      (x, new ArrayBuffer[Double](numIterations))
    ).toMap

    val numExamples = data.countByKey()
    val miniBatchSize = numExamples.map( x => (x._1, x._2 * miniBatchFraction) )

    // Initialize weights as a column vector
    var weightSet = initialWeightsSet.map(x => (x._1, Vectors.dense(x._2.toArray)))

    var regVal = weightSet.map( weights => (weights._1, updater.compute(
      weights._2, Vectors.dense(new Array[Double](weights._2.size)), 0, 1, regParam)._2 ))

    val dataWithKey = data.map( x => (x._1, (x._1, x._2._1, x._2._2)))
    val gradient_b = data.context.broadcast(gradient)
    val updater_b = data.context.broadcast(updater)
    for (i <- 1 to numIterations) {
      // Local copies of all the variables used in the loop. If you try
      // to access variables outside the for loop in the closure then
      // scala will all an $outer to the closer and pull in all the variables
      // which will cause some memory issues
      val gradient_b_local = gradient_b
      val updater_b_local = updater_b
      val weightSet_b = data.context.broadcast(weightSet)
      val miniBatchSize_local = miniBatchSize

      val gradientOut = dataWithKey.sample(false, miniBatchFraction, 42 + i)
        .combineByKey[(BDV[Double], Double)](
          createCombiner = (x : (K, Double, Vector)) => {
            val key = x._1
            val label = x._2
            val features = x._3
            val (new_gradient, new_loss) = gradient_b_local.value.compute(
              features, label, weightSet_b.value(key)
            )
            (BDV(new_gradient.toArray), new_loss)
          },
          mergeValue = (x : (BDV[Double],Double), y : (K,Double,Vector)) => {
            var in_gradient = x._1
            val loss = x._2
            val key = y._1
            val label = y._2
            val features = Vectors.dense(y._3.toArray)
            val (new_gradient, new_loss) = gradient_b_local.value.compute(
              features, label, weightSet_b.value(key)
            )
            (in_gradient + BDV(new_gradient.toArray), loss + new_loss)
          },
          mergeCombiners = (x:(BDV[Double],Double), y:(BDV[Double],Double)) => {
            (x._1 + y._1, x._2 + y._2)
          }
        )
      val lossSums = gradientOut.map(x => (x._1, x._2._2)).collect.toMap
      stochasticLossHistory.foreach( x => {
        x._2.append(lossSums(x._1) / miniBatchSize(x._1) + regVal(x._1))
      } )
      val update = gradientOut.map( x => {
        val a : BDV[Double] = BDV(x._2._1.toArray) / miniBatchSize_local(x._1)
        val supdate = updater_b_local.value.compute(
          weightSet_b.value(x._1), Vectors.dense(a.toArray), stepSize, i, regParam)
        (x._1, supdate)
      })
      val update_c = update.collect()
      weightSet = update_c.map( x => (x._1, x._2._1)).toMap
      regVal = update_c.map( x => (x._1, x._2._2)).toMap
      weightSet_b.unpersist()
    }
    weightSet.map( x => (x._1, (x._2, stochasticLossHistory(x._1).toArray)))
  }
}
