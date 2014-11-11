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

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{Logging, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.{GeneralizedLinearAlgorithm, GeneralizedLinearModel, LabeledPoint}
import org.apache.spark.SparkContext._
import scala.reflect.ClassTag


/**
 * :: DeveloperApi ::
 * GeneralizedLinearAlgorithm implements methods to train a Generalized Linear Model (GLM).
 * This class should be extended with an Optimizer to create a new GLM.
 */
@DeveloperApi
abstract class GroupedGeneralizedLinearAlgorithm[K, M <: GeneralizedLinearModel]
  extends Logging with Serializable {

  protected val validators: Seq[RDD[LabeledPoint] => Boolean] = List()

  /** The optimizer to solve the problem. */
  def optimizer: GroupedOptimizer[K]

  /**
   * Create a model given the weights and intercept
   */
  protected def createModel(weights: Vector, intercept: Double): M


  /** Whether to add intercept (default: false). */
  protected var addIntercept: Boolean = false

  protected var validateData: Boolean = true

  /**
   * Set if the algorithm should add an intercept. Default false.
   * We set the default to false because adding the intercept will cause memory allocation.
   */
  def setIntercept(addIntercept: Boolean): this.type = {
    this.addIntercept = addIntercept
    this
  }

  /**
   * Run the algorithm with the configured parameters on an input
   * RDD of LabeledPoint entries.
   */
  def run(input: RDD[(K,LabeledPoint)]) (implicit tag:ClassTag[K]): Map[K,M] = {
    // get the number of features for each group
    val groupNumFeatures = input.combineByKey[Int](
      (x : LabeledPoint) => x.features.size,
      (x : Int,y :LabeledPoint) => x,
      (x : Int,y : Int) => x
    ).collect.toMap

    val group_initialWeights = input.keys.distinct.collect.map( x =>
      (x,Vectors.dense(new Array[Double](groupNumFeatures(x))))
    ).toMap

    run(input, group_initialWeights)
  }

  /** Prepends one to the input vector. */
  private def prependOne(vector: Vector): Vector = {
    val vector1 = vector.toBreeze match {
      case dv: BDV[Double] => BDV.vertcat(BDV.ones[Double](1), dv)
      case sv: BSV[Double] => BSV.vertcat(new BSV[Double](Array(0), Array(1.0), 1), sv)
      case v: Any => throw new IllegalArgumentException("Do not support vector type " + v.getClass)
    }
    Vectors.fromBreeze(vector1)
  }

  /**
   * Run the algorithm with the configured parameters on an input RDD
   * of LabeledPoint entries starting from the initial weights provided.
   */
  def run(input: RDD[(K,LabeledPoint)], initialWeights: Map[K,Vector]): Map[K,M] = {

    // Check the data properties before running the optimizer
    if (validateData && !validators.forall(func => func(input.map(_._2)))) {
      throw new SparkException("Input validation failed.")
    }

    // Prepend an extra variable consisting of all 1.0's for the intercept.
    val data = if (addIntercept) {
      input.map(labeledPoint =>
        (labeledPoint._1, (labeledPoint._2.label, prependOne(labeledPoint._2.features)))
      )
    } else {
      input.map(labeledPoint =>
        (labeledPoint._1, (labeledPoint._2.label, labeledPoint._2.features))
      )
    }

    val initialWeightsWithIntercept = if (addIntercept) {
      initialWeights.map( x=> (x._1, prependOne(x._2)))
    } else {
      initialWeights
    }

    val weightsWithInterceptSet = optimizer.optimize(data, initialWeightsWithIntercept)
    val intercepts = weightsWithInterceptSet.map( x => {
      (x._1, if (addIntercept) x._2(0) else 0.0)
    })

    val weights = weightsWithInterceptSet.map( w => {
      if (addIntercept) {
        (w._1, Vectors.dense(w._2.toArray.slice(1, w._2.size)))
      } else {
        w
      }
    })

    weights.map( x => {
      (x._1, createModel(x._2, intercepts(x._1)))
    })
  }
}


