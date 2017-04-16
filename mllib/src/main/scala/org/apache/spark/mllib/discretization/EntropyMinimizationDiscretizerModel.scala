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

package org.apache.spark.mllib.discretization

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * This class provides the methods to discretize data with the given thresholds.
 * @param thresholds Thresholds used to discretize.
 */
class EntropyMinimizationDiscretizerModel (val thresholds: Map[Int, Seq[Double]])
  extends DiscretizerModel[LabeledPoint] with Serializable {

  /**
   * Discretizes values for the given data set using the model trained.
   *
   * @param data Data point to discretize.
   * @return Data point with values discretized
   */
  override def discretize(data: LabeledPoint): LabeledPoint = {
    val newValues = data.features.zipWithIndex.map({ case (value, i) =>
      if (this.thresholds.keySet contains i) {
        assignDiscreteValue(value, thresholds(i))
      } else {
        value
      }
    })
    LabeledPoint(data.label, newValues)
  }

  /**
   * Discretizes values for the given data set using the model trained.
   *
   * @param data RDD representing data points to discretize.
   * @return RDD with values discretized
   */
  override def discretize(data: RDD[LabeledPoint]): RDD[LabeledPoint] = {
    val bc_thresholds = data.context.broadcast(this.thresholds)

    // applies thresholds to discretize every continuous feature
    data.map({ case LabeledPoint(label, values) =>
      val newValues = values.zipWithIndex.map({ case (value, i) =>
        if (bc_thresholds.value.keySet contains i) {
          assignDiscreteValue(value, bc_thresholds.value(i))
        } else {
          value
        }
      })
      LabeledPoint(label, newValues)
    })
  }


  /**
   * Discretizes a value with a set of intervals.
   *
   * @param value The value to be discretized
   * @param thresholds Thresholds used to asign a discrete value
   */
  private def assignDiscreteValue(value: Double, thresholds: Seq[Double]) = {
    var aux = thresholds.zipWithIndex
    while (value > aux.head._1) aux = aux.tail
    aux.head._2
  }

}
