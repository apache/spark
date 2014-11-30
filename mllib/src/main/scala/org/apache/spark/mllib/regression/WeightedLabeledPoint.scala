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

package org.apache.spark.mllib.regression

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.beans.BeanInfo

object WeightedLabeledPointConversions {
  implicit def labeledPointToWeightedLabeledPoint(
      labeledPoint: LabeledPoint): WeightedLabeledPoint = {
    WeightedLabeledPoint(labeledPoint.label, labeledPoint.features)
  }

  implicit def labeledPointRDDToWeightedLabeledPointRDD(
      rdd: RDD[LabeledPoint]): RDD[WeightedLabeledPoint] = {
    rdd.map(lp => WeightedLabeledPoint(lp.label, lp.features))
  }
}

/**
 * Class that represents the features and labels of a data point with associated weight
 *
 * @param label Label for this data point.
 * @param features List of features for this data point.
 * @param weight Weight of the data point. Defaults to 1.
 */
@BeanInfo
case class WeightedLabeledPoint(label: Double, features: Vector, weight: Double = 1)
