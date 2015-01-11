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

package org.apache.spark.mllib.util

import scala.collection.JavaConversions._
import java.lang.{Double => JDouble}

object IsotonicDataGenerator {

  /**
   * Return a Java List of ordered labeled points
   * @param labels list of labels for the data points
   * @return Java List of input.
   */
  def generateIsotonicInputAsList(labels: Array[Double]): java.util.List[(JDouble, JDouble)] = {
    seqAsJavaList(generateIsotonicInput(wrapDoubleArray(labels):_*).map(x => (new JDouble(x._1), new JDouble(x._2))))
      //.map(d => new Tuple3(new java.lang.Double(d._1), new java.lang.Double(d._2), new java.lang.Double(d._3))))
  }

  /**
   * Return an ordered sequence of labeled data points with default weights
   * @param labels list of labels for the data points
   * @return sequence of data points
   */
  def generateIsotonicInput(labels: Double*): Seq[(Double, Double, Double)] = {
    labels.zip(1 to labels.size)
      .map(point => (point._1, point._2.toDouble, 1d))
  }

  /**
   * Return an ordered sequence of labeled weighted data points
   * @param labels list of labels for the data points
   * @param weights list of weights for the data points
   * @return sequence of data points
   */
  def generateWeightedIsotonicInput(
      labels: Seq[Double],
      weights: Seq[Double]): Seq[(Double, Double, Double)] = {
    labels.zip(1 to labels.size).zip(weights)
      .map(point => (point._1._1, point._1._2.toDouble, point._2))
  }
}
