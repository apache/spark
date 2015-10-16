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

import org.json4s.{DefaultFormats, JValue}

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

@Since("0.8.0")
@Experimental
trait RegressionModel extends Serializable {
  /**
   * Predict values for the given data set using the model trained.
   *
   * @param testData RDD representing data points to be predicted
   * @return RDD[Double] where each entry contains the corresponding prediction
   *
   */
  @Since("1.0.0")
  def predict(testData: RDD[Vector]): RDD[Double]

  /**
   * Predict values for a single data point using the model trained.
   *
   * @param testData array representing a single data point
   * @return Double prediction from the trained model
   *
   */
  @Since("1.0.0")
  def predict(testData: Vector): Double

  /**
   * Predict values for examples stored in a JavaRDD.
   * @param testData JavaRDD representing data points to be predicted
   * @return a JavaRDD[java.lang.Double] where each entry contains the corresponding prediction
   *
   */
  @Since("1.0.0")
  def predict(testData: JavaRDD[Vector]): JavaRDD[java.lang.Double] =
    predict(testData.rdd).toJavaRDD().asInstanceOf[JavaRDD[java.lang.Double]]
}

private[mllib] object RegressionModel {

  /**
   * Helper method for loading GLM regression model metadata.
   * @return numFeatures
   */
  def getNumFeatures(metadata: JValue): Int = {
    implicit val formats = DefaultFormats
    (metadata \ "numFeatures").extract[Int]
  }
}
