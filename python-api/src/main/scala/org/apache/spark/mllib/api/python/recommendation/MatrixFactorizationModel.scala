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

package org.apache.spark.mllib.api.python.recommendation

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.api.python.PythonMLLibAPI

class MatrixFactorizationModel(
    override val rank: Int,
    override val userFeatures: RDD[(Int, Array[Double])],
    override val productFeatures: RDD[(Int, Array[Double])])
  extends org.apache.spark.mllib.recommendation.MatrixFactorizationModel(rank,
    userFeatures, productFeatures) {

  /**
   * :: DeveloperApi ::
   * Predict the rating of many users for many products.
   * This is a Java stub for python predictAll()
   *
   * @param usersProductsJRDD A JavaRDD with serialized tuples (user, product)
   * @return JavaRDD of serialized Rating objects.
   */
  def predict(usersProductsJRDD: JavaRDD[Array[Byte]]): JavaRDD[Array[Byte]] = {
    val pythonAPI = new PythonMLLibAPI()
    val usersProducts = usersProductsJRDD.rdd.map(xBytes => pythonAPI.unpackTuple(xBytes))
    predict(usersProducts).map(rate => pythonAPI.serializeRating(rate))
  }

}
