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

package org.apache.spark.mllib.api.python

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

/**
 * A Wrapper of MatrixFactorizationModel to provide helper method for Python.
 */
private[python] class MatrixFactorizationModelWrapper(model: MatrixFactorizationModel)
  extends MatrixFactorizationModel(model.rank, model.userFeatures, model.productFeatures) {

  def predict(userAndProducts: JavaRDD[Array[Any]]): RDD[Rating] =
    predict(SerDe.asTupleRDD(userAndProducts.rdd))

  def getUserFeatures: RDD[Array[Any]] = {
    SerDe.fromTuple2RDD(userFeatures.asInstanceOf[RDD[(Any, Any)]])
  }

  def getProductFeatures: RDD[Array[Any]] = {
    SerDe.fromTuple2RDD(productFeatures.asInstanceOf[RDD[(Any, Any)]])
  }
}
