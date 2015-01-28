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

package org.apache.spark.mllib.neuralNetwork

import org.scalatest.{Matchers, FunSuite}
import org.apache.spark.mllib.util.MnistDatasetSuite

class StackedRBMSuite extends FunSuite with MnistDatasetSuite with Matchers {

  ignore("StackedRBM") {
    val (data, numVisible) = mnistTrainDataset(5000)
    data.cache()
    val topology = Array(numVisible, 300, 300, 500)
    val stackedRBM = StackedRBM.train(data.map(_._1), 23, 1200, topology, 0.01, 0.1, 0.0)
  }

}
