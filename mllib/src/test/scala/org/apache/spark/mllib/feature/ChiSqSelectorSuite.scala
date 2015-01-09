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

package org.apache.spark.mllib.feature

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.scalatest.FunSuite

class ChiSqSelectorSuite extends FunSuite with MLlibTestSparkContext {

  lazy val labeledDiscreteData = sc.parallelize(
    Seq( new LabeledPoint(0.0, Vectors.dense(Array(8.0, 7.0, 0.0))),
      new LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 6.0))),
      new LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 8.0))),
      new LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 5.0)))
    ), 2)

  /*
   *  Contingency tables
   *  feature0 = {8.0, 0.0}
   *  class  0 1 2
   *    8.0||1|0|1|
   *    0.0||0|2|0|
   *
   *  feature1 = {7.0, 9.0}
   *  class  0 1 2
   *    7.0||1|0|0|
   *    9.0||0|2|1|
   *
   *  feature2 = {0.0, 6.0, 8.0, 5.0}
   *  class  0 1 2
   *    0.0||1|0|0|
   *    6.0||0|1|0|
   *    8.0||0|1|0|
   *    5.0||0|0|1|
   *
   *  Use chi-squared calculator from Internet
   */

  test("ChiSqSelector transform test") {
    val preFilteredData =
      Set( new LabeledPoint(0.0, Vectors.dense(Array(0.0))),
        new LabeledPoint(1.0, Vectors.dense(Array(6.0))),
        new LabeledPoint(1.0, Vectors.dense(Array(8.0))),
        new LabeledPoint(2.0, Vectors.dense(Array(5.0)))
      )
    val model = ChiSqSelector.fit(labeledDiscreteData, 1)
    val filteredData = labeledDiscreteData.map(lp =>
      new LabeledPoint(lp.label, model.transform(lp.features))).collect().toSet
    assert(filteredData == preFilteredData)
  }
}
