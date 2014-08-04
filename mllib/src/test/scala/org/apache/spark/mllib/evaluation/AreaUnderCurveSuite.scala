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

package org.apache.spark.mllib.evaluation

import org.scalatest.FunSuite

import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class AreaUnderCurveSuite extends FunSuite with LocalSparkContext {
  test("auc computation") {
    val curve = Seq((0.0, 0.0), (1.0, 1.0), (2.0, 3.0), (3.0, 0.0))
    val auc = 4.0
    assert(AreaUnderCurve.of(curve) ~== auc absTol 1E-5)
    val rddCurve = sc.parallelize(curve, 2)
    assert(AreaUnderCurve.of(rddCurve) ~== auc absTol 1E-5)
  }

  test("auc of an empty curve") {
    val curve = Seq.empty[(Double, Double)]
    assert(AreaUnderCurve.of(curve) ~== 0.0 absTol 1E-5)
    val rddCurve = sc.parallelize(curve, 2)
    assert(AreaUnderCurve.of(rddCurve) ~== 0.0 absTol 1E-5)
  }

  test("auc of a curve with a single point") {
    val curve = Seq((1.0, 1.0))
    assert(AreaUnderCurve.of(curve) ~== 0.0 absTol 1E-5)
    val rddCurve = sc.parallelize(curve, 2)
    assert(AreaUnderCurve.of(rddCurve) ~== 0.0 absTol 1E-5)
  }
}
