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

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors

class LabeledPointSuite extends SparkFunSuite {

  test("parse labeled points") {
    val points = Seq(
      LabeledPoint(1.0, Vectors.dense(1.0, 0.0)),
      LabeledPoint(0.0, Vectors.sparse(2, Array(1), Array(-1.0))))
    points.foreach { p =>
      assert(p === LabeledPoint.parse(p.toString))
    }
  }

  test("parse labeled points with whitespaces") {
    val point = LabeledPoint.parse("(0.0, [1.0, 2.0])")
    assert(point === LabeledPoint(0.0, Vectors.dense(1.0, 2.0)))
  }

  test("parse labeled points with v0.9 format") {
    val point = LabeledPoint.parse("1.0,1.0 0.0 -2.0")
    assert(point === LabeledPoint(1.0, Vectors.dense(1.0, 0.0, -2.0)))
  }
}
