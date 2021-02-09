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

package org.apache.spark.ml.tuning

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.{Param, ParamMap, ParamPair, TestParams}

import org.scalatest.matchers.must.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class ParamRandomBuilderSuite extends SparkFunSuite with ScalaCheckDrivenPropertyChecks
  with Matchers {

  val solver = new TestParams() {
    val randomCol: Param[Double] = new Param[Double](this, "randomVal", "randomly generated value")
  }
  import solver.{inputCol, maxIter, randomCol}

  test("random params mixed with fixed values") {
    import RandomRanges._
    val maxIterations: Int = 10
    val basedOn: Array[ParamPair[_]] = Array(maxIter -> maxIterations)
    val inputCols: Array[String] = Array("input0", "input1")
    val limit: Limits[Double] = Limits(0d, 100d)
    val nRandoms: Int = 5
    val paramMap: Array[ParamMap] = new ParamRandomBuilder()
      .baseOn(basedOn: _*)
      .addGrid(inputCol, inputCols)
      .addRandom(randomCol, limit, nRandoms)
      .build()
    assert(paramMap.length == inputCols.length * nRandoms * basedOn.length)
    paramMap.foreach { m: ParamMap =>
      assert(m(maxIter) == maxIterations)
      assert(inputCols contains  m(inputCol))
      assert(m(randomCol) >= limit.x && m(randomCol) <= limit.y)
    }
  }

}
