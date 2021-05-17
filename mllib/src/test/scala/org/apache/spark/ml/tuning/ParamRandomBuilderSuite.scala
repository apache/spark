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

import org.scalatest.matchers.must.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param._

class ParamRandomBuilderSuite extends SparkFunSuite with ScalaCheckDrivenPropertyChecks
  with Matchers {

  val solver = new TestParams() {
    private val randomColName = "randomVal"
    val DummyDoubleParam = new DoubleParam(this, randomColName, "doc")
    val DummyFloatParam = new FloatParam(this, randomColName, "doc")
    val DummyIntParam = new IntParam(this, randomColName, "doc")
  }
  import solver._

  val DoubleLimits: Limits[Double] = Limits(1d, 100d)
  val FloatLimits: Limits[Float] = Limits(1f, 100f)
  val IntLimits: Limits[Int] = Limits(1, 100)
  val nRandoms: Int = 5

  // Java API

  test("Java API random Double linear params mixed with fixed values") {
    checkRangeAndCardinality(
      _.addRandom(DummyDoubleParam, DoubleLimits.x, DoubleLimits.y, nRandoms),
      DoubleLimits,
      DummyDoubleParam)
  }

  test("Java API random Double log10 params mixed with fixed values") {
    checkRangeAndCardinality(
      _.addLog10Random(DummyDoubleParam, DoubleLimits.x, DoubleLimits.y, nRandoms),
      DoubleLimits,
      DummyDoubleParam)
  }

  test("Java API random Float linear params mixed with fixed values") {
    checkRangeAndCardinality(
      _.addRandom(DummyFloatParam, FloatLimits.x, FloatLimits.y, nRandoms),
      FloatLimits,
      DummyFloatParam)
  }

  test("Java API random Float log10 params mixed with fixed values") {
    checkRangeAndCardinality(
      _.addLog10Random(DummyFloatParam, FloatLimits.x, FloatLimits.y, nRandoms),
      FloatLimits,
      DummyFloatParam)
  }

  test("Java API random Int linear params mixed with fixed values") {
    checkRangeAndCardinality(
      _.addRandom(DummyIntParam, IntLimits.x, IntLimits.y, nRandoms),
      IntLimits,
      DummyIntParam)
  }

  test("Java API random Int log10 params mixed with fixed values") {
    checkRangeAndCardinality(
      _.addLog10Random(DummyIntParam, IntLimits.x, IntLimits.y, nRandoms),
      IntLimits,
      DummyIntParam)
  }

  // Scala API

  test("random linear params mixed with fixed values") {
    import RandomRanges._
    checkRangeAndCardinality(_.addRandom(DummyDoubleParam, DoubleLimits, nRandoms),
      DoubleLimits,
      DummyDoubleParam)
  }

  test("random log10 params mixed with fixed values") {
    import RandomRanges._
    checkRangeAndCardinality(_.addLog10Random(DummyDoubleParam, DoubleLimits, nRandoms),
      DoubleLimits,
      DummyDoubleParam)
  }

  def checkRangeAndCardinality[T: Numeric](addFn: ParamRandomBuilder => ParamRandomBuilder,
                               lim: Limits[T],
                               randomCol: Param[T]): Unit = {
    val maxIterations: Int = 10
    val basedOn: Array[ParamPair[_]] = Array(maxIter -> maxIterations)
    val inputCols: Array[String] = Array("input0", "input1")
    val ops: Numeric[T] = implicitly[Numeric[T]]

    val builder: ParamRandomBuilder = new ParamRandomBuilder()
      .baseOn(basedOn: _*)
      .addGrid(inputCol, inputCols)
    val paramMap: Array[ParamMap] = addFn(builder).build()
    assert(paramMap.length == inputCols.length * nRandoms * basedOn.length)
    paramMap.foreach { m: ParamMap =>
      assert(m(maxIter) == maxIterations)
      assert(inputCols contains  m(inputCol))
      assert(ops.gteq(m(randomCol), lim.x))
      assert(ops.lteq(m(randomCol), lim.y))
    }
  }

}
