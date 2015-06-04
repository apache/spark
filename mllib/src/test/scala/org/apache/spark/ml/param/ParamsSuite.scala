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

package org.apache.spark.ml.param

import org.apache.spark.SparkFunSuite

class ParamsSuite extends SparkFunSuite {

  test("param") {
    val solver = new TestParams()
    val uid = solver.uid
    import solver.{maxIter, inputCol}

    assert(maxIter.name === "maxIter")
    assert(maxIter.doc === "maximum number of iterations (>= 0)")
    assert(maxIter.parent === uid)
    assert(maxIter.toString === s"${uid}__maxIter")
    assert(!maxIter.isValid(-1))
    assert(maxIter.isValid(0))
    assert(maxIter.isValid(1))

    solver.setMaxIter(5)
    assert(solver.explainParam(maxIter) ===
      "maxIter: maximum number of iterations (>= 0) (default: 10, current: 5)")

    assert(inputCol.toString === s"${uid}__inputCol")

    intercept[IllegalArgumentException] {
      solver.setMaxIter(-1)
    }
  }

  test("param pair") {
    val solver = new TestParams()
    import solver.maxIter

    val pair0 = maxIter -> 5
    val pair1 = maxIter.w(5)
    val pair2 = ParamPair(maxIter, 5)
    for (pair <- Seq(pair0, pair1, pair2)) {
      assert(pair.param.eq(maxIter))
      assert(pair.value === 5)
    }
    intercept[IllegalArgumentException] {
      val pair = maxIter -> -1
    }
  }

  test("param map") {
    val solver = new TestParams()
    import solver.{maxIter, inputCol}

    val map0 = ParamMap.empty

    assert(!map0.contains(maxIter))
    map0.put(maxIter, 10)
    assert(map0.contains(maxIter))
    assert(map0(maxIter) === 10)
    intercept[IllegalArgumentException] {
      map0.put(maxIter, -1)
    }

    assert(!map0.contains(inputCol))
    intercept[NoSuchElementException] {
      map0(inputCol)
    }
    map0.put(inputCol -> "input")
    assert(map0.contains(inputCol))
    assert(map0(inputCol) === "input")

    val map1 = map0.copy
    val map2 = ParamMap(maxIter -> 10, inputCol -> "input")
    val map3 = new ParamMap()
      .put(maxIter, 10)
      .put(inputCol, "input")
    val map4 = ParamMap.empty ++ map0
    val map5 = ParamMap.empty
    map5 ++= map0

    for (m <- Seq(map1, map2, map3, map4, map5)) {
      assert(m.contains(maxIter))
      assert(m(maxIter) === 10)
      assert(m.contains(inputCol))
      assert(m(inputCol) === "input")
    }
  }

  test("params") {
    val solver = new TestParams()
    import solver.{maxIter, inputCol}

    val params = solver.params
    assert(params.length === 2)
    assert(params(0).eq(inputCol), "params must be ordered by name")
    assert(params(1).eq(maxIter))

    assert(!solver.isSet(maxIter))
    assert(solver.isDefined(maxIter))
    assert(solver.getMaxIter === 10)
    solver.setMaxIter(100)
    assert(solver.isSet(maxIter))
    assert(solver.getMaxIter === 100)
    assert(!solver.isSet(inputCol))
    assert(!solver.isDefined(inputCol))
    intercept[NoSuchElementException](solver.getInputCol)

    assert(solver.explainParam(maxIter) ===
      "maxIter: maximum number of iterations (>= 0) (default: 10, current: 100)")
    assert(solver.explainParams() ===
      Seq(inputCol, maxIter).map(solver.explainParam).mkString("\n"))

    assert(solver.getParam("inputCol").eq(inputCol))
    assert(solver.getParam("maxIter").eq(maxIter))
    assert(solver.hasParam("inputCol"))
    assert(!solver.hasParam("abc"))
    intercept[NoSuchElementException] {
      solver.getParam("abc")
    }

    intercept[IllegalArgumentException] {
      solver.validateParams()
    }
    solver.copy(ParamMap(inputCol -> "input")).validateParams()
    solver.setInputCol("input")
    assert(solver.isSet(inputCol))
    assert(solver.isDefined(inputCol))
    assert(solver.getInputCol === "input")
    solver.validateParams()
    intercept[IllegalArgumentException] {
      ParamMap(maxIter -> -10)
    }
    intercept[IllegalArgumentException] {
      solver.setMaxIter(-10)
    }

    solver.clearMaxIter()
    assert(!solver.isSet(maxIter))

    val copied = solver.copy(ParamMap(solver.maxIter -> 50))
    assert(copied.uid === solver.uid)
    assert(copied.getInputCol === solver.getInputCol)
    assert(copied.getMaxIter === 50)
  }

  test("ParamValidate") {
    val alwaysTrue = ParamValidators.alwaysTrue[Int]
    assert(alwaysTrue(1))

    val gt1Int = ParamValidators.gt[Int](1)
    assert(!gt1Int(1) && gt1Int(2))
    val gt1Double = ParamValidators.gt[Double](1)
    assert(!gt1Double(1.0) && gt1Double(1.1))

    val gtEq1Int = ParamValidators.gtEq[Int](1)
    assert(!gtEq1Int(0) && gtEq1Int(1))
    val gtEq1Double = ParamValidators.gtEq[Double](1)
    assert(!gtEq1Double(0.9) && gtEq1Double(1.0))

    val lt1Int = ParamValidators.lt[Int](1)
    assert(lt1Int(0) && !lt1Int(1))
    val lt1Double = ParamValidators.lt[Double](1)
    assert(lt1Double(0.9) && !lt1Double(1.0))

    val ltEq1Int = ParamValidators.ltEq[Int](1)
    assert(ltEq1Int(1) && !ltEq1Int(2))
    val ltEq1Double = ParamValidators.ltEq[Double](1)
    assert(ltEq1Double(1.0) && !ltEq1Double(1.1))

    val inRange02IntInclusive = ParamValidators.inRange[Int](0, 2)
    assert(inRange02IntInclusive(0) && inRange02IntInclusive(1) && inRange02IntInclusive(2) &&
      !inRange02IntInclusive(-1) && !inRange02IntInclusive(3))
    val inRange02IntExclusive =
      ParamValidators.inRange[Int](0, 2, lowerInclusive = false, upperInclusive = false)
    assert(!inRange02IntExclusive(0) && inRange02IntExclusive(1) && !inRange02IntExclusive(2))

    val inRange02DoubleInclusive = ParamValidators.inRange[Double](0, 2)
    assert(inRange02DoubleInclusive(0) && inRange02DoubleInclusive(1) &&
      inRange02DoubleInclusive(2) &&
      !inRange02DoubleInclusive(-0.1) && !inRange02DoubleInclusive(2.1))
    val inRange02DoubleExclusive =
      ParamValidators.inRange[Double](0, 2, lowerInclusive = false, upperInclusive = false)
    assert(!inRange02DoubleExclusive(0) && inRange02DoubleExclusive(1) &&
      !inRange02DoubleExclusive(2))

    val inArray = ParamValidators.inArray[Int](Array(1, 2))
    assert(inArray(1) && inArray(2) && !inArray(0))
  }
}

object ParamsSuite extends SparkFunSuite {

  /**
   * Checks common requirements for [[Params.params]]: 1) number of params; 2) params are ordered
   * by names; 3) param parent has the same UID as the object's UID; 4) param name is the same as
   * the param method name.
   */
  def checkParams(obj: Params, expectedNumParams: Int): Unit = {
    val params = obj.params
    require(params.length === expectedNumParams,
      s"Expect $expectedNumParams params but got ${params.length}: ${params.map(_.name).toSeq}.")
    val paramNames = params.map(_.name)
    require(paramNames === paramNames.sorted)
    params.foreach { p =>
      assert(p.parent === obj.uid)
      assert(obj.getParam(p.name) === p)
    }
  }
}
