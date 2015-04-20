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

import org.scalatest.FunSuite

class ParamsSuite extends FunSuite {

  test("param") {
    val solver = new TestParams()
    import solver.{maxIter, inputCol}

    assert(maxIter.name === "maxIter")
    assert(maxIter.doc === "max number of iterations")
    assert(maxIter.parent.eq(solver))
    assert(maxIter.toString === "maxIter: max number of iterations (default: 10)")

    solver.setMaxIter(5)
    assert(maxIter.toString === "maxIter: max number of iterations (default: 10, current: 5)")

    assert(inputCol.toString === "inputCol: input column name (undefined)")
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
  }

  test("param map") {
    val solver = new TestParams()
    import solver.{maxIter, inputCol}

    val map0 = ParamMap.empty

    assert(!map0.contains(maxIter))
    map0.put(maxIter, 10)
    assert(map0.contains(maxIter))
    assert(map0(maxIter) === 10)

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

    assert(solver.explainParams() === Seq(inputCol, maxIter).mkString("\n"))

    assert(solver.getParam("inputCol").eq(inputCol))
    assert(solver.getParam("maxIter").eq(maxIter))
    intercept[NoSuchElementException] {
      solver.getParam("abc")
    }

    intercept[IllegalArgumentException] {
      solver.validate()
    }
    solver.validate(ParamMap(inputCol -> "input"))
    solver.setInputCol("input")
    assert(solver.isSet(inputCol))
    assert(solver.isDefined(inputCol))
    assert(solver.getInputCol === "input")
    solver.validate()
    intercept[IllegalArgumentException] {
      solver.validate(ParamMap(maxIter -> -10))
    }
    solver.setMaxIter(-10)
    intercept[IllegalArgumentException] {
      solver.validate()
    }

    solver.clearMaxIter()
    assert(!solver.isSet(maxIter))
  }
}
