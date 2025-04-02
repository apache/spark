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

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.{Estimator, Transformer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.util.MyParams
import org.apache.spark.sql.Dataset

class ParamsSuite extends SparkFunSuite {

  test("json encode/decode") {
    val dummy = new Params {
      override def copy(extra: ParamMap): Params = defaultCopy(extra)

      override val uid: String = "dummy"
    }

    { // BooleanParam
      val param = new BooleanParam(dummy, "name", "doc")
      for (value <- Seq(true, false)) {
        val json = param.jsonEncode(value)
        assert(param.jsonDecode(json) === value)
      }
    }

    { // IntParam
      val param = new IntParam(dummy, "name", "doc")
      for (value <- Seq(Int.MinValue, -1, 0, 1, Int.MaxValue)) {
        val json = param.jsonEncode(value)
        assert(param.jsonDecode(json) === value)
      }
    }

    { // LongParam
      val param = new LongParam(dummy, "name", "doc")
      for (value <- Seq(Long.MinValue, -1L, 0L, 1L, Long.MaxValue)) {
        val json = param.jsonEncode(value)
        assert(param.jsonDecode(json) === value)
      }
    }

    { // FloatParam
      val param = new FloatParam(dummy, "name", "doc")
      for (value <- Seq(Float.NaN, Float.NegativeInfinity, Float.MinValue, -1.0f, -0.5f, 0.0f,
        Float.MinPositiveValue, 0.5f, 1.0f, Float.MaxValue, Float.PositiveInfinity)) {
        val json = param.jsonEncode(value)
        val decoded = param.jsonDecode(json)
        if (value.isNaN) {
          assert(decoded.isNaN)
        } else {
          assert(decoded === value)
        }
      }
    }

    { // DoubleParam
      val param = new DoubleParam(dummy, "name", "doc")
      for (value <- Seq(Double.NaN, Double.NegativeInfinity, Double.MinValue, -1.0, -0.5, 0.0,
          Double.MinPositiveValue, 0.5, 1.0, Double.MaxValue, Double.PositiveInfinity)) {
        val json = param.jsonEncode(value)
        val decoded = param.jsonDecode(json)
        if (value.isNaN) {
          assert(decoded.isNaN)
        } else {
          assert(decoded === value)
        }
      }
    }

    { // Param[String]
      val param = new Param[String](dummy, "name", "doc")
      // Currently we do not support null.
      for (value <- Seq("", "1", "abc", "quote\"", "newline\n")) {
        val json = param.jsonEncode(value)
        assert(param.jsonDecode(json) === value)
      }
    }

    { // Param[Vector]
      val param = new Param[Vector](dummy, "name", "doc")
      val values = Seq(
        Vectors.dense(Array.empty[Double]),
        Vectors.dense(0.0, 2.0),
        Vectors.sparse(0, Array.empty, Array.empty),
        Vectors.sparse(2, Array(1), Array(2.0)))
      for (value <- values) {
        val json = param.jsonEncode(value)
        assert(param.jsonDecode(json) === value)
      }
    }

    { // IntArrayParam
      val param = new IntArrayParam(dummy, "name", "doc")
      val values: Seq[Array[Int]] = Seq(
        Array(),
        Array(1),
        Array(Int.MinValue, 0, Int.MaxValue))
      for (value <- values) {
        val json = param.jsonEncode(value)
        assert(param.jsonDecode(json) === value)
      }
    }

    { // DoubleArrayParam
      val param = new DoubleArrayParam(dummy, "name", "doc")
      val values: Seq[Array[Double]] = Seq(
        Array(),
        Array(1.0),
        Array(Double.NaN, Double.NegativeInfinity, Double.MinValue, -1.0, 0.0,
          Double.MinPositiveValue, 1.0, Double.MaxValue, Double.PositiveInfinity))
      for (value <- values) {
        val json = param.jsonEncode(value)
        val decoded = param.jsonDecode(json)
        assert(decoded.length === value.length)
        decoded.zip(value).foreach { case (actual, expected) =>
          if (expected.isNaN) {
            assert(actual.isNaN)
          } else {
            assert(actual === expected)
          }
        }
      }
    }

    { // DoubleArrayArrayParam
      val param = new DoubleArrayArrayParam(dummy, "name", "doc")
      val values: Seq[Array[Array[Double]]] = Seq(
        Array(Array()),
        Array(Array(1.0)),
        Array(Array(1.0), Array(2.0)),
        Array(
          Array(Double.NaN, Double.NegativeInfinity, Double.MinValue, -1.0, 0.0,
            Double.MinPositiveValue, 1.0, Double.MaxValue, Double.PositiveInfinity),
          Array(Double.MaxValue, Double.PositiveInfinity, Double.MinPositiveValue, 1.0,
            Double.NaN, Double.NegativeInfinity, Double.MinValue, -1.0, 0.0)
        ))

      for (value <- values) {
        val json = param.jsonEncode(value)
        val decoded = param.jsonDecode(json)
        assert(decoded.length === value.length)
        decoded.zip(value).foreach { case (actualArray, expectedArray) =>
          assert(actualArray.length === expectedArray.length)
          actualArray.zip(expectedArray).foreach { case (actual, expected) =>
            if (expected.isNaN) {
              assert(actual.isNaN)
            } else {
              assert(actual === expected)
            }
          }
        }
      }
    }

    { // StringArrayParam
      val param = new StringArrayParam(dummy, "name", "doc")
      val values: Seq[Array[String]] = Seq(
        Array(),
        Array(""),
        Array("", "1", "abc", "quote\"", "newline\n"))
      for (value <- values) {
        val json = param.jsonEncode(value)
        assert(param.jsonDecode(json) === value)
      }
    }
  }

  test("param") {
    val solver = new TestParams()
    val uid = solver.uid
    import solver.{inputCol, maxIter}

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

    intercept[java.util.NoSuchElementException] {
      solver.getOrDefault(solver.handleInvalid)
    }

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
    import solver.{inputCol, maxIter}

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
    import solver.{handleInvalid, inputCol, maxIter}

    val params = solver.params
    assert(params.length === 3)
    assert(params(0).eq(handleInvalid), "params must be ordered by name")
    assert(params(1).eq(inputCol), "params must be ordered by name")
    assert(params(2).eq(maxIter))

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
      Seq(handleInvalid, inputCol, maxIter).map(solver.explainParam).mkString("\n"))

    assert(solver.getParam("inputCol").eq(inputCol))
    assert(solver.getParam("maxIter").eq(maxIter))
    assert(solver.hasParam("inputCol"))
    assert(!solver.hasParam("abc"))
    intercept[NoSuchElementException] {
      solver.getParam("abc")
    }

    solver.setInputCol("input")
    assert(solver.isSet(inputCol))
    assert(solver.isDefined(inputCol))
    assert(solver.getInputCol === "input")
    intercept[IllegalArgumentException] {
      ParamMap(maxIter -> -10)
    }
    intercept[IllegalArgumentException] {
      solver.setMaxIter(-10)
    }

    solver.clearMaxIter()
    assert(!solver.isSet(maxIter))

    // Re-set and clear maxIter using the generic clear API
    solver.setMaxIter(10)
    solver.clear(maxIter)
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

    val arrayLengthGt = ParamValidators.arrayLengthGt[Int](2.0)
    assert(arrayLengthGt(Array(0, 1, 2)) && !arrayLengthGt(Array(0, 1)))
  }

  test("Params.copyValues") {
    val t = new TestParams()
    val t2 = t.copy(ParamMap.empty)
    assert(!t2.isSet(t2.maxIter))
    val t3 = t.copy(ParamMap(t.maxIter -> 20))
    assert(t3.isSet(t3.maxIter))
  }

  test("Filtering ParamMap") {
    val params1 = new MyParams("my_params1")
    val params2 = new MyParams("my_params2")
    val paramMap = ParamMap(
      params1.intParam -> 1,
      params2.intParam -> 1,
      params1.doubleParam -> 0.2,
      params2.doubleParam -> 0.2)
    val filteredParamMap = paramMap.filter(params1)

    assert(filteredParamMap.size === 2)
    filteredParamMap.toSeq.foreach {
      case ParamPair(p, _) =>
        assert(p.parent === params1.uid)
    }

    // At the previous implementation of ParamMap#filter,
    // mutable.Map#filterKeys was used internally but
    // the return type of the method is not serializable (see SI-6654).
    // Now mutable.Map#filter is used instead of filterKeys and the return type is serializable.
    // So let's ensure serializability.
    val objOut = new ObjectOutputStream(new ByteArrayOutputStream())
    objOut.writeObject(filteredParamMap)
  }
}

object ParamsSuite extends SparkFunSuite {

  /**
   * Checks common requirements for `Params.params`:
   *   - params are ordered by names
   *   - param parent has the same UID as the object's UID
   *   - param name is the same as the param method name
   *   - obj.copy should return the same type as the obj
   */
  def checkParams(obj: Params): Unit = {
    val clazz = obj.getClass

    val params = obj.params
    val paramNames = params.map(_.name)
    require(paramNames === paramNames.sorted, "params must be ordered by names")
    params.foreach { p =>
      assert(p.parent === obj.uid)
      assert(obj.getParam(p.name) === p)
      // TODO: Check that setters return self, which needs special handling for generic types.
    }

    val copyMethod = clazz.getMethod("copy", classOf[ParamMap])
    val copyReturnType = copyMethod.getReturnType
    require(copyReturnType === obj.getClass,
      s"${clazz.getName}.copy should return ${clazz.getName} instead of ${copyReturnType.getName}.")
  }

  /**
   * Checks that the class throws an exception in case multiple exclusive params are set.
   * The params to be checked are passed as arguments with their value.
   */
  def testExclusiveParams(
      model: Params,
      dataset: Dataset[_],
      paramsAndValues: (String, Any)*): Unit = {
    val m = model.copy(ParamMap.empty)
    paramsAndValues.foreach { case (paramName, paramValue) =>
      m.set(m.getParam(paramName), paramValue)
    }
    intercept[IllegalArgumentException] {
      m match {
        case t: Transformer => t.transform(dataset)
        case e: Estimator[_] => e.fit(dataset)
      }
    }
  }
}
