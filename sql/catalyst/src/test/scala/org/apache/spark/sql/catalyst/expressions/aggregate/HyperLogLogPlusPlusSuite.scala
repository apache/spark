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

package org.apache.spark.sql.catalyst.expressions.aggregate

import java.lang.{Double => JDouble}
import java.nio.ByteBuffer
import java.util.Random
import scala.collection.mutable
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, EmptyRow, HyperLogLogPlusPlusEvalSketch, Literal, SpecificInternalRow}
import org.apache.spark.sql.types.{BinaryType, DataType, DoubleType, FloatType, IntegerType}


class HyperLogLogPlusPlusSuite extends SparkFunSuite {

  /** Create a HLL++ instance and an input and output buffer. */
  def createEstimator(rsd: Double, dt: DataType = IntegerType):
      (HyperLogLogPlusPlus, InternalRow, InternalRow) = {
    val input = new SpecificInternalRow(Seq(dt))
    val hll = new HyperLogLogPlusPlus(new BoundReference(0, dt, true), rsd)
    val buffer = createBuffer(hll)
    (hll, input, buffer)
  }

  /** Create a HLL++ instance and an input and output buffer. */
  def createSketchEstimator(rsd: Double, dt: DataType = IntegerType):
      (HyperLogLogPlusPlusSketch, InternalRow, InternalRow) = {
    val input = new SpecificInternalRow(Seq(dt))
    val hll = new HyperLogLogPlusPlusSketch(new BoundReference(0, dt, true), rsd)
    val buffer = createBuffer(hll)
    (hll, input, buffer)
  }

  /** Create a HLL++ instance and an input and output buffer. */
  def createAggSketchEstimator(rsd: Double):
      (HyperLogLogPlusPlusAggSketch, InternalRow, InternalRow) = {
    val input = new SpecificInternalRow(Seq(BinaryType))
    val hll = new HyperLogLogPlusPlusAggSketch(new BoundReference(0, BinaryType, true), rsd)
    val buffer = createBuffer(hll)
    (hll, input, buffer)
  }

  def createBuffer(hll: HyperLogLogPlusPlusTrait): InternalRow = {
    val buffer = new SpecificInternalRow(hll.aggBufferAttributes.map(_.dataType))
    hll.initialize(buffer)
    buffer
  }

  /** Evaluate the estimate. It should be within 3*SD's of the given true rsd. */
  def evaluateEstimate(hll: HyperLogLogPlusPlus, buffer: InternalRow, cardinality: Int): Unit = {
    val estimate = hll.eval(buffer).asInstanceOf[Long].toDouble
    val error = math.abs((estimate / cardinality.toDouble) - 1.0d)
    assert(error < hll.hllppHelper.trueRsd * 3.0d, "Error should be within 3 std. errors.")
  }

  test("test invalid parameter relativeSD") {
    // `relativeSD` should be at most 39%.
    intercept[IllegalArgumentException] {
      new HyperLogLogPlusPlus(new BoundReference(0, IntegerType, true), relativeSD = 0.4)
    }

    intercept[IllegalArgumentException] {
      new HyperLogLogPlusPlusSketch(new BoundReference(0, IntegerType, true), relativeSD = 0.4)
    }

    intercept[IllegalArgumentException] {
      new HyperLogLogPlusPlusAggSketch(new BoundReference(0, IntegerType, true), relativeSD = 0.4)
    }
  }

  test("add nulls") {
    val (hll, input, buffer) = createEstimator(0.05)
    input.setNullAt(0)
    hll.update(buffer, input)
    hll.update(buffer, input)
    val estimate = hll.eval(buffer).asInstanceOf[Long]
    assert(estimate == 0L, "Nothing meaningful added; estimate should be 0.")
  }

  def testCardinalityEstimates(
      rsds: Seq[Double],
      ns: Seq[Int],
      f: Int => Int,
      c: Int => Int): Unit = {
    rsds.flatMap(rsd => ns.map(n => (rsd, n))).foreach {
      case (rsd, n) =>
        val (hll, input, buffer) = createEstimator(rsd)
        var i = 0
        while (i < n) {
          input.setInt(0, f(i))
          hll.update(buffer, input)
          i += 1
        }
        val estimate = hll.eval(buffer).asInstanceOf[Long].toDouble
        val cardinality = c(n)
        val error = math.abs((estimate / cardinality.toDouble) - 1.0d)
        assert(error < hll.hllppHelper.trueRsd * 3.0d, "Error should be within 3 std. errors.")
    }
  }

  // Additional helper class used to round-robin values between a random number of
  // sketch estimators, and then round-robin sketches between aggregators, then
  // merge aggregators together for cardinality estimation
  case class sketchEstimatorHelper(
      rsd: Double = 0.05,
      dataType: DataType = IntegerType,
      numSketches: Integer = 10,
      numAggregators: Integer = 2) {

    val generators = (0 until numSketches).map(_ => createSketchEstimator(rsd, dataType))
    val aggregators = (0 until numAggregators).map(_ => createAggSketchEstimator(rsd))

    val r = new Random()

    def applyValue(value: Any): Unit = {
      val (hll, input, buffer) = generators(r.nextInt(numSketches))
      input.update(0, value)
      hll.update(buffer, input)
    }

    def evalSketches(): Long = {
      generators.map({
        case (hll, _, buffer) => hll.eval(buffer).asInstanceOf[Array[Byte]]
      }).foreach({
        case sketch =>
          val (agghll, agginput, aggbuffer) = aggregators(r.nextInt(numAggregators))
          agginput.update(0, sketch)
          agghll.update(aggbuffer, agginput)
      })

      val (reducedHll: HyperLogLogPlusPlusAggSketch, reducedBuffer: InternalRow) =
        aggregators.map({
          case (a, _, r2) => (a, r2)
        }).reduce((t1, t2) => {
          t1._1.merge(t1._2, t2._2)
          (t1._1, t1._2)
        })

      reducedHll.eval(reducedBuffer).asInstanceOf[Long]
    }
  }

  // another cardinality estimate tester that utilizes the sketch helper above
  def testCardinalityEstimatesWithSketches(
      rsds: Seq[Double],
      ns: Seq[Int],
      f: Int => Int,
      c: Int => Int): Unit = {
    rsds.flatMap(rsd => ns.map(n => (rsd, n))).foreach {
      case (rsd, n) =>
        val helper = sketchEstimatorHelper(
          rsd,
          IntegerType,
          1 + new Random().nextInt(10),
          1 + new Random().nextInt(2))
        var i = 0
        while (i < n) {
          helper.applyValue(f(i))
          i += 1
        }
        val estimate = helper.evalSketches().toDouble
        val cardinality = c(n)
        val error = math.abs((estimate / cardinality.toDouble) - 1.0d)
        assert(error < helper.generators.head._1.hllppHelper.trueRsd * 3.0d,
          "Error should be within 3 std. errors.")
    }
  }

  test("deterministic cardinality estimation") {
    val repeats = 10
    testCardinalityEstimates(
      Seq(0.1, 0.05, 0.025, 0.01, 0.001),
      Seq(100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000).map(_ * repeats),
      i => i / repeats,
      i => i / repeats)
  }

  test("SPARK-16484: deterministic cardinality estimation with sketches") {
    val repeats = 10
    testCardinalityEstimatesWithSketches(
      Seq(0.1, 0.05, 0.025, 0.01, 0.001),
      Seq(100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000).map(_ * repeats),
      i => i / repeats,
      i => i / repeats)
  }

  test("random cardinality estimation") {
    val srng = new Random(323981238L)
    val seen = mutable.HashSet.empty[Int]
    val update = (i: Int) => {
      val value = srng.nextInt()
      seen += value
      value
    }
    val eval = (n: Int) => {
      val cardinality = seen.size
      seen.clear()
      cardinality
    }
    testCardinalityEstimates(
      Seq(0.05, 0.01),
      Seq(100, 10000, 500000),
      update,
      eval)
  }

  test("SPARK-16484: random cardinality estimation with sketches") {
    val srng = new Random(323981238L)
    val seen = mutable.HashSet.empty[Int]
    val update = (i: Int) => {
      val value = srng.nextInt()
      seen += value
      value
    }
    val eval = (n: Int) => {
      val cardinality = seen.size
      seen.clear()
      cardinality
    }
    testCardinalityEstimatesWithSketches(
      Seq(0.05, 0.01),
      Seq(100, 10000, 500000),
      update,
      eval)
  }

  // Test merging
  test("merging HLL instances") {
    val (hll, input, buffer1a) = createEstimator(0.05)
    val buffer1b = createBuffer(hll)
    val buffer2 = createBuffer(hll)

    // Create the
    // Add the lower half
    var i = 0
    while (i < 500000) {
      input.setInt(0, i)
      hll.update(buffer1a, input)
      i += 1
    }

    // Add the upper half
    i = 500000
    while (i < 1000000) {
      input.setInt(0, i)
      hll.update(buffer1b, input)
      i += 1
    }

    // Merge the lower and upper halves.
    hll.merge(buffer1a, buffer1b)

    // Create the other buffer in reverse
    i = 999999
    while (i >= 0) {
      input.setInt(0, i)
      hll.update(buffer2, input)
      i -= 1
    }

    // Check if the buffers are equal.
    assert(buffer2 == buffer1a, "Buffers should be equal")
  }

  test("SPARK-32110: add 0.0 and -0.0") {
    val (hll, input, buffer) = createEstimator(0.05, DoubleType)
    input.setDouble(0, 0.0)
    hll.update(buffer, input)
    input.setDouble(0, -0.0)
    hll.update(buffer, input)
    evaluateEstimate(hll, buffer, 1);
  }

  test("SPARK-32110: add NaN") {
    val (hll, input, buffer) = createEstimator(0.05, DoubleType)
    input.setDouble(0, Double.NaN)
    hll.update(buffer, input)
    val specialNaN = JDouble.longBitsToDouble(0x7ff1234512345678L)
    assert(JDouble.isNaN(specialNaN))
    assert(JDouble.doubleToRawLongBits(Double.NaN) != JDouble.doubleToRawLongBits(specialNaN))
    input.setDouble(0, specialNaN)
    hll.update(buffer, input)
    evaluateEstimate(hll, buffer, 1);
  }

  test("SPARK-16484: Test sketch evaluation") {
    val (hll, input, buffer) = createSketchEstimator(0.05, DoubleType)
    List(1, 1, 2, 2, 3).foreach(i => {
      input.setDouble(0, i)
      hll.update(buffer, input)
    })
    val sketch = hll.eval(buffer).asInstanceOf[Array[Byte]]
    val bb = ByteBuffer.wrap(sketch)
    assert(bb.getInt() == 1)
    assert(bb.getDouble() == 0.05)

    // unclear if this is correctly reproducing round trip spark behavior via EmptyRow input
    val approxDistinctCount = HyperLogLogPlusPlusEvalSketch(Literal(sketch)).eval(EmptyRow)
    assert(approxDistinctCount == 3)
  }

  test("SPARK-16484: Test sketch re-aggregation and evaluation") {
    val (hll1, input1, buffer1) = createSketchEstimator(0.05, DoubleType)
    List(1, 1, 2, 2, 3).foreach(i => {
      input1.setDouble(0, i)
      hll1.update(buffer1, input1)
    })
    val sketch1 = hll1.eval(buffer1).asInstanceOf[Array[Byte]]

    val (hll2, input2, buffer2) = createSketchEstimator(0.05, DoubleType)
    List(3, 3, 4, 4, 5).foreach(i => {
      input2.setDouble(0, i)
      hll2.update(buffer2, input2)
    })
    val sketch2 = hll2.eval(buffer2).asInstanceOf[Array[Byte]]

    val (agghll, agginput, aggbuffer) = createAggSketchEstimator(0.05)
    List(sketch1, sketch2).foreach(i => {
      agginput.update(0, i)
      agghll.update(aggbuffer, agginput)
    })

    val approxDistinctCount = agghll.eval(aggbuffer)
    assert(approxDistinctCount == 5)
  }

  test("Spark-16484: Test sketch functionality with edge cases") {
    // test a regular numeric type's min/max bounds
    val l1 = List(Integer.MIN_VALUE, -1000000, 0, 1000000, Integer.MAX_VALUE)
    val helper1 = sketchEstimatorHelper(dataType = IntegerType, numSketches = 2)
    l1.foreach(helper1.applyValue(_))
    assert(helper1.evalSketches() == l1.size)

    // test a floating point type's min/max bounds
    val l2 = List(Float.MinValue, -1000000f, 0f, 1000000f, Float.MaxValue)
    val helper2 = sketchEstimatorHelper(dataType = FloatType, numSketches = 2)
    l2.foreach(helper2.applyValue(_))
    assert(helper2.evalSketches() == l2.size)

    // test an empty input set
    val l3 = List()
    val helper3 = sketchEstimatorHelper(dataType = FloatType, numSketches = 2)
    l3.foreach(helper3.applyValue(_))
    assert(helper3.evalSketches() == 0)

    // test 0s
    val l4 = List(0, -0, 0.0, -0.0)
    val helper4 = sketchEstimatorHelper(dataType = DoubleType)
    l4.foreach(helper4.applyValue(_))
    assert(helper4.evalSketches() == 1)

    // test NaNs
    val specialNaN = JDouble.longBitsToDouble(0x7ff1234512345678L)
    val l5 = List(Double.NaN, Double.NaN, specialNaN, Double.NaN, specialNaN, specialNaN)
    val helper5 = sketchEstimatorHelper(dataType = DoubleType)
    l5.foreach(helper5.applyValue(_))
    assert(helper5.evalSketches() == 1)
  }

}
