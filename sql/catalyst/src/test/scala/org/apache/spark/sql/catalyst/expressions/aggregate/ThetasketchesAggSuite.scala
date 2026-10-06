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

import scala.collection.immutable.NumericRange
import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, GenericInternalRow, ThetaSketchEstimate}
import org.apache.spark.sql.catalyst.util.{ArrayData, ThetaSketchUtils}
import org.apache.spark.sql.types.{ArrayType, BinaryType, DataType, DoubleType, FloatType, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

class ThetasketchesAggSuite extends SparkFunSuite {

  def simulateUpdateMerge(
      dataType: DataType,
      input: Seq[Any],
      numSketches: Integer = 5): (Long, NumericRange[Long]) = {

    // Create a map of the agg function instances.
    val aggFunctionMap = Seq
      .tabulate(numSketches)(index => {
        val sketch = new ThetaSketchAgg(BoundReference(0, dataType, nullable = true))
        index -> (sketch, sketch.createAggregationBuffer())
      })
      .toMap

    // Randomly update agg function instances.
    input.map(value => {
      val (aggFunction, aggBuffer) = aggFunctionMap(Random.nextInt(numSketches))
      aggFunction.update(aggBuffer, InternalRow(value))
    })

    def serializeDeserialize(
        tuple: (ThetaSketchAgg, ThetaSketchState)): (ThetaSketchAgg, ThetaSketchState) = {
      val (agg, buf) = tuple
      val serialized = agg.serialize(buf)
      (agg, agg.deserialize(serialized))
    }

    // Simulate serialization -> deserialization -> merge.
    val mapValues = aggFunctionMap.values
    val (mergedAgg, UnionAggregationBuffer(mergedBuf)) =
      mapValues.tail.foldLeft(mapValues.head)((prev, cur) => {
        val (prevAgg, prevBuf) = serializeDeserialize(prev)
        val (_, curBuf) = serializeDeserialize(cur)

        (prevAgg, prevAgg.merge(prevBuf, curBuf))
      })

    val estimator = ThetaSketchEstimate(BoundReference(0, BinaryType, nullable = true))
    val estimate =
      estimator.eval(InternalRow(mergedBuf.getResult.toByteArrayCompressed)).asInstanceOf[Long]
    (
      estimate,
      mergedBuf.getResult.getLowerBound(3).toLong to mergedBuf.getResult.getUpperBound(3).toLong)
  }

  private def buildThetaSketch(keys: Seq[Int]): Array[Byte] = {
    val agg = new ThetaSketchAgg(BoundReference(0, IntegerType, nullable = false))
    val buffer = keys.foldLeft(agg.createAggregationBuffer()) { (buffer, key) =>
      agg.update(buffer, InternalRow(key))
    }
    agg.eval(buffer).asInstanceOf[Array[Byte]]
  }

  private def createIntersectionBuffer(
      agg: ThetaIntersectionAgg,
      sketches: Seq[Array[Byte]]): ThetaSketchState = {
    sketches.foldLeft(agg.createAggregationBuffer()) { (buffer, sketch) =>
      agg.update(buffer, InternalRow(sketch))
    }
  }

  private def checkIntersectionEstimate(
      agg: ThetaIntersectionAgg,
      buffer: ThetaSketchState,
      expected: Double): Unit = {
    val result = agg.eval(buffer).asInstanceOf[Array[Byte]]
    assert(result != null && result.nonEmpty)
    assert(ThetaSketchUtils.wrapCompactSketch(result, agg.prettyName).getEstimate == expected)
  }

  test("SPARK-52407: Test min/max values of supported datatypes") {
    val intRange = Integer.MIN_VALUE to Integer.MAX_VALUE by 10000000
    val (intEstimate, intEstimateRange) = simulateUpdateMerge(IntegerType, intRange)
    assert(intEstimate == intRange.size || intEstimateRange.contains(intRange.size.toLong))

    val longRange = Long.MinValue to Long.MaxValue by 1000000000000000L
    val (longEstimate, longEstimateRange) = simulateUpdateMerge(LongType, longRange)
    assert(longEstimate == longRange.size || longEstimateRange.contains(longRange.size.toLong))

    val stringRange = Seq.tabulate(1000)(i => UTF8String.fromString(Random.nextString(i + 1)))
    val (stringEstimate, stringEstimateRange) = simulateUpdateMerge(StringType, stringRange)
    assert(
      stringEstimate == stringRange.size ||
        stringEstimateRange.contains(stringRange.size.toLong))

    val binaryRange =
      Seq.tabulate(1000)(i => UTF8String.fromString(Random.nextString(i + 1)).getBytes)
    val (binaryEstimate, binaryEstimateRange) = simulateUpdateMerge(BinaryType, binaryRange)
    assert(
      binaryEstimate == binaryRange.size ||
        binaryEstimateRange.contains(binaryRange.size.toLong))

    val floatRange = (1 to 1000).map(_.toFloat)
    val (floatEstimate, floatRangeEst) = simulateUpdateMerge(FloatType, floatRange)
    assert(floatEstimate == floatRange.size || floatRangeEst.contains(floatRange.size.toLong))

    val doubleRange = (1 to 1000).map(_.toDouble)
    val (doubleEstimate, doubleRangeEst) = simulateUpdateMerge(DoubleType, doubleRange)
    assert(doubleEstimate == doubleRange.size || doubleRangeEst.contains(doubleRange.size.toLong))

    val arrayIntRange = (1 to 500).map(i => ArrayData.toArrayData(Array(i, i + 1)))
    val (arrayIntEstimate, arrayIntRangeEst) =
      simulateUpdateMerge(ArrayType(IntegerType), arrayIntRange)
    assert(
      arrayIntEstimate == arrayIntRange.size ||
        arrayIntRangeEst.contains(arrayIntRange.size.toLong))

    val arrayLongRange =
      (1 to 500).map(i => ArrayData.toArrayData(Array(i.toLong, (i + 1).toLong)))
    val (arrayLongEstimate, arrayLongRangeEst) =
      simulateUpdateMerge(ArrayType(LongType), arrayLongRange)
    assert(
      arrayLongEstimate == arrayLongRange.size ||
        arrayLongRangeEst.contains(arrayLongRange.size.toLong))
  }

  test("SPARK-52407: Test lgNomEntries results in downsampling sketches during Union") {
    // Create a sketch with larger configuration (more precise).
    val aggFunc1 = new ThetaSketchAgg(BoundReference(0, IntegerType, nullable = true), 12)
    val sketch1 = aggFunc1.createAggregationBuffer()
    (0 to 100).map(i => aggFunc1.update(sketch1, InternalRow(i)))
    val binary1 = aggFunc1.eval(sketch1)

    // Create a sketch with smaller configuration (less precise).
    val aggFunc2 = new ThetaSketchAgg(BoundReference(0, IntegerType, nullable = true), 10)
    val sketch2 = aggFunc2.createAggregationBuffer()
    (0 to 100).map(i => aggFunc2.update(sketch2, InternalRow(i)))
    val binary2 = aggFunc2.eval(sketch2)

    // Union the sketches.
    val unionAgg = new ThetaUnionAgg(BoundReference(0, BinaryType, nullable = true), 12)
    val union = unionAgg.createAggregationBuffer()
    unionAgg.update(union, InternalRow(binary1))
    unionAgg.update(union, InternalRow(binary2))
    val unionResult = unionAgg.eval(union)

    // Verify the estimate is still accurate despite different configurations
    val estimate = ThetaSketchEstimate(BoundReference(0, BinaryType, nullable = true))
      .eval(InternalRow(unionResult))
    assert(estimate.asInstanceOf[Long] >= 95 && estimate.asInstanceOf[Long] <= 105)
  }

  test("SPARK-52407: Test lgNomEntries results in downsampling sketches during intersection") {
    // Create sketch with a larger configuration (more precise).
    val aggFunc1 = new ThetaSketchAgg(BoundReference(0, IntegerType, nullable = true), 12)
    val sketch1 = aggFunc1.createAggregationBuffer()
    (0 to 150).map(i => aggFunc1.update(sketch1, InternalRow(i)))
    val binary1 = aggFunc1.eval(sketch1)

    // Create a sketch with smaller configuration (less precise).
    val aggFunc2 = new ThetaSketchAgg(BoundReference(0, IntegerType, nullable = true), 10)
    val sketch2 = aggFunc2.createAggregationBuffer()
    (50 to 200).map(i => aggFunc2.update(sketch2, InternalRow(i)))
    val binary2 = aggFunc2.eval(sketch2)

    // Intersect the sketches.
    val intersectionAgg =
      new ThetaIntersectionAgg(BoundReference(0, BinaryType, nullable = true))
    val intersection = intersectionAgg.createAggregationBuffer()
    intersectionAgg.update(intersection, InternalRow(binary1))
    intersectionAgg.update(intersection, InternalRow(binary2))
    val intersectionResult = intersectionAgg.eval(intersection)

    // Verify the estimate is still accurate despite different configurations,
    // should be around 101 (overlap from 50 to 150).
    val estimate = ThetaSketchEstimate(BoundReference(0, BinaryType, nullable = true))
      .eval(InternalRow(intersectionResult))
    assert(estimate.asInstanceOf[Long] >= 95 && estimate.asInstanceOf[Long] <= 105)
  }

  gridTest(
    "theta intersection keeps no-input partials null and returns an empty final")(
    Seq(0, 2)) { numNulls =>
    val agg = new ThetaIntersectionAgg(BoundReference(0, BinaryType, nullable = true))
    val buffer = createIntersectionBuffer(agg, Seq.fill[Array[Byte]](numNulls)(null))
    assert(agg.serialize(buffer) == null)
    assert(agg.deserialize(null) == null)
    checkIntersectionEstimate(agg, buffer, 0.0)
    assert(agg.eval(buffer).asInstanceOf[Array[Byte]].sameElements(buildThetaSketch(Seq.empty)))

    // Final evaluation must not change the untouched intermediate state.
    assert(agg.serialize(buffer) == null)
    val updated = agg.update(buffer, InternalRow(buildThetaSketch(Seq(1, 2))))
    checkIntersectionEstimate(agg, updated, 2.0)
  }

  test("theta intersection preserves null through partial merge and final merge") {
    val agg = new ThetaIntersectionAgg(BoundReference(0, BinaryType, nullable = true))
    val partial = createIntersectionBuffer(agg, Seq(null, null))
    val partialMerge = agg.merge(
      agg.createAggregationBuffer(),
      agg.deserialize(agg.serialize(partial)))
    val merged = agg.merge(partialMerge, agg.deserialize(agg.serialize(partial)))
    assert(agg.serialize(merged) == null)

    val result = agg.merge(
      agg.createAggregationBuffer(),
      agg.deserialize(agg.serialize(merged)))
    checkIntersectionEstimate(agg, result, 0.0)
  }

  gridTest("theta intersection skips no-input partials (serialized, nullFirst)")(
    Seq((false, false), (false, true), (true, false), (true, true))) {
    case (serialized, nullFirst) =>
      val agg = new ThetaIntersectionAgg(BoundReference(0, BinaryType, nullable = true))
      val untouched = createIntersectionBuffer(agg, Seq(null, null))
      val populated = createIntersectionBuffer(agg, Seq(null, buildThetaSketch(Seq(1, 2)), null))
      val partials = if (nullFirst) Seq(untouched, populated) else Seq(populated, untouched)
      val merged = partials.foldLeft(agg.createAggregationBuffer()) { (buffer, partial) =>
        val input = if (serialized) agg.deserialize(agg.serialize(partial)) else partial
        agg.merge(buffer, input)
      }
      checkIntersectionEstimate(agg, merged, 2.0)

      // A further partial-merge serialization must preserve the populated result.
      val result = agg.merge(
        agg.createAggregationBuffer(),
        agg.deserialize(agg.serialize(merged)))
      checkIntersectionEstimate(agg, result, 2.0)
  }

  test("theta intersection skips untouched objects in mergeBuffersObjects") {
    val agg = new ThetaIntersectionAgg(BoundReference(0, BinaryType, nullable = true))
    val destination = new GenericInternalRow(1)
    val incoming = new GenericInternalRow(1)
    agg.initialize(destination)
    agg.initialize(incoming)
    agg.update(destination, InternalRow(buildThetaSketch(Seq(1, 2))))
    agg.update(incoming, InternalRow(null: Array[Byte]))

    agg.mergeBuffersObjects(destination, incoming)
    val result = agg.eval(destination).asInstanceOf[Array[Byte]]
    assert(ThetaSketchUtils.wrapCompactSketch(result, agg.prettyName).getEstimate == 2.0)
  }

  gridTest("theta intersection does not skip real empty partials " +
    "(serialized, emptyFirst)")(
    Seq((false, false), (false, true), (true, false), (true, true))) {
    case (serialized, emptyFirst) =>
      val agg = new ThetaIntersectionAgg(BoundReference(0, BinaryType, nullable = true))
      val empty = createIntersectionBuffer(agg, Seq(buildThetaSketch(Seq.empty)))
      val populated = createIntersectionBuffer(agg, Seq(buildThetaSketch(Seq(1, 2))))
      assert(agg.serialize(empty) != null)
      val partials = if (emptyFirst) Seq(empty, populated) else Seq(populated, empty)
      val result = partials.foldLeft(agg.createAggregationBuffer()) { (buffer, partial) =>
        val input = if (serialized) agg.deserialize(agg.serialize(partial)) else partial
        agg.merge(buffer, input)
      }
      assert(agg.serialize(result) != null)
      checkIntersectionEstimate(agg, result, 0.0)
  }

  test("theta intersection preserves an empty intersection of disjoint sketches") {
    val agg = new ThetaIntersectionAgg(BoundReference(0, BinaryType, nullable = true))
    val disjoint = createIntersectionBuffer(agg,
      Seq(buildThetaSketch(Seq(1)), buildThetaSketch(Seq(2))))
    val serialized = agg.serialize(disjoint)
    assert(serialized != null)
    assert(ThetaSketchUtils.wrapCompactSketch(serialized, agg.prettyName).getEstimate == 0.0)

    val populated = createIntersectionBuffer(agg, Seq(buildThetaSketch(Seq(1, 2))))
    val result = agg.merge(populated, agg.deserialize(serialized))
    checkIntersectionEstimate(agg, result, 0.0)
  }
}
