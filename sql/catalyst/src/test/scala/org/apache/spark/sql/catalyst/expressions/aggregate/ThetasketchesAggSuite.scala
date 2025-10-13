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
import org.apache.spark.sql.catalyst.expressions.{BoundReference, ThetaSketchEstimate}
import org.apache.spark.sql.catalyst.util.ArrayData
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
}
