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

import org.apache.datasketches.hll.HllSketch
import org.apache.datasketches.memory.Memory

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, HllSketchEstimate}
import org.apache.spark.sql.types.{BinaryType, DataType, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String


class DatasketchesHllSketchSuite extends SparkFunSuite {

  def simulateUpdateMerge[T](dataType: DataType, input: Seq[Any], numSketches: Integer = 5):
    (Long, NumericRange[Long]) = {

    // create a map of agg function instances
    val aggFunctionMap = Seq.tabulate(numSketches)(index => {
      val sketch = new HllSketchAgg(BoundReference(0, dataType, nullable = true))
      index -> (sketch, sketch.createAggregationBuffer())
    }).toMap

    // randomly update agg function instances
    input.map(value => {
      val (aggFunction, aggBuffer) = aggFunctionMap(Random.nextInt(numSketches))
      aggFunction.update(aggBuffer, InternalRow(value))
    })

    def serializeDeserialize(tuple: (HllSketchAgg, HllSketch)):
      (HllSketchAgg, HllSketch) = {
      val (agg, buf) = tuple
      val serialized = agg.serialize(buf)
      (agg, agg.deserialize(serialized))
    }

    // simulate serialization -> deserialization -> merge
    val mapValues = aggFunctionMap.values
    val (mergedAgg, mergedBuf) = mapValues.tail.foldLeft(mapValues.head)((prev, cur) => {
      val (prevAgg, prevBuf) = serializeDeserialize(prev)
      val (_, curBuf) = serializeDeserialize(cur)

      (prevAgg, prevAgg.merge(prevBuf, curBuf))
    })

    val estimator = HllSketchEstimate(BoundReference(0, BinaryType, nullable = true))
    val estimate = estimator.eval(InternalRow(mergedBuf.toUpdatableByteArray)).asInstanceOf[Long]
    (estimate, mergedBuf.getLowerBound(3).toLong to mergedBuf.getUpperBound(3).toLong)
  }

  test("Test min/max values of supported datatypes") {
    val intRange = Integer.MIN_VALUE to Integer.MAX_VALUE by 10000000
    val (intEstimate, intEstimateRange) = simulateUpdateMerge(IntegerType, intRange)
    assert(intEstimate == intRange.size || intEstimateRange.contains(intRange.size.toLong))

    val longRange = Long.MinValue to Long.MaxValue by 1000000000000000L
    val (longEstimate, longEstimateRange) = simulateUpdateMerge(LongType, longRange)
    assert(longEstimate == longRange.size || longEstimateRange.contains(longRange.size.toLong))

    val stringRange = Seq.tabulate(1000)(i => UTF8String.fromString(Random.nextString(i)))
    val (stringEstimate, stringEstimateRange) = simulateUpdateMerge(StringType, stringRange)
    assert(stringEstimate == stringRange.size ||
      stringEstimateRange.contains(stringRange.size.toLong))

    val binaryRange = Seq.tabulate(1000)(i => UTF8String.fromString(Random.nextString(i)).getBytes)
    val (binaryEstimate, binaryEstimateRange) = simulateUpdateMerge(BinaryType, binaryRange)
    assert(binaryEstimate == binaryRange.size ||
      binaryEstimateRange.contains(binaryRange.size.toLong))
  }

  test("Test lgMaxK results in downsampling sketches with larger lgConfigK") {
    val aggFunc1 = new HllSketchAgg(BoundReference(0, IntegerType, nullable = true), 12)
    val sketch1 = aggFunc1.createAggregationBuffer()
    (0 to 100).map(i => aggFunc1.update(sketch1, InternalRow(i)))
    val binary1 = aggFunc1.eval(sketch1)

    val aggFunc2 = new HllSketchAgg(BoundReference(0, IntegerType, nullable = true), 10)
    val sketch2 = aggFunc2.createAggregationBuffer()
    (0 to 100).map(i => aggFunc2.update(sketch2, InternalRow(i)))
    sketch2.isCompact
    val binary2 = aggFunc2.eval(sketch2)

    val aggFunc3 = new HllUnionAgg(BoundReference(0, BinaryType, nullable = true), true)
    val union1 = aggFunc3.createAggregationBuffer()
    aggFunc3.update(union1, InternalRow(binary1))
    aggFunc3.update(union1, InternalRow(binary2))
    val binary3 = aggFunc3.eval(union1)

    assert(HllSketch.heapify(Memory.wrap(binary3.asInstanceOf[Array[Byte]])).getLgConfigK == 12)
  }

  test("HllUnionAgg throws proper error for invalid binary input causing ArrayIndexOutOfBounds") {
    val aggFunc = new HllUnionAgg(BoundReference(0, BinaryType, nullable = true), true)
    val union = aggFunc.createAggregationBuffer()

    // Craft a byte array that passes initial size checks but has an invalid CurMode ordinal.
    // HLL preamble layout:
    //   Byte 0: preInts (preamble size in ints)
    //   Byte 1: serVer (must be 1)
    //   Byte 2: famId (must be 7 for HLL)
    //   Byte 3: lgK (4-21)
    //   Byte 5: flags
    //   Byte 7: modeByte - bits 0-1 contain curMode ordinal (0=LIST, 1=SET, 2=HLL)
    //
    // Setting bits 0-1 of byte 7 to 0b11 (=3) causes CurMode.fromOrdinal(3) to throw
    // ArrayIndexOutOfBoundsException since CurMode only has ordinals 0, 1, 2.
    // This happens in PreambleUtil.extractCurMode() before other validations run.
    val invalidBinary = Array[Byte](
      2,    // byte 0: preInts = 2 (LIST_PREINTS, passes check)
      1,    // byte 1: serVer = 1 (valid)
      7,    // byte 2: famId = 7 (HLL family)
      12,   // byte 3: lgK = 12 (valid range 4-21)
      0,    // byte 4: unused
      0,    // byte 5: flags = 0
      0,    // byte 6: unused
      3     // byte 7: modeByte with bits 0-1 = 0b11 = 3 (INVALID curMode ordinal!)
    )

    val exception = intercept[Exception] {
      aggFunc.update(union, InternalRow(invalidBinary))
    }

    // Verify that ArrayIndexOutOfBoundsException is properly caught and converted
    // to the user-friendly HLL_INVALID_INPUT_SKETCH_BUFFER error
    assert(
      !exception.isInstanceOf[ArrayIndexOutOfBoundsException],
      s"ArrayIndexOutOfBoundsException should be caught and converted to " +
        s"HLL_INVALID_INPUT_SKETCH_BUFFER error, but got: ${exception.getClass.getName}"
    )
    assert(
      exception.getMessage.contains("HLL_INVALID_INPUT_SKETCH_BUFFER"),
      s"Expected HLL_INVALID_INPUT_SKETCH_BUFFER error, " +
        s"but got: ${exception.getClass.getName}: ${exception.getMessage}"
    )
  }
}
