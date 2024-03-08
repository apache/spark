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
}
