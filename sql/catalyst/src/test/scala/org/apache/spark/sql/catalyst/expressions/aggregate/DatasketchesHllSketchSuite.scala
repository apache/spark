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

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, HllSketchEstimate, HllUnion, Literal}
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
  /** Runs HllUnionAgg over `inputs` (a NULL entry stands for a NULL sketch) for a single group. */
  private def unionAgg(inputs: Seq[Any], allowDifferentLgConfigK: Boolean): Array[Byte] = {
    val aggFunc = new HllUnionAgg(
      BoundReference(0, BinaryType, nullable = true), allowDifferentLgConfigK)
    val buffer = inputs.foldLeft(aggFunc.createAggregationBuffer()) { (buf, input) =>
      aggFunc.update(buf, InternalRow(input))
    }
    aggFunc.eval(buffer).asInstanceOf[Array[Byte]]
  }

  /** Runs HllSketchAgg at `lgConfigK` over `values` (a NULL entry stands for a NULL value). */
  private def sketchAgg(values: Seq[Any], lgConfigK: Int): Array[Byte] = {
    val aggFunc = new HllSketchAgg(BoundReference(0, StringType, nullable = true), lgConfigK)
    val buffer = values.foldLeft(aggFunc.createAggregationBuffer()) { (buf, value) =>
      aggFunc.update(buf, InternalRow(value))
    }
    aggFunc.eval(buffer).asInstanceOf[Array[Byte]]
  }

  /** Evaluates the scalar hll_union over two serialized sketches. */
  private def scalarUnion(
      left: Array[Byte], right: Array[Byte], allowDifferentLgConfigK: Boolean): Array[Byte] =
    HllUnion(
      Literal(left, BinaryType),
      Literal(right, BinaryType),
      Literal(allowDifferentLgConfigK)).eval(InternalRow.empty).asInstanceOf[Array[Byte]]

  private def lgConfigKOf(sketch: Array[Byte]): Int =
    HllSketch.heapify(Memory.wrap(sketch)).getLgConfigK

  private def estimateOf(sketch: Array[Byte]): Long =
    HllSketchEstimate(BoundReference(0, BinaryType, nullable = true))
      .eval(InternalRow(sketch)).asInstanceOf[Long]

  private def stringValues(n: Int): Seq[Any] =
    Seq.tabulate(n)(i => UTF8String.fromString(i.toString))

  test("hll_union_agg on a group with no non-NULL sketch yields an empty default-lgConfigK " +
    "sketch") {
    // The aggregate has no lgConfigK parameter and never saw a sketch, so it has no precision to
    // report and falls back to the Datasketches default. Documented here because the resulting
    // sketch is observable, and must stay harmless to later unions (see the tests below).
    val allNull = unionAgg(Seq(null, null), allowDifferentLgConfigK = false)
    assert(estimateOf(allNull) == 0L)
    assert(lgConfigKOf(allNull) == HllSketch.DEFAULT_LG_K)

    // hll_sketch_agg does not share the problem only because it has the parameter: it builds its
    // buffer eagerly at the requested lgConfigK. Without the argument it defaults to 12 as well.
    val emptyAt15 = sketchAgg(Seq(null, null), 15)
    assert(estimateOf(emptyAt15) == 0L)
    assert(lgConfigKOf(emptyAt15) == 15)
  }

  test("hll_union_agg merges an empty sketch of a different lgConfigK without an error") {
    // An empty sketch holds no coupons, so unioning it cannot lose information at any lgConfigK.
    // This is the shape produced by the test above, i.e. what a persisted table ends up holding
    // for a group whose sketches were all NULL.
    val emptyAtDefaultLgK = unionAgg(Seq(null), allowDifferentLgConfigK = false)
    val sketchAt15 = sketchAgg(stringValues(1000), 15)

    Seq(true, false).foreach { allowDifferentLgConfigK =>
      Seq(
        ("empty sketch first", Seq[Any](emptyAtDefaultLgK, sketchAt15)),
        ("empty sketch last", Seq[Any](sketchAt15, emptyAtDefaultLgK))
      ).foreach { case (order, inputs) =>
        val merged = unionAgg(inputs, allowDifferentLgConfigK)
        // The non-empty sketch decides the precision, whichever order the rows arrive in: the
        // result must not depend on which row the aggregate happens to see first.
        assert(lgConfigKOf(merged) == 15,
          s"$order (allowDifferentLgConfigK=$allowDifferentLgConfigK) changed the lgConfigK")
        assert(estimateOf(merged) == estimateOf(sketchAt15),
          s"$order (allowDifferentLgConfigK=$allowDifferentLgConfigK) changed the estimate")
      }
    }
  }

  test("hll_union_agg still rejects non-empty sketches with different lgConfigK") {
    val sketchAt12 = sketchAgg(stringValues(1000), 12)
    val sketchAt15 = sketchAgg(stringValues(1000), 15)

    Seq(
      Seq[Any](sketchAt12, sketchAt15),
      Seq[Any](sketchAt15, sketchAt12)
    ).foreach { inputs =>
      val exception = intercept[SparkRuntimeException] {
        unionAgg(inputs, allowDifferentLgConfigK = false)
      }
      assert(exception.getCondition == "HLL_UNION_DIFFERENT_LG_K")
    }

    // And still downsamples rather than erroring when the caller opts in.
    assert(lgConfigKOf(unionAgg(Seq(sketchAt15, sketchAt12), allowDifferentLgConfigK = true)) == 12)
  }

  test("hll_union merges an empty sketch of a different lgConfigK without an error") {
    val emptyAtDefaultLgK = unionAgg(Seq(null), allowDifferentLgConfigK = false)
    val sketchAt15 = sketchAgg(stringValues(1000), 15)

    Seq(true, false).foreach { allowDifferentLgConfigK =>
      Seq(
        ("empty sketch first", emptyAtDefaultLgK, sketchAt15),
        ("empty sketch last", sketchAt15, emptyAtDefaultLgK)
      ).foreach { case (order, left, right) =>
        val merged = scalarUnion(left, right, allowDifferentLgConfigK)
        assert(lgConfigKOf(merged) == 15,
          s"$order (allowDifferentLgConfigK=$allowDifferentLgConfigK) changed the lgConfigK")
        assert(estimateOf(merged) == estimateOf(sketchAt15),
          s"$order (allowDifferentLgConfigK=$allowDifferentLgConfigK) changed the estimate")
      }
    }
  }

  test("hll_union still rejects non-empty sketches with different lgConfigK") {
    val sketchAt12 = sketchAgg(stringValues(1000), 12)
    val sketchAt15 = sketchAgg(stringValues(1000), 15)

    Seq((sketchAt12, sketchAt15), (sketchAt15, sketchAt12)).foreach { case (left, right) =>
      val exception = intercept[SparkRuntimeException] {
        scalarUnion(left, right, allowDifferentLgConfigK = false)
      }
      assert(exception.getCondition == "HLL_UNION_DIFFERENT_LG_K")
    }

    // Two populated sketches still downsample to the smaller lgConfigK when opted in.
    assert(lgConfigKOf(scalarUnion(sketchAt15, sketchAt12, allowDifferentLgConfigK = true)) == 12)
  }
}
