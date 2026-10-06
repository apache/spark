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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.util.TupleSketchUtils
import org.apache.spark.sql.types.{BinaryType, IntegerType}

class TuplesketchAggSuite extends SparkFunSuite {
  private val sketchInput = BoundReference(0, BinaryType, nullable = true)
  private val keyInput = BoundReference(0, IntegerType, nullable = false)

  private def buildSketch[T](agg: TypedImperativeAggregate[T], keys: Seq[Int]): Array[Byte] = {
    val buffer = keys.foldLeft(agg.createAggregationBuffer()) { (buffer, key) =>
      agg.update(buffer, InternalRow(key))
    }
    agg.eval(buffer).asInstanceOf[Array[Byte]]
  }

  private def testIntersection[T <: AnyRef](
      createAggregate: () => TypedImperativeAggregate[T],
      createSketch: Seq[Int] => Array[Byte],
      estimate: Array[Byte] => Double): Unit = {
    val name = createAggregate().prettyName

    def createBuffer(agg: TypedImperativeAggregate[T], sketches: Seq[Array[Byte]]): T = {
      sketches.foldLeft(agg.createAggregationBuffer()) { (buffer, sketch) =>
        agg.update(buffer, InternalRow(sketch))
      }
    }

    def checkEstimate(agg: TypedImperativeAggregate[T], buffer: T, expected: Double): Unit = {
      val result = agg.eval(buffer).asInstanceOf[Array[Byte]]
      assert(result != null && result.nonEmpty)
      assert(estimate(result) == expected)
    }

    gridTest(s"$name keeps no-input partials null and returns an empty final sketch")(
      Seq(0, 2)) { numNulls =>
      val agg = createAggregate()
      val buffer = createBuffer(agg, Seq.fill[Array[Byte]](numNulls)(null))
      assert(agg.serialize(buffer) == null)
      assert(agg.deserialize(null) == null)
      checkEstimate(agg, buffer, 0.0)
      assert(agg.eval(buffer).asInstanceOf[Array[Byte]].sameElements(createSketch(Seq.empty)))

      // Final evaluation must not change the untouched intermediate state.
      assert(agg.serialize(buffer) == null)
      val updated = agg.update(buffer, InternalRow(createSketch(Seq(1, 2))))
      checkEstimate(agg, updated, 2.0)
    }

    test(s"$name preserves null through partial merge and final merge") {
      val agg = createAggregate()
      val partial = createBuffer(agg, Seq(null, null))
      val partialMerge = agg.merge(
        agg.createAggregationBuffer(),
        agg.deserialize(agg.serialize(partial)))
      val merged = agg.merge(partialMerge, agg.deserialize(agg.serialize(partial)))
      assert(agg.serialize(merged) == null)

      val result = agg.merge(
        agg.createAggregationBuffer(),
        agg.deserialize(agg.serialize(merged)))
      checkEstimate(agg, result, 0.0)
    }

    gridTest(s"$name skips no-input partials (serialized, nullFirst)")(
      Seq((false, false), (false, true), (true, false), (true, true))) {
      case (serialized, nullFirst) =>
        val agg = createAggregate()
        val untouched = createBuffer(agg, Seq(null, null))
        val populated = createBuffer(agg, Seq(null, createSketch(Seq(1, 2)), null))
        val partials = if (nullFirst) Seq(untouched, populated) else Seq(populated, untouched)
        val merged = partials.foldLeft(agg.createAggregationBuffer()) { (buffer, partial) =>
          val input = if (serialized) agg.deserialize(agg.serialize(partial)) else partial
          agg.merge(buffer, input)
        }
        checkEstimate(agg, merged, 2.0)

        // A further partial-merge serialization must preserve the populated result.
        val result = agg.merge(
          agg.createAggregationBuffer(),
          agg.deserialize(agg.serialize(merged)))
        checkEstimate(agg, result, 2.0)
    }

    test(s"$name skips untouched objects in mergeBuffersObjects") {
      val agg = createAggregate()
      val destination = new GenericInternalRow(1)
      val incoming = new GenericInternalRow(1)
      agg.initialize(destination)
      agg.initialize(incoming)
      agg.update(destination, InternalRow(createSketch(Seq(1, 2))))
      agg.update(incoming, InternalRow(null: Array[Byte]))

      agg.mergeBuffersObjects(destination, incoming)
      val result = agg.eval(destination).asInstanceOf[Array[Byte]]
      assert(estimate(result) == 2.0)
    }

    gridTest(s"$name does not skip real empty partials, (serialized, emptyFirst) =")(
      Seq((false, false), (false, true), (true, false), (true, true))) {
      case (serialized, emptyFirst) =>
        val agg = createAggregate()
        val empty = createBuffer(agg, Seq(createSketch(Seq.empty)))
        val populated = createBuffer(agg, Seq(createSketch(Seq(1, 2))))
        assert(agg.serialize(empty) != null)
        val partials = if (emptyFirst) Seq(empty, populated) else Seq(populated, empty)
        val result = partials.foldLeft(agg.createAggregationBuffer()) { (buffer, partial) =>
          val input = if (serialized) agg.deserialize(agg.serialize(partial)) else partial
          agg.merge(buffer, input)
        }
        assert(agg.serialize(result) != null)
        checkEstimate(agg, result, 0.0)
    }

    test(s"$name preserves an empty intersection of disjoint sketches") {
      val agg = createAggregate()
      val disjoint = createBuffer(agg, Seq(createSketch(Seq(1)), createSketch(Seq(2))))
      val serialized = agg.serialize(disjoint)
      assert(serialized != null)
      assert(estimate(serialized) == 0.0)

      val populated = createBuffer(agg, Seq(createSketch(Seq(1, 2))))
      val result = agg.merge(populated, agg.deserialize(serialized))
      checkEstimate(agg, result, 0.0)
    }
  }

  testIntersection(
    () => new TupleIntersectionAggDouble(sketchInput),
    keys => buildSketch(new TupleSketchAggDouble(keyInput, Literal(1.0)), keys),
    bytes =>
      TupleSketchUtils.heapifyDoubleSketch(bytes, "tuple_intersection_agg_double").getEstimate)

  testIntersection(
    () => new TupleIntersectionAggInteger(sketchInput),
    keys => buildSketch(new TupleSketchAggInteger(keyInput, Literal(1)), keys),
    bytes =>
      TupleSketchUtils.heapifyIntegerSketch(bytes, "tuple_intersection_agg_integer").getEstimate)
}
