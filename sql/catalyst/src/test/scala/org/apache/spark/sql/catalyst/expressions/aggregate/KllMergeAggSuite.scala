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

import scala.util.Random

import org.apache.datasketches.kll.{KllDoublesSketch, KllFloatsSketch, KllLongsSketch, KllSketch}
import org.apache.datasketches.memory.Memory

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.types.BinaryType

class KllMergeAggSuite extends SparkFunSuite {

  private val types = Seq("bigint", "float", "double")

  // Sketch bytes in order, with None for a NULL.
  private type Rows = Seq[Option[Array[Byte]]]

  // A sketch over [0, n). It compacts iff n > k, and n = 0 gives an empty sketch.
  private def sketch(t: String, k: Int, n: Int): Array[Byte] = t match {
    case "bigint" =>
      val s = KllLongsSketch.newHeapInstance(k)
      (0 until n).foreach(i => s.update(i.toLong))
      s.toByteArray
    case "float" =>
      val s = KllFloatsSketch.newHeapInstance(k)
      (0 until n).foreach(i => s.update(i.toFloat))
      s.toByteArray
    case "double" =>
      val s = KllDoublesSketch.newHeapInstance(k)
      (0 until n).foreach(i => s.update(i.toDouble))
      s.toByteArray
  }

  private def heapify(t: String, bytes: Any): KllSketch = {
    val mem = Memory.wrap(bytes.asInstanceOf[Array[Byte]])
    t match {
      case "bigint" => KllLongsSketch.heapify(mem)
      case "float" => KllFloatsSketch.heapify(mem)
      case "double" => KllDoublesSketch.heapify(mem)
    }
  }

  private def newAgg(t: String, lowestCompactingK: Boolean): KllMergeAggBase[_ <: KllSketch] = {
    val child = BoundReference(0, BinaryType, nullable = true)
    t match {
      case "bigint" => KllMergeAggBigint(child, lowestCompactingK = lowestCompactingK)
      case "float" => KllMergeAggFloat(child, lowestCompactingK = lowestCompactingK)
      case "double" => KllMergeAggDouble(child, lowestCompactingK = lowestCompactingK)
    }
  }

  // The k rule for non-NULL sketches in merge order.
  private def ruleK(inputs: Seq[KllSketch]): Int = inputs.filterNot(_.isEmpty) match {
    case first +: rest => (first.getK +: rest.filter(_.isEstimationMode).map(_.getK)).min
    case _ => inputs.headOption.map(_.getK).getOrElse(200)
  }

  // Updates one buffer per partition with its rows in order, sends each buffer through serialize
  // and deserialize, and merges them in order. Returns the partial results and the result.
  private def updateAndMerge(
      t: String,
      lowestCompactingK: Boolean,
      partitions: Seq[Rows]): (Seq[Option[KllSketch]], KllSketch) = {
    val (partials, result) = updateAndMerge(newAgg(t, lowestCompactingK), partitions)
    (partials, heapify(t, result))
  }

  private def updateAndMerge[T <: KllSketch](
      agg: KllMergeAggBase[T],
      partitions: Seq[Rows]): (Seq[Option[T]], Any) = {
    val partials = partitions.map { rows =>
      val buffer = rows.foldLeft(agg.createAggregationBuffer()) { (buffer, row) =>
        agg.update(buffer, InternalRow(row.orNull))
      }
      agg.deserialize(agg.serialize(buffer))
    }
    val merged = partials.foldLeft(agg.createAggregationBuffer())((b, p) => agg.merge(b, p))
    (partials, agg.eval(merged))
  }

  test("SPARK-61368: update and merge follow the k rule in the order that they read inputs") {
    val rng = new Random(20261123)
    val ks = Seq(8, 50, 100, 200, 400, 1000)
    for (t <- types; _ <- 0 until 400) {
      // Up to 4 partitions, each with up to 4 rows. A partition can have no rows.
      val partitions = Seq.fill(1 + rng.nextInt(4)) {
        Seq.fill(rng.nextInt(5)) {
          val k = ks(rng.nextInt(ks.size))
          // 10% NULL, 20% empty, 35% exact, 35% compacting.
          rng.nextInt(20) match {
            case r if r < 2 => None
            case r if r < 6 => Some(sketch(t, k, 0))
            case r if r < 13 => Some(sketch(t, k, 1 + rng.nextInt(k)))
            case _ => Some(sketch(t, k, k + 1 + rng.nextInt(2 * k)))
          }
        }
      }
      val inputs = partitions.map(_.flatten.map(heapify(t, _)))
      val n = inputs.flatten.map(_.getN).sum

      val (partials, merged) = updateAndMerge(t, lowestCompactingK = true, partitions)
      inputs.zip(partials).foreach { case (in, partial) =>
        // A partition without an input sketch sends no sketch.
        assert(partial.map(_.getK) == Option.when(in.nonEmpty)(ruleK(in)), (t, partitions))
      }
      assert((merged.getK, merged.getN) == ((ruleK(partials.flatten), n)), (t, partitions))

      // With the conf off, a partial result takes the k of its first input, or 200 without input.
      // The result takes the k of the first partial result.
      val (partialsOff, mergedOff) = updateAndMerge(t, lowestCompactingK = false, partitions)
      inputs.zip(partialsOff).foreach { case (in, partial) =>
        assert(partial.map(_.getK) == Some(in.headOption.map(_.getK).getOrElse(200)),
          (t, partitions))
      }
      assert((mergedOff.getK, mergedOff.getN) == ((partialsOff.head.get.getK, n)), (t, partitions))
    }
  }

  test("SPARK-61368: merge follows the k rule over the partial results") {
    // The (k, n) rows of each partition, with None for a NULL, and the expected k.
    val cases: Seq[(Seq[Seq[Option[(Int, Int)]]], Int)] = Seq(
      // A partition without an input sketch does not take part.
      Seq(Seq(), Seq(Some((1000, 0)))) -> 1000,
      Seq(Seq(None), Seq(Some((1000, 0)))) -> 1000,
      Seq(Seq(Some((100, 0))), Seq(None, None), Seq()) -> 100,
      // A partial result with only empty inputs has the k of its first input, until a non-empty
      // partial result replaces it.
      Seq(Seq(Some((1000, 0))), Seq(Some((100, 0)))) -> 1000,
      Seq(Seq(Some((1000, 0))), Seq(Some((400, 10)))) -> 400,
      Seq(Seq(Some((8, 0))), Seq(Some((1000, 10))), Seq(Some((400, 0)))) -> 1000,
      // An exact partial result after the first never lowers k.
      Seq(Seq(Some((1000, 10))), Seq(Some((8, 3)))) -> 1000,
      // A compacting partial result with a smaller k lowers k.
      Seq(Seq(Some((1000, 10))), Seq(Some((100, 5000)))) -> 100,
      Seq(Seq(Some((100, 5000))), Seq(Some((1000, 10)))) -> 100,
      // A partial result that compacted counts as compacting, though no input of it compacts.
      Seq(Seq(Some((1000, 10))), Seq(Some((100, 60)), Some((200, 60)))) -> 100)
    for (t <- types; (partitions, k) <- cases) {
      val rows = partitions.map(_.map(_.map { case (k, n) => sketch(t, k, n) }))
      assert(updateAndMerge(t, lowestCompactingK = true, rows)._2.getK == k, (t, partitions))
    }
  }

  test("SPARK-61368: with the conf on, a buffer without input is serialized as zero bytes") {
    for (t <- types; on <- Seq(true, false)) {
      val agg = newAgg(t, on)
      val serialized = agg.serialize(None)
      if (on) {
        assert(serialized.isEmpty, t)
        assert(agg.deserialize(serialized).isEmpty, t)
      } else {
        // With the conf off, it is an empty sketch with the default k, as before.
        val sketch = heapify(t, serialized)
        assert(sketch.isEmpty && sketch.getK == 200, t)
      }
      // The result without input is an empty sketch with the default k.
      val result = heapify(t, agg.eval(None))
      assert(result.isEmpty && result.getK == 200, (t, on))
    }
  }
}
