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

import org.apache.datasketches.kll.KllLongsSketch
import org.apache.datasketches.memory.Memory

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.types.BinaryType

class KllMergeAggSuite extends SparkFunSuite {

  // A sketch over [0, n). It compacts iff n > k, and n = 0 gives an empty sketch.
  private def sketch(k: Int, n: Int): KllLongsSketch = {
    val s = KllLongsSketch.newHeapInstance(k)
    (0 until n).foreach(i => s.update(i.toLong))
    s
  }

  private def newAgg(lowestCompactingK: Boolean): KllMergeAggBigint =
    KllMergeAggBigint(BoundReference(0, BinaryType, nullable = true),
      lowestCompactingK = lowestCompactingK)

  private def heapify(bytes: Any): KllLongsSketch =
    KllLongsSketch.heapify(Memory.wrap(bytes.asInstanceOf[Array[Byte]]))

  // The k rule for non-NULL sketches in merge order.
  private def ruleK(inputs: Seq[KllLongsSketch]): Int = inputs.filterNot(_.isEmpty) match {
    case first +: rest => (first.getK +: rest.filter(_.isEstimationMode).map(_.getK)).min
    case _ => inputs.headOption.map(_.getK).getOrElse(200)
  }

  // Sketches in order, with None for a NULL.
  private type Rows = Seq[Option[KllLongsSketch]]

  // Updates one buffer per partition with its rows in order, sends each buffer
  // through serialize and deserialize, and merges them in order. Returns the partial results and
  // the result.
  private def updateAndMerge(
      agg: KllMergeAggBigint,
      partitions: Seq[Rows]): (Rows, KllLongsSketch) = {
    val partials = partitions.map { rows =>
      val buffer = rows.foldLeft(agg.createAggregationBuffer()) { (buffer, row) =>
        agg.update(buffer, InternalRow(row.map(_.toByteArray).orNull))
      }
      agg.deserialize(agg.serialize(buffer))
    }
    val merged = partials.foldLeft(agg.createAggregationBuffer())((b, p) => agg.merge(b, p))
    (partials, heapify(agg.eval(merged)))
  }

  test("SPARK-61368: update and merge follow the k rule in the order that they read inputs") {
    val rng = new Random(20261123)
    val ks = Seq(8, 50, 100, 200, 400, 1000)
    for (_ <- 0 until 1000) {
      // Up to 4 partitions, each with up to 4 rows. A partition can have no rows.
      val partitions = Seq.fill(1 + rng.nextInt(4)) {
        Seq.fill(rng.nextInt(5)) {
          val k = ks(rng.nextInt(ks.size))
          // 10% NULL, 20% empty, 35% exact, 35% compacting.
          rng.nextInt(20) match {
            case r if r < 2 => None
            case r if r < 6 => Some(sketch(k, 0))
            case r if r < 13 => Some(sketch(k, 1 + rng.nextInt(k)))
            case _ => Some(sketch(k, k + 1 + rng.nextInt(2 * k)))
          }
        }
      }
      val n = partitions.flatten.flatten.map(_.getN).sum

      val (partials, merged) = updateAndMerge(newAgg(lowestCompactingK = true), partitions)
      partitions.zip(partials).foreach { case (rows, partial) =>
        // A partition without an input sketch sends no sketch.
        assert(partial.map(_.getK) == Option.when(rows.exists(_.isDefined))(ruleK(rows.flatten)),
          partitions)
      }
      assert((merged.getK, merged.getN) == ((ruleK(partials.flatten), n)), partitions)

      // With the conf off, a partial result takes the k of its first input, or 200 without input.
      // The result takes the k of the first partial result.
      val (partialsOff, mergedOff) = updateAndMerge(newAgg(lowestCompactingK = false), partitions)
      partitions.zip(partialsOff).foreach { case (rows, partial) =>
        assert(partial.map(_.getK) == Some(rows.flatten.headOption.map(_.getK).getOrElse(200)),
          partitions)
      }
      assert((mergedOff.getK, mergedOff.getN) == ((partialsOff.head.get.getK, n)), partitions)
    }
  }

  test("SPARK-61368: merge follows the k rule over the partial results") {
    // The rows of each partition, merged in this order, and the expected k with the conf on.
    val cases = Seq(
      // A partition without an input sketch does not take part.
      Seq(Seq(), Seq(Some(sketch(1000, 0)))) -> 1000,
      Seq(Seq(None), Seq(Some(sketch(1000, 0)))) -> 1000,
      Seq(Seq(Some(sketch(100, 0))), Seq(None, None), Seq()) -> 100,
      // A partial result with only empty inputs has the k of its first input, until a non-empty
      // partial result replaces it.
      Seq(Seq(Some(sketch(1000, 0))), Seq(Some(sketch(100, 0)))) -> 1000,
      Seq(Seq(Some(sketch(1000, 0))), Seq(Some(sketch(400, 10)))) -> 400,
      Seq(Seq(Some(sketch(8, 0))), Seq(Some(sketch(1000, 10))), Seq(Some(sketch(400, 0)))) -> 1000,
      // An exact partial result after the first never lowers k.
      Seq(Seq(Some(sketch(1000, 10))), Seq(Some(sketch(8, 3)))) -> 1000,
      // A compacting partial result with a smaller k lowers k.
      Seq(Seq(Some(sketch(1000, 10))), Seq(Some(sketch(100, 5000)))) -> 100,
      Seq(Seq(Some(sketch(100, 5000))), Seq(Some(sketch(1000, 10)))) -> 100,
      // A partial result that compacted counts as compacting, though no input of it compacts.
      Seq(Seq(Some(sketch(1000, 10))), Seq(Some(sketch(100, 60)), Some(sketch(200, 60)))) -> 100)
    for ((partitions, k) <- cases) {
      assert(updateAndMerge(newAgg(lowestCompactingK = true), partitions)._2.getK == k,
        partitions)
    }
  }

  test("SPARK-61368: with the conf on, a buffer without input is serialized as zero bytes") {
    val on = newAgg(lowestCompactingK = true)
    assert(on.serialize(None).isEmpty)
    assert(on.deserialize(Array.emptyByteArray).isEmpty)
    // The result without input is still an empty sketch with the default k.
    val result = heapify(on.eval(None))
    assert(result.isEmpty && result.getK == 200)

    val off = newAgg(lowestCompactingK = false)
    val serialized = heapify(off.serialize(None))
    assert(serialized.isEmpty && serialized.getK == 200)
  }
}
