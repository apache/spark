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

package org.apache.spark.sql.streaming

import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions._

class StreamingJoinWatermarkDirectionSuite extends StreamTest {
  import testImplicits._

  test("left outer join on event time needs a watermark on the right input") {
    val leftInput = MemoryStream[(Int, Int)]
    val rightInput = MemoryStream[(Int, Int)]
    val left = leftInput.toDF().toDF("lk", "t")
      .select($"lk", timestamp_seconds($"t").as("lt"))
      .withWatermark("lt", "10 seconds")
    val right = rightInput.toDF().toDF("rk", "t")
      .select($"rk", timestamp_seconds($"t").as("rt"))
    // The left watermark would finalize left rows that a late right row can still match.
    val joined = left.join(right, expr("lk = rk AND lt = rt"), "left_outer")

    val e = intercept[AnalysisException] {
      testStream(joined)(
        AddData(leftInput, (1, 5)),
        CheckNewAnswer())
    }
    assert(e.getMessage.contains("without a watermark on the right input that bounds the " +
      "state of the left input"))
  }

  private def watermarkedInputs(): (MemoryStream[(Int, Int, Int)], MemoryStream[(Int, Int, Int)],
      DataFrame, DataFrame) = {
    val leftInput = MemoryStream[(Int, Int, Int)]
    val rightInput = MemoryStream[(Int, Int, Int)]
    val left = leftInput.toDF().toDF("lk", "t", "b")
      .select($"lk", timestamp_seconds($"t").as("lt"), timestamp_seconds($"b").as("lb"))
      .withWatermark("lt", "10 seconds")
    val right = rightInput.toDF().toDF("rk", "t", "a")
      .select($"rk", timestamp_seconds($"t").as("rt"), timestamp_seconds($"a").as("ra"))
      .withWatermark("rt", "10 seconds")
    (leftInput, rightInput, left, right)
  }

  test("a no-op cast does not move the key that the check reads") {
    // The optimizer removes the cast, so eviction uses the key `lt = ra`, which the right
    // watermark does not bound, whichever way the condition spells it.
    Seq("CAST(lt AS TIMESTAMP) = ra", "lt = ra").foreach { key =>
      val (leftInput, _, left, right) = watermarkedInputs()
      val joined = left.join(right, expr(s"lk = rk AND $key AND lb = rt"), "left_outer")
      val e = intercept[AnalysisException] {
        testStream(joined)(
          AddData(leftInput, (1, 5, 5)),
          CheckNewAnswer())
      }
      assert(e.getMessage.contains("without a watermark on the right input that bounds the " +
        "state of the left input"), key)
      // The message names the equality that eviction uses, not `lb = rt`.
      assert(e.getMessage.contains("by the equality \"(") && e.getMessage.contains("= ra)\""),
        e.getMessage)
    }
  }

  test("a key shifted forward from the right event time bounds the left state") {
    Seq("rt + INTERVAL 10 SECONDS", "timestampadd(SECOND, 10, rt)").foreach { shifted =>
      val (leftInput, rightInput, left, right) = watermarkedInputs()
      val joined = left.join(right, expr(s"lk = rk AND lt = $shifted"), "left_outer")
        .select($"lk", $"lt".cast("long"), $"rt".cast("long"))
      testStream(joined)(
        AddData(leftInput, (1, 15, 0)),
        AddData(rightInput, (1, 5, 0)),
        CheckNewAnswer((1, 15L, 5L)))
    }
  }

  test("a key that the right input computes before the join as a forward shift bounds the left " +
      "state") {
    val (leftInput, rightInput, left, right) = watermarkedInputs()
    val shifted = right.withColumn("rs", $"rt" + expr("INTERVAL 10 SECONDS"))
    val joined = left.join(shifted, expr("lk = rk AND lt = rs"), "left_outer")
      .select($"lk", $"lt".cast("long"), $"rt".cast("long"))
    testStream(joined)(
      AddData(leftInput, (1, 15, 0)),
      AddData(rightInput, (1, 5, 0)),
      CheckNewAnswer((1, 15L, 5L)))
  }

  test("a key computed before the right input renames its event time bounds the left state") {
    val (leftInput, rightInput, left, right) = watermarkedInputs()
    val renamed = right.withColumn("rs", $"rt" + expr("INTERVAL 10 SECONDS"))
      .withColumnRenamed("rt", "rtime")
    val joined = left.join(renamed, expr("lk = rk AND lt = rs"), "left_outer")
      .select($"lk", $"lt".cast("long"), $"rtime".cast("long"))
    testStream(joined)(
      AddData(leftInput, (1, 15, 0)),
      AddData(rightInput, (1, 5, 0)),
      CheckNewAnswer((1, 15L, 5L)))
  }

  test("the end of a window that the right input adds beside its event time bounds the left " +
      "state") {
    val (leftInput, rightInput, left, right) = watermarkedInputs()
    // Column pruning drops the unreferenced rt, so late-row filtering compares the window.
    val windowed = right.withColumn("rw", window($"rt", "10 seconds"))
    val joined = left.join(windowed, expr("lk = rk AND lt = rw.end"), "left_outer")
      .select($"lk", $"lt".cast("long"), $"rw.end".cast("long"))
    testStream(joined)(
      AddData(leftInput, (1, 10, 0)),
      AddData(rightInput, (1, 5, 0)),
      CheckNewAnswer((1, 10L, 10L)))
  }

  test("a key shifted backward from the right event time does not bound the left state") {
    val (leftInput, _, left, right) = watermarkedInputs()
    val joined = left.join(right, expr("lk = rk AND lt = rt - INTERVAL 10 SECONDS"), "left_outer")
    val e = intercept[AnalysisException] {
      testStream(joined)(
        AddData(leftInput, (1, 5, 0)),
        CheckNewAnswer())
    }
    assert(e.getMessage.contains("without a watermark on the right input that bounds the " +
      "state of the left input"))
    assert(e.getMessage.contains("optionally shifted forward by a constant"))
    // Eviction uses the key and ignores a range condition, so the message does not offer one.
    assert(!e.getMessage.contains("range condition"), e.getMessage)
  }

  // A DataFrame column reference keeps the metadata of the plan it was taken from, but late-row
  // filtering reads the watermark of the joined input itself.
  test("a key reference taken before withWatermark is bounded by the input's watermark") {
    val leftInput = MemoryStream[(Int, Int)]
    val rightInput = MemoryStream[(Int, Int)]
    val left = leftInput.toDF().toDF("lk", "t")
      .select($"lk", timestamp_seconds($"t").as("lt"))
      .withWatermark("lt", "10 seconds")
    val rightWithoutWatermark = rightInput.toDF().toDF("rk", "t")
      .select($"rk", timestamp_seconds($"t").as("rt"))
    val right = rightWithoutWatermark.withWatermark("rt", "10 seconds")
    val joined = left.join(right,
      left("lk") === rightWithoutWatermark("rk") && left("lt") === rightWithoutWatermark("rt"),
      "left_outer")
      .select($"lk", $"lt".cast("long"), $"rt".cast("long"))
    testStream(joined)(
      AddData(leftInput, (1, 5)),
      AddData(rightInput, (1, 5)),
      CheckNewAnswer((1, 5L, 5L)))
  }

  test("a key reference from a watermarked copy does not bound the state of the other input") {
    val leftInput = MemoryStream[(Int, Int)]
    val rightInput = MemoryStream[(Int, Int)]
    val left = leftInput.toDF().toDF("lk", "t")
      .select($"lk", timestamp_seconds($"t").as("lt"))
      .withWatermark("lt", "10 seconds")
    val right = rightInput.toDF().toDF("rk", "t")
      .select($"rk", timestamp_seconds($"t").as("rt"))
    // The joined right input has no watermark, although the reference to rt carries one.
    val watermarkedCopy = right.withWatermark("rt", "10 seconds")
    val joined = left.join(right,
      left("lk") === right("rk") && left("lt") === watermarkedCopy("rt"), "left_outer")
    val e = intercept[AnalysisException] {
      testStream(joined)(
        AddData(leftInput, (1, 5)),
        CheckNewAnswer())
    }
    assert(e.getMessage.contains("without a watermark on the right input that bounds the " +
      "state of the left input"))
  }
}
