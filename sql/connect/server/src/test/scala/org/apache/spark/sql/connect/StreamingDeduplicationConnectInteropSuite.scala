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

package org.apache.spark.sql.connect

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * Interop tests for dropDuplicates key resolution across Spark Classic and Spark Connect. Both
 * engines now resolve the dedup keys through the same analyzer rule with a deterministic
 * (positional) order, so a checkpoint written by one engine must be restartable under the other:
 * the state-store keys (bound by position) line up and previously-seen rows are still recognized
 * as duplicates.
 *
 * The in-process [[SparkConnectServerTest]] harness exposes a Classic session (`spark`) and a
 * Connect session (`withSession`) sharing one SparkContext / filesystem, so both can drive the
 * same checkpoint directory. Input is plain CSV files (no commit markers) read by a file source
 * with Trigger.AvailableNow for deterministic batching; the sink is a recoverable parquet file
 * sink. See SPARK-XXXXX.
 */
class StreamingDeduplicationConnectInteropSuite extends SparkConnectServerTest {

  private val rowSchema = StructType(Seq(
    StructField("a", IntegerType),
    StructField("b", IntegerType),
    StructField("c", IntegerType),
    StructField("d", IntegerType),
    StructField("e", IntegerType)))

  /** Writes one CSV file (one batch worth of input) into `inputDir` without any commit markers. */
  private def writeInputCsv(inputDir: File, rows: Seq[(Int, Int, Int, Int, Int)]): Unit = {
    inputDir.mkdirs()
    val content =
      rows.map { case (a, b, c, d, e) => s"$a,$b,$c,$d,$e" }.mkString("", "\n", "\n")
    Files.write(
      new File(inputDir, s"${UUID.randomUUID()}.csv").toPath,
      content.getBytes(StandardCharsets.UTF_8))
  }

  /**
   * Starts a dropDuplicates streaming query on `session` reading `inputDir`, processes all
   * currently-available files against `checkpoint`, appending deduplicated rows to the parquet
   * sink at `outputDir`.
   */
  // Fully qualified on purpose: this file lives in package org.apache.spark.sql.connect, which also
  // defines a `SparkSession`; an import of the common type is shadowed by it under Scala 2.12. The
  // common base accepts both the Classic `spark` and the Connect `withSession` session.
  private def runDedup(
      session: org.apache.spark.sql.SparkSession,
      inputDir: File,
      checkpoint: File,
      outputDir: File): Unit = {
    val query = session.readStream
      .schema(rowSchema)
      .csv(inputDir.getCanonicalPath)
      .dropDuplicates()
      .writeStream
      .format("parquet")
      .option("path", outputDir.getCanonicalPath)
      .option("checkpointLocation", checkpoint.getCanonicalPath)
      .trigger(Trigger.AvailableNow())
      .start()
    try {
      query.processAllAvailable()
    } finally {
      query.stop()
    }
  }

  /** Reads every row the sink has emitted so far (across runs), sorted for stable comparison. */
  private def readOutput(outputDir: File): Seq[Seq[Int]] =
    spark.read
      .schema(rowSchema)
      .parquet(outputDir.getCanonicalPath)
      .collect()
      .map(r => (0 until 5).map(r.getInt).toList)
      .toSeq
      .sortBy(_.mkString(","))

  test("SPARK-XXXXX: dropDuplicates checkpoint written by Classic restarts under Connect") {
    withTempDir { dir =>
      val inputDir = new File(dir, "input")
      val outputDir = new File(dir, "output")
      val checkpoint = new File(dir, "checkpoint")

      // Run 1 on Classic seeds the dedup state with one row.
      writeInputCsv(inputDir, Seq((1, 2, 3, 4, 5)))
      runDedup(spark, inputDir, checkpoint, outputDir)

      // Run 2 on Connect restarts the same checkpoint with a duplicate + a new row. The duplicate
      // must be recognized against the Classic-created state and dropped; only the new row is
      // emitted. A cross-engine key-order mismatch would re-emit the duplicate.
      writeInputCsv(inputDir, Seq((1, 2, 3, 4, 5), (6, 7, 8, 9, 10)))
      withSession { session =>
        runDedup(session, inputDir, checkpoint, outputDir)
      }

      assert(readOutput(outputDir) === Seq(Seq(1, 2, 3, 4, 5), Seq(6, 7, 8, 9, 10)))
    }
  }

  test("SPARK-XXXXX: dropDuplicates checkpoint written by Connect restarts under Classic") {
    withTempDir { dir =>
      val inputDir = new File(dir, "input")
      val outputDir = new File(dir, "output")
      val checkpoint = new File(dir, "checkpoint")

      writeInputCsv(inputDir, Seq((1, 2, 3, 4, 5)))
      withSession { session =>
        runDedup(session, inputDir, checkpoint, outputDir)
      }

      writeInputCsv(inputDir, Seq((1, 2, 3, 4, 5), (6, 7, 8, 9, 10)))
      runDedup(spark, inputDir, checkpoint, outputDir)

      assert(readOutput(outputDir) === Seq(Seq(1, 2, 3, 4, 5), Seq(6, 7, 8, 9, 10)))
    }
  }
}
