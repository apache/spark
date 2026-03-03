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

package org.apache.spark.sql.execution.streaming

import java.io.File

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeq, OffsetSeqLog}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{StreamTest, Trigger}

/**
 * Test suite for automatic V1 to V2 offset log upgrade functionality.
 *
 * Tests the migration from positional offset tracking (OffsetSeq) to named
 * offset tracking (OffsetMap) when users add .name() to their streaming sources.
 */
class OffsetLogV1ToV2UpgradeSuite extends StreamTest with BeforeAndAfter {

  after {
    spark.streams.active.foreach(_.stop())
  }

  /**
   * Helper to write JSON data to a directory for file streaming sources.
   */
  private def writeJsonData(dir: File, data: Seq[String]): Unit = {
    val file = new File(dir, s"data-${System.currentTimeMillis()}.json")
    val writer = new java.io.PrintWriter(file)
    try {
      // scalastyle:off println
      data.foreach(writer.println)
      // scalastyle:on println
    } finally {
      writer.close()
    }
  }

  /**
   * Helper to get the OffsetSeqLog from a checkpoint directory.
   */
  private def getOffsetLog(checkpointPath: String): OffsetSeqLog = {
    new OffsetSeqLog(spark, s"$checkpointPath/offsets")
  }

  /**
   * Helper to create multiple temporary directories for testing.
   */
  private def withTempDirs(f: (File, File, File) => Unit): Unit = {
    withTempDir { dir1 =>
      withTempDir { dir2 =>
        withTempDir { dir3 =>
          f(dir1, dir2, dir3)
        }
      }
    }
  }

  /**
   * Helper for 4-directory variant.
   */
  private def withTempDirs(f: (File, File, File, File) => Unit): Unit = {
    withTempDir { dir1 =>
      withTempDir { dir2 =>
        withTempDir { dir3 =>
          withTempDir { dir4 =>
            f(dir1, dir2, dir3, dir4)
          }
        }
      }
    }
  }

  /**
   * Helper for 5-directory variant.
   */
  private def withTempDirs(f: (File, File, File, File, File) => Unit): Unit = {
    withTempDir { dir1 =>
      withTempDir { dir2 =>
        withTempDir { dir3 =>
          withTempDir { dir4 =>
            withTempDir { dir5 =>
              f(dir1, dir2, dir3, dir4, dir5)
            }
          }
        }
      }
    }
  }

  test("V1 offset log + all sources named auto-upgrades to V2") {
    withTempDirs { (checkpointDir, dataDir1, dataDir2) =>
      // Write initial data
      writeJsonData(dataDir1, Seq("""{"value": 1}""", """{"value": 2}"""))
      writeJsonData(dataDir2, Seq("""{"value": 10}""", """{"value": 20}"""))

      // Step 1: Start with V1 offset log (no names, enforcement disabled)
      withSQLConf(
        SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "1",
        SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "false") {
        val query1 = spark.readStream
          .format("json")
          .schema("value INT")
          .load(dataDir1.getCanonicalPath)
          .union(
            spark.readStream
              .format("json")
              .schema("value INT")
              .load(dataDir2.getCanonicalPath)
          )
          .writeStream
          .format("noop")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .trigger(Trigger.Once())
          .start()

        query1.awaitTermination()

        // Verify V1 was used
        val offsetLog = getOffsetLog(checkpointDir.getCanonicalPath)
        val v1LatestOpt = offsetLog.getLatest()
        assert(v1LatestOpt.isDefined, "Should have offset log entry")
        val (_, v1Offset) = v1LatestOpt.get
        assert(v1Offset.isInstanceOf[OffsetSeq], "Should be using V1 OffsetSeq")
        assert(v1Offset.version == OffsetSeqLog.VERSION_1)
      }

      // Step 2: Restart with named sources + V2 explicitly requested - should auto-upgrade to V2
      withSQLConf(
        SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "2",
        SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true",
        SQLConf.STREAMING_OFFSET_LOG_V1_TO_V2_AUTO_UPGRADE_ENABLED.key -> "true") {
        val query2 = spark.readStream
          .format("json")
          .schema("value INT")
          .name("source_a")
          .load(dataDir1.getCanonicalPath)
          .union(
            spark.readStream
              .format("json")
              .schema("value INT")
              .name("source_b")
              .load(dataDir2.getCanonicalPath)
          )
          .writeStream
          .format("noop")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .trigger(Trigger.Once())
          .start()

        query2.awaitTermination()

        // Verify upgrade occurred
        val offsetLog2 = getOffsetLog(checkpointDir.getCanonicalPath)
        val v2LatestOpt = offsetLog2.getLatest()
        assert(v2LatestOpt.isDefined, "Should have offset log entry after upgrade")
        val (_, v2Offset) = v2LatestOpt.get
        assert(v2Offset.isInstanceOf[OffsetMap], "Should have upgraded to V2 OffsetMap")
        assert(v2Offset.version == OffsetSeqLog.VERSION_2)

        // Verify offsets are keyed by name
        val offsetMap = v2Offset.asInstanceOf[OffsetMap]
        assert(offsetMap.offsetsMap.contains("source_a"), "Should contain source_a")
        assert(offsetMap.offsetsMap.contains("source_b"), "Should contain source_b")
      }

      // Step 3: Add more data and restart again to verify query can continue with new paths
      writeJsonData(dataDir1, Seq("""{"value": 3}""", """{"value": 4}"""))
      writeJsonData(dataDir2, Seq("""{"value": 30}""", """{"value": 40}"""))

      withSQLConf(
        SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "2",
        SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true",
        SQLConf.STREAMING_OFFSET_LOG_V1_TO_V2_AUTO_UPGRADE_ENABLED.key -> "true") {
        val query3 = spark.readStream
          .format("json")
          .schema("value INT")
          .name("source_a")
          .load(dataDir1.getCanonicalPath)
          .union(
            spark.readStream
              .format("json")
              .schema("value INT")
              .name("source_b")
              .load(dataDir2.getCanonicalPath)
          )
          .writeStream
          .format("noop")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .trigger(Trigger.Once())
          .start()

        query3.awaitTermination()

        // Verify query continued with V2 and processed new data
        val offsetLog3 = getOffsetLog(checkpointDir.getCanonicalPath)
        val v2Latest = offsetLog3.getLatest()
        assert(v2Latest.isDefined, "Should have offset log after third run")
        val (latestBatchId, latestOffset) = v2Latest.get
        assert(latestOffset.isInstanceOf[OffsetMap], "Should still be V2")
        assert(latestBatchId >= 2, "Should have processed at least batch 2")
      }
    }
  }

  test("V1 offset log + no sources named continues with V1") {
    withSQLConf(
      SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "1",
      SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "false") {
      withTempDir { checkpointDir =>
        withTempDir { dataDir =>
        writeJsonData(dataDir, Seq("""{"value": 1}""", """{"value": 2}"""))

        // Start with V1, no names
        val query1 = spark.readStream
          .format("json")
          .schema("value INT")
          .load(dataDir.getCanonicalPath)
          .writeStream
          .format("noop")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .trigger(Trigger.Once())
          .start()

        query1.awaitTermination()

        val offsetLog = getOffsetLog(checkpointDir.getCanonicalPath)
        val v1Offset = offsetLog.getLatest().get._2
        assert(v1Offset.isInstanceOf[OffsetSeq])
        assert(v1Offset.version == OffsetSeqLog.VERSION_1)

        // Restart without names - should remain V1
        writeJsonData(dataDir, Seq("""{"value": 3}"""))

        val query2 = spark.readStream
          .format("json")
          .schema("value INT")
          .load(dataDir.getCanonicalPath)
          .writeStream
          .format("noop")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .trigger(Trigger.Once())
          .start()

        query2.awaitTermination()

        val offsetLog2 = getOffsetLog(checkpointDir.getCanonicalPath)
        val v1OffsetAfter = offsetLog2.getLatest().get._2
        assert(v1OffsetAfter.isInstanceOf[OffsetSeq], "Should still be V1")
        assert(v1OffsetAfter.version == OffsetSeqLog.VERSION_1)
        }
      }
    }
  }

  test("Already V2 offset log + named sources continues with V2") {
    withSQLConf(
      SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "2",
      SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
      withTempDir { checkpointDir =>
        withTempDir { dataDir =>
        writeJsonData(dataDir, Seq("""{"value": 1}"""))

        // Start with V2
        val query1 = spark.readStream
          .format("json")
          .schema("value INT")
          .name("my_source")
          .load(dataDir.getCanonicalPath)
          .writeStream
          .format("noop")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .trigger(Trigger.Once())
          .start()

        query1.awaitTermination()

        val offsetLog = getOffsetLog(checkpointDir.getCanonicalPath)
        val v2Offset = offsetLog.getLatest().get._2
        assert(v2Offset.isInstanceOf[OffsetMap])

        // Restart - should remain V2
        writeJsonData(dataDir, Seq("""{"value": 2}"""))

        val query2 = spark.readStream
          .format("json")
          .schema("value INT")
          .name("my_source")
          .load(dataDir.getCanonicalPath)
          .writeStream
          .format("noop")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .trigger(Trigger.Once())
          .start()

        query2.awaitTermination()

        val offsetLog2 = getOffsetLog(checkpointDir.getCanonicalPath)
        val v2OffsetAfter = offsetLog2.getLatest().get._2
        assert(v2OffsetAfter.isInstanceOf[OffsetMap], "Should still be V2")
        }
      }
    }
  }

  test("Multi-source upgrade preserves all offsets correctly") {
    withTempDirs { (checkpointDir, dataDir1, dataDir2, dataDir3) =>
      // Write initial data
      writeJsonData(dataDir1, Seq("""{"value": 1}"""))
      writeJsonData(dataDir2, Seq("""{"value": 2}"""))
      writeJsonData(dataDir3, Seq("""{"value": 3}"""))

      // Start with V1, 3 sources (enforcement disabled)
      withSQLConf(
        SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "1",
        SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "false") {
        val query1 = spark.readStream
          .format("json")
          .schema("value INT")
          .load(dataDir1.getCanonicalPath)
          .union(
            spark.readStream.format("json").schema("value INT")
              .load(dataDir2.getCanonicalPath))
          .union(
            spark.readStream.format("json").schema("value INT")
              .load(dataDir3.getCanonicalPath))
          .writeStream
          .format("noop")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .trigger(Trigger.Once())
          .start()

        query1.awaitTermination()

        val offsetLog = getOffsetLog(checkpointDir.getCanonicalPath)
        val v1Offset = offsetLog.getLatest().get._2.asInstanceOf[OffsetSeq]
        assert(v1Offset.offsets.size == 3, "Should have 3 sources in V1")
      }

      // Restart with all sources named + V2 explicitly requested
      withSQLConf(
        SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "2",
        SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true",
        SQLConf.STREAMING_OFFSET_LOG_V1_TO_V2_AUTO_UPGRADE_ENABLED.key -> "true") {
        val query2 = spark.readStream
          .format("json")
          .schema("value INT")
          .name("payments")
          .load(dataDir1.getCanonicalPath)
          .union(
            spark.readStream
              .format("json")
              .schema("value INT")
              .name("refunds")
              .load(dataDir2.getCanonicalPath)
          )
          .union(
            spark.readStream
              .format("json")
              .schema("value INT")
              .name("adjustments")
              .load(dataDir3.getCanonicalPath)
          )
          .writeStream
          .format("noop")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .trigger(Trigger.Once())
          .start()

        query2.awaitTermination()

        val offsetLog2 = getOffsetLog(checkpointDir.getCanonicalPath)
        val v2Offset = offsetLog2.getLatest().get._2.asInstanceOf[OffsetMap]

        // Verify all three sources are in the map with correct names
        assert(v2Offset.offsetsMap.size == 3, "Should have 3 sources in V2")
        assert(v2Offset.offsetsMap.contains("payments"))
        assert(v2Offset.offsetsMap.contains("refunds"))
        assert(v2Offset.offsetsMap.contains("adjustments"))
      }
    }
  }

  test("Source count mismatch throws clear error") {
    withTempDirs { (checkpointDir, dataDir1, dataDir2, dataDir3) =>
      writeJsonData(dataDir1, Seq("""{"value": 1}"""))
      writeJsonData(dataDir2, Seq("""{"value": 2}"""))

      // Start with 2 sources (enforcement disabled)
      withSQLConf(
        SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "1",
        SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "false") {
        val query1 = spark.readStream
          .format("json")
          .schema("value INT")
          .load(dataDir1.getCanonicalPath)
          .union(
            spark.readStream.format("json").schema("value INT")
              .load(dataDir2.getCanonicalPath))
          .writeStream
          .format("noop")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .trigger(Trigger.Once())
          .start()

        query1.awaitTermination()
      }

      // Try to restart with 3 named sources + V2 requested - should fail
      writeJsonData(dataDir3, Seq("""{"value": 3}"""))

      val e = intercept[Exception] {
        withSQLConf(
          SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "2",
          SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true",
          SQLConf.STREAMING_OFFSET_LOG_V1_TO_V2_AUTO_UPGRADE_ENABLED.key -> "true") {
          val query2 = spark.readStream
            .format("json")
            .schema("value INT")
            .name("a")
            .load(dataDir1.getCanonicalPath)
            .union(
              spark.readStream
                .format("json")
                .schema("value INT")
                .name("b")
                .load(dataDir2.getCanonicalPath)
            )
            .union(
              spark.readStream
                .format("json")
                .schema("value INT")
                .name("c")
                .load(dataDir3.getCanonicalPath)
            )
            .writeStream
            .format("noop")
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .trigger(Trigger.Once())
            .start()

          query2.awaitTermination()
        }
      }

      // The error can be either wrapped in StreamingQueryException or thrown directly
      val errorMessage = e match {
        case sqe: org.apache.spark.sql.streaming.StreamingQueryException =>
          sqe.getCause.getMessage
        case other => other.getMessage
      }

      assert(errorMessage.contains("2") && errorMessage.contains("3"),
        s"Error should mention source count mismatch (2 vs 3): $errorMessage")
    }
  }

  test("V1 offset log + V2 requested without upgrade config throws clear error") {
    withTempDirs { (checkpointDir, dataDir1, dataDir2) =>
      // Write initial data
      writeJsonData(dataDir1, Seq("""{"value": 1}"""))
      writeJsonData(dataDir2, Seq("""{"value": 2}"""))

      // Start with V1 offset log (no names, enforcement disabled)
      withSQLConf(
        SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "1",
        SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "false") {
        val query1 = spark.readStream
          .format("json")
          .schema("value INT")
          .load(dataDir1.getCanonicalPath)
          .union(
            spark.readStream
              .format("json")
              .schema("value INT")
              .load(dataDir2.getCanonicalPath)
          )
          .writeStream
          .format("noop")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .trigger(Trigger.Once())
          .start()

        query1.awaitTermination()
      }

      // Try to restart with named sources + V2 requested but WITHOUT upgrade config
      val e = intercept[Exception] {
        withSQLConf(
          SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "2",
          SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true") {
          val query2 = spark.readStream
            .format("json")
            .schema("value INT")
            .name("source_a")
            .load(dataDir1.getCanonicalPath)
            .union(
              spark.readStream
                .format("json")
                .schema("value INT")
                .name("source_b")
                .load(dataDir2.getCanonicalPath)
            )
            .writeStream
            .format("noop")
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .trigger(Trigger.Once())
            .start()

          query2.awaitTermination()
        }
      }

      // The error can be either wrapped in StreamingQueryException or thrown directly
      val errorMessage = e match {
        case sqe: org.apache.spark.sql.streaming.StreamingQueryException =>
          sqe.getCause.getMessage
        case other => other.getMessage
      }

      // Verify error message contains key guidance
      assert(errorMessage.contains("V1 format") &&
             errorMessage.contains("V2 format was requested") &&
             errorMessage.contains("v1ToV2.autoUpgrade.enabled"),
        s"Error should explain upgrade requirement and how to enable it: $errorMessage")
    }
  }

  test("V1 offset log + V2 requested without named sources upgrades with positional keys") {
    withTempDir { checkpointDir =>
      withTempDir { dataDir =>
        // Write initial data
        writeJsonData(dataDir, Seq("""{"value": 1}"""))

        // Start with V1 offset log, unnamed sources
        withSQLConf(
          SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "1",
          SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "false") {
          val query1 = spark.readStream
            .format("json")
            .schema("value INT")
            .load(dataDir.getCanonicalPath)
            .writeStream
            .format("noop")
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .trigger(Trigger.Once())
            .start()

          query1.awaitTermination()
        }

        // Restart requesting V2 WITHOUT naming sources - should upgrade with positional keys
        writeJsonData(dataDir, Seq("""{"value": 2}"""))

        withSQLConf(
          SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "2",
          SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "false",
          SQLConf.STREAMING_OFFSET_LOG_V1_TO_V2_AUTO_UPGRADE_ENABLED.key -> "true") {
          val query2 = spark.readStream
            .format("json")
            .schema("value INT")
            // Note: NO .name() call - source is unnamed
            .load(dataDir.getCanonicalPath)
            .writeStream
            .format("noop")
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .trigger(Trigger.Once())
            .start()

          query2.awaitTermination()

          // Verify upgrade to V2 happened with positional keys
          val offsetLog = getOffsetLog(checkpointDir.getCanonicalPath)
          val latestOffset = offsetLog.getLatest().get._2
          assert(latestOffset.isInstanceOf[OffsetMap],
            "Should have upgraded to V2 OffsetMap with positional keys")
          assert(latestOffset.version == OffsetSeqLog.VERSION_2)

          // Verify offset is keyed by positional index "0"
          val offsetMap = latestOffset.asInstanceOf[OffsetMap]
          assert(offsetMap.offsetsMap.contains("0"),
            "Should contain positional key '0'")
        }
      }
    }
  }

}
