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

import java.io._
import java.nio.charset.StandardCharsets._

import scala.language.implicitConversions

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.execution.streaming.FakeFileSystem._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test.SharedSQLContext

class CompactibleFileStreamLogSuite extends SparkFunSuite with SharedSQLContext {

  /** To avoid caching of FS objects */
  override protected val sparkConf =
    new SparkConf().set(s"spark.hadoop.fs.$scheme.impl.disable.cache", "true")

  import CompactibleFileStreamLog._

  test("getBatchIdFromFileName") {
    assert(1234L === getBatchIdFromFileName("1234"))
    assert(1234L === getBatchIdFromFileName("1234.compact"))
    intercept[NumberFormatException] {
      getBatchIdFromFileName("1234a")
    }
  }

  test("isCompactionBatch") {
    assert(false === isCompactionBatch(0, compactInterval = 3))
    assert(false === isCompactionBatch(1, compactInterval = 3))
    assert(true === isCompactionBatch(2, compactInterval = 3))
    assert(false === isCompactionBatch(3, compactInterval = 3))
    assert(false === isCompactionBatch(4, compactInterval = 3))
    assert(true === isCompactionBatch(5, compactInterval = 3))
  }

  test("nextCompactionBatchId") {
    assert(2 === nextCompactionBatchId(0, compactInterval = 3))
    assert(2 === nextCompactionBatchId(1, compactInterval = 3))
    assert(5 === nextCompactionBatchId(2, compactInterval = 3))
    assert(5 === nextCompactionBatchId(3, compactInterval = 3))
    assert(5 === nextCompactionBatchId(4, compactInterval = 3))
    assert(8 === nextCompactionBatchId(5, compactInterval = 3))
  }

  test("getValidBatchesBeforeCompactionBatch") {
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(0, compactInterval = 3)
    }
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(1, compactInterval = 3)
    }
    assert(Seq(0, 1) === getValidBatchesBeforeCompactionBatch(2, compactInterval = 3))
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(3, compactInterval = 3)
    }
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(4, compactInterval = 3)
    }
    assert(Seq(2, 3, 4) === getValidBatchesBeforeCompactionBatch(5, compactInterval = 3))
  }

  test("getAllValidBatches") {
    assert(Seq(0) === getAllValidBatches(0, compactInterval = 3))
    assert(Seq(0, 1) === getAllValidBatches(1, compactInterval = 3))
    assert(Seq(2) === getAllValidBatches(2, compactInterval = 3))
    assert(Seq(2, 3) === getAllValidBatches(3, compactInterval = 3))
    assert(Seq(2, 3, 4) === getAllValidBatches(4, compactInterval = 3))
    assert(Seq(5) === getAllValidBatches(5, compactInterval = 3))
    assert(Seq(5, 6) === getAllValidBatches(6, compactInterval = 3))
    assert(Seq(5, 6, 7) === getAllValidBatches(7, compactInterval = 3))
    assert(Seq(8) === getAllValidBatches(8, compactInterval = 3))
  }

  test("batchIdToPath") {
    withSQLConf(SQLConf.FILE_SINK_LOG_COMPACT_INTERVAL.key -> "3") {
      withFileStreamSinkLog { sinkLog =>
        assert("0" === sinkLog.batchIdToPath(0).getName)
        assert("1" === sinkLog.batchIdToPath(1).getName)
        assert("2.compact" === sinkLog.batchIdToPath(2).getName)
        assert("3" === sinkLog.batchIdToPath(3).getName)
        assert("4" === sinkLog.batchIdToPath(4).getName)
        assert("5.compact" === sinkLog.batchIdToPath(5).getName)
      }
    }
  }


}
