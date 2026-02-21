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

import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetSeqBase, OffsetSeqLog, OffsetSeqMetadata}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for the spark.sql.function.protobufExtensions.enabled flag and its persistence in the
 * offset log.
 */
class ProtobufExtensionsSupportOffsetLogSuite extends SharedSparkSession {

  test("Protobuf extensions support disabled by default for new queries") {
    val offsetSeqMetadata =
      OffsetSeqMetadata(batchWatermarkMs = 0, batchTimestampMs = 0, spark.conf)
    assert(
      offsetSeqMetadata.conf.get(SQLConf.PROTOBUF_EXTENSIONS_SUPPORT_ENABLED.key) ===
        Some(false.toString))
  }

  test("Protobuf extensions support enabled when session sets it to true") {
    val protobufExtConf = SQLConf.PROTOBUF_EXTENSIONS_SUPPORT_ENABLED.key
    withSQLConf(protobufExtConf -> true.toString) {
      val offsetSeqMetadata =
        OffsetSeqMetadata(batchWatermarkMs = 0, batchTimestampMs = 0, spark.conf)
      assert(offsetSeqMetadata.conf.get(protobufExtConf) === Some(true.toString))
    }
  }

  test(
    "Protobuf extensions support uses default false for old checkpoint when enabled in session") {
    val protobufExtConf = SQLConf.PROTOBUF_EXTENSIONS_SUPPORT_ENABLED.key
    withSQLConf(protobufExtConf -> true.toString) {
      val existingChkpt = "offset-log-version-2.1.0"
      val (_, offsetSeq) = readFromResource(existingChkpt)
      val offsetSeqMetadata = offsetSeq.metadataOpt.get
      // Not present in existing checkpoint
      assert(offsetSeqMetadata.conf.get(protobufExtConf) === None)

      val clonedSqlConf = spark.sessionState.conf.clone()
      OffsetSeqMetadata.setSessionConf(offsetSeqMetadata, clonedSqlConf)
      assert(!clonedSqlConf.getConf(SQLConf.PROTOBUF_EXTENSIONS_SUPPORT_ENABLED))
    }
  }

  test(
    "Protobuf extensions support uses default false for old checkpoint when unset in session") {
    val existingChkpt = "offset-log-version-2.1.0"
    val (_, offsetSeq) = readFromResource(existingChkpt)
    val offsetSeqMetadata = offsetSeq.metadataOpt.get
    val protobufExtConf = SQLConf.PROTOBUF_EXTENSIONS_SUPPORT_ENABLED.key
    // Not present in existing checkpoint
    assert(offsetSeqMetadata.conf.get(protobufExtConf) === None)

    val clonedSqlConf = spark.sessionState.conf.clone()
    OffsetSeqMetadata.setSessionConf(offsetSeqMetadata, clonedSqlConf)
    assert(!clonedSqlConf.getConf(SQLConf.PROTOBUF_EXTENSIONS_SUPPORT_ENABLED))
  }

  test("protobuf extensions support in existing checkpoint takes precedence over session value") {
    val protobufExtConf = SQLConf.PROTOBUF_EXTENSIONS_SUPPORT_ENABLED.key

    // Enabled when checkpoint = enabled and session = disabled
    withSQLConf(protobufExtConf -> false.toString) {
      val offsetSeqMetadata = OffsetSeqMetadata(
        batchWatermarkMs = 0,
        batchTimestampMs = 0,
        Map(protobufExtConf -> true.toString))

      val clonedSqlConf = spark.sessionState.conf.clone()
      OffsetSeqMetadata.setSessionConf(offsetSeqMetadata, clonedSqlConf)
      assert(clonedSqlConf.getConf(SQLConf.PROTOBUF_EXTENSIONS_SUPPORT_ENABLED))
    }

    // Disabled when checkpoint = disabled and session = enabled
    withSQLConf(protobufExtConf -> true.toString) {
      val offsetSeqMetadata = OffsetSeqMetadata(
        batchWatermarkMs = 0,
        batchTimestampMs = 0,
        Map(protobufExtConf -> false.toString))

      val clonedSqlConf = spark.sessionState.conf.clone()
      OffsetSeqMetadata.setSessionConf(offsetSeqMetadata, clonedSqlConf)
      assert(!clonedSqlConf.getConf(SQLConf.PROTOBUF_EXTENSIONS_SUPPORT_ENABLED))
    }
  }

  private def readFromResource(dir: String): (Long, OffsetSeqBase) = {
    val input = getClass.getResource(s"/structured-streaming/$dir")
    val log = new OffsetSeqLog(spark, input.toString)
    log.getLatest().get
  }
}
