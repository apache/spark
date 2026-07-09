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

package org.apache.spark.sql.execution.arrow

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels

import org.apache.arrow.vector.{VarCharVector, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.compression.NoCompressionCodec
import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils

class ArrowCompressionUtilsSuite extends SparkFunSuite {

  // Serializes one Arrow record batch compressed at the given zstd level and returns its size.
  private def compressedSize(level: Int): Int = {
    val sparkSchema = StructType(Seq(StructField("str_col", StringType)))
    val arrowSchema = ArrowUtils.toArrowSchema(sparkSchema, "UTC", true, false)
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator(this.getClass.getSimpleName, 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    try {
      root.allocateNew()
      val strVector = root.getVector("str_col").asInstanceOf[VarCharVector]
      // Compressible but non-trivial corpus: shared structure with per-row variation, so
      // different zstd levels produce measurably different output sizes.
      (0 until 2000).foreach { i =>
        val value =
          s"user-$i@example.com,record-${i % 97},payload-${i * 2654435761L}".getBytes("UTF-8")
        strVector.setSafe(i, value, 0, value.length)
      }
      root.setRowCount(2000)
      val codec = ArrowCompressionUtils.createCompressionCodec("zstd", level)
      val recordBatch = new VectorUnloader(root, true, codec, true).getRecordBatch()
      try {
        val out = new ByteArrayOutputStream()
        MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), recordBatch)
        out.size()
      } finally {
        recordBatch.close()
      }
    } finally {
      root.close()
      allocator.close()
    }
  }

  test("SPARK-57383: zstd compression level is honored by createCompressionCodec") {
    // Regression test: the codec used to be rebuilt through the single-argument factory
    // overload, which silently dropped the level, so every configured level compressed at the
    // zstd default. Compress the same batch at an ultra-fast negative level and at a high level
    // and assert the high level yields a strictly smaller payload.
    val fastSize = compressedSize(-5)
    val highSize = compressedSize(19)
    assert(highSize < fastSize,
      s"zstd level 19 should compress smaller than level -5, " +
        s"got level 19 -> $highSize bytes vs level -5 -> $fastSize bytes; " +
        "equal sizes mean the configured level is being ignored")
  }

  test("codec name 'none' maps to the no-op codec and unknown names fail") {
    assert(ArrowCompressionUtils.createCompressionCodec("none", 3) ===
      NoCompressionCodec.INSTANCE)
    val e = intercept[SparkException] {
      ArrowCompressionUtils.createCompressionCodec("snappy", 3)
    }
    assert(e.getMessage.contains("Unsupported Arrow compression codec: snappy"))
  }
}
