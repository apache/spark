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
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.Channels

import scala.jdk.CollectionConverters._
import scala.util.Random

import org.apache.arrow.compression.ZstdCompressionCodec
import org.apache.arrow.memory.{ArrowBuf, BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{VarCharVector, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.compression.NoCompressionCodec
import org.apache.arrow.vector.ipc.WriteChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.Utils

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

  // Expose the protected `doCompress` so that a test can look at the raw output buffer.
  private class RawBoundedZstdCompressionCodec extends BoundedZstdCompressionCodec(3) {
    def compressRaw(allocator: BufferAllocator, in: ArrowBuf): ArrowBuf = doCompress(allocator, in)
  }

  // Arrow's own codec, which up to arrow-java 19.0 gives zstd a destination capacity that is 8
  // bytes too large.
  private class RawStockZstdCompressionCodec extends ZstdCompressionCodec(3) {
    def compressRaw(allocator: BufferAllocator, in: ArrowBuf): ArrowBuf = doCompress(allocator, in)
  }

  // Random bytes with about 2% of them zeroed. Pure random bytes only make zstd fill its whole
  // destination when one byte value happens to be frequent enough (more than about n / 128 + 4
  // occurrences) for zstd to try Huffman-coding the literals and give up. Zeroing some bytes
  // makes that the usual case instead of a property of the seed.
  private def noisyBytes(random: Random, n: Int): Array[Byte] = {
    val bytes = random.nextBytes(n)
    (0 until n).foreach { i => if (random.nextInt(50) == 0) bytes(i) = 0 }
    bytes
  }

  // Runs `compressRaw` into a buffer that is followed by sentinel bytes, and returns whether the
  // sentinel was overwritten, i.e. whether zstd wrote past the end of the buffer.
  private def writesPastEnd(
      compressRaw: (BufferAllocator, ArrowBuf) => ArrowBuf,
      data: Array[Byte]): Boolean = {
    val guardSize = 64
    val guardByte = 0x5A.toByte
    var requestedSize = 0L
    val guardedAllocator = new RootAllocator(Long.MaxValue) {
      override def buffer(size: Long): ArrowBuf = {
        requestedSize = size
        val buf = super.buffer(size + guardSize)
        buf.setBytes(size, Array.fill(guardSize)(guardByte))
        buf
      }
    }
    val inputAllocator =
      ArrowUtils.rootAllocator.newChildAllocator(this.getClass.getSimpleName, 0, Long.MaxValue)
    Utils.tryWithSafeFinally {
      val input = inputAllocator.buffer(data.length)
      val out = try {
        input.setBytes(0, data)
        input.writerIndex(data.length)
        compressRaw(guardedAllocator, input)
      } finally {
        input.close()
      }
      try {
        val guard = new Array[Byte](guardSize)
        out.getBytes(requestedSize, guard)
        !guard.forall(_ == guardByte)
      } finally {
        out.close()
      }
    } {
      Utils.tryWithSafeFinally(guardedAllocator.close())(inputAllocator.close())
    }
  }

  test("SPARK-60707: zstd compression does not write past the end of its output buffer") {
    val bounded = new RawBoundedZstdCompressionCodec
    val stock = new RawStockZstdCompressionCodec
    val random = new Random(0)
    // Of these inputs, only those of roughly 280 to 1000 bytes make zstd fill its whole
    // destination; smaller and larger ones stop short of the end. The sizes in that range are
    // checked with many inputs each, and the other sizes only as smoke checks.
    Seq(280, 500, 700).foreach { n =>
      val inputs = Seq.fill(100)(noisyBytes(random, n))
      inputs.foreach { data =>
        assert(!writesPastEnd(bounded.compressRaw, data),
          s"zstd wrote past the end of its output buffer (input size $n)")
      }
      // Control: these inputs do trigger the overrun in Arrow's own codec, so the checks above
      // can fail. This depends on how zstd compresses literals. If it starts failing after an
      // Arrow upgrade, Arrow has fixed the codec and BoundedZstdCompressionCodec can be removed;
      // if it starts failing after a zstd-jni upgrade, pick inputs that still overrun instead.
      assert(inputs.exists(writesPastEnd(stock.compressRaw, _)),
        s"no input of size $n made Arrow's ZstdCompressionCodec write past its output buffer")
    }
    Seq(1, 7, 100, 4096, 65536, 262144).foreach { n =>
      assert(!writesPastEnd(bounded.compressRaw, noisyBytes(random, n)),
        s"zstd wrote past the end of its output buffer (input size $n)")
    }
  }

  test("SPARK-60707: zstd codec round-trips compressible and incompressible buffers") {
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator(this.getClass.getSimpleName, 0, Long.MaxValue)
    val codec = ArrowCompressionUtils.createCompressionCodec("zstd", 3)
    val random = new Random(1)
    Utils.tryWithSafeFinally {
      // The write paths get the bounded codec, not Arrow's own.
      assert(codec.isInstanceOf[BoundedZstdCompressionCodec])
      Seq(1, 100, 4096, 100000).foreach { n =>
        val compressible = Array.tabulate[Byte](n)(i => (i % 7).toByte)
        val incompressible = random.nextBytes(n)
        Seq(compressible -> (n >= 100), incompressible -> false).foreach {
          case (expected, expectZstdFrame) =>
            val input = allocator.buffer(n)
            input.setBytes(0, expected)
            input.writerIndex(n)
            val compressed = codec.compress(allocator, input)
            // The buffer starts with the uncompressed length, or -1 if Arrow kept the input as is
            // because zstd did not make it smaller. Check which path each case takes.
            val prefix = compressed.nioBuffer(0, 8).order(ByteOrder.LITTLE_ENDIAN).getLong(0)
            assert(prefix === (if (expectZstdFrame) n.toLong else -1L))
            val decompressed = codec.decompress(allocator, compressed)
            try {
              val actual = new Array[Byte](n)
              decompressed.getBytes(0, actual)
              assert(actual.sameElements(expected))
            } finally {
              decompressed.close()
            }
        }
      }
    } {
      allocator.close()
    }
  }

  test("SPARK-60707: batches written through VectorUnloader keep their length prefixes") {
    // Writes batches the way ArrowConverters does, with one reused root, and checks the length
    // prefix of every data buffer. An overrun of one buffer can overwrite the prefix of the next
    // buffer in the same batch, which makes the batch fail to decompress when it is read. How
    // often that happens depends on the memory allocator's layout, so this cannot replace the
    // guard test above, but it covers the path that readers see.
    val rows = 55
    val columns = 3
    val sparkSchema = StructType(
      (0 until columns).map(i => StructField(s"c$i", LongType, nullable = false)))
    val arrowSchema = ArrowUtils.toArrowSchema(sparkSchema, "UTC", true, false)
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator(this.getClass.getSimpleName, 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val writer = ArrowWriter.create(root)
    val unloader = new VectorUnloader(
      root, true, ArrowCompressionUtils.createCompressionCodec("zstd", 3), true)
    val random = new Random(2)
    Utils.tryWithSafeFinally {
      (0 until 300).foreach { _ =>
        val values = ByteBuffer.wrap(noisyBytes(random, rows * columns * 8))
          .order(ByteOrder.LITTLE_ENDIAN).asLongBuffer()
        (0 until rows).foreach { _ =>
          writer.write(InternalRow.fromSeq(Seq.fill(columns)(values.get())))
        }
        writer.finish()
        val batch = unloader.getRecordBatch()
        try {
          // The buffers of a column are its validity buffer and then its data buffer.
          (0 until columns).foreach { c =>
            val dataBuffer = batch.getBuffers.get(2 * c + 1)
            val prefix = dataBuffer.nioBuffer(0, 8).order(ByteOrder.LITTLE_ENDIAN).getLong(0)
            assert(prefix === -1L || prefix === rows * 8L,
              s"the length prefix of column $c was overwritten")
          }
        } finally {
          batch.close()
        }
        writer.reset()
      }
    } {
      root.close()
      allocator.close()
    }
  }

  test("SPARK-60776: unloader releases the buffers of a batch it fails to unload") {
    var calls = 0
    val codec = new BoundedZstdCompressionCodec(3) {
      override protected def doCompress(allocator: BufferAllocator, input: ArrowBuf): ArrowBuf = {
        calls += 1
        if (calls == 4) throw new IllegalStateException("compression failed")
        super.doCompress(allocator, input)
      }
    }
    val arrowSchema = ArrowUtils.toArrowSchema(
      StructType(Seq(StructField("c", LongType))), "UTC", true, false)
    val allocator =
      ArrowUtils.rootAllocator.newChildAllocator(this.getClass.getSimpleName, 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val writer = ArrowWriter.create(root)
    val unloader = ArrowCompressionUtils.createVectorUnloader(root, codec)
    Utils.tryWithSafeFinally {
      writer.write(InternalRow(1L))
      writer.finish()
      // A batch compresses a validity buffer and a data buffer, so the first batch succeeds and the
      // second fails holding its compressed validity buffer and its retained data buffer.
      val batch = unloader.getRecordBatch()
      Utils.tryWithSafeFinally {
        val e = intercept[IllegalStateException](unloader.getRecordBatch())
        assert(e.getMessage === "compression failed")
        assert(batch.getBuffers.asScala.forall(_.refCnt() == 1))
      } {
        batch.close()
      }
    } {
      // Closing the allocator fails if a buffer is left behind. If closing the root fails after an
      // Arrow upgrade because a reference count went negative, Arrow releases these buffers itself
      // and ReleasingVectorUnloader must be removed.
      root.close()
      allocator.close()
    }
  }
}
