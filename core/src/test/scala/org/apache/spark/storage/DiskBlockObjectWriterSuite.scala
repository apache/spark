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
package org.apache.spark.storage

import java.io.{File, InputStream, OutputStream}
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.serializer.{DeserializationStream, JavaSerializer, SerializationStream, Serializer, SerializerInstance, SerializerManager}
import org.apache.spark.util.Utils

class DiskBlockObjectWriterSuite extends SparkFunSuite {

  var tempDir: File = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    tempDir = Utils.createTempDir()
  }

  override def afterEach(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
    } finally {
      super.afterEach()
    }
  }

  private def createWriter(): (DiskBlockObjectWriter, File, ShuffleWriteMetrics) = {
    val file = new File(tempDir, "somefile")
    val conf = new SparkConf()
    val serializerManager = new CustomSerializerManager(new JavaSerializer(conf), conf, None)
    val writeMetrics = new ShuffleWriteMetrics()
    val writer = new DiskBlockObjectWriter(
      file,
      serializerManager,
      new CustomJavaSerializer(new SparkConf()).newInstance(),
      1024,
      true,
      writeMetrics)
    (writer, file, writeMetrics)
  }

  test("verify write metrics") {
    val (writer, file, writeMetrics) = createWriter()

    writer.write(Long.box(20), Long.box(30))
    // Record metrics update on every write
    assert(writeMetrics.recordsWritten === 1)
    // Metrics don't update on every write
    assert(writeMetrics.bytesWritten == 0)
    // After 16384 writes, metrics should update
    for (i <- 0 until 16384) {
      writer.flush()
      writer.write(Long.box(i), Long.box(i))
    }
    assert(writeMetrics.bytesWritten > 0)
    assert(writeMetrics.recordsWritten === 16385)
    writer.commitAndGet()
    writer.close()
    assert(file.length() == writeMetrics.bytesWritten)
  }

  test("verify write metrics on revert") {
    val (writer, _, writeMetrics) = createWriter()

    writer.write(Long.box(20), Long.box(30))
    // Record metrics update on every write
    assert(writeMetrics.recordsWritten === 1)
    // Metrics don't update on every write
    assert(writeMetrics.bytesWritten == 0)
    // After 16384 writes, metrics should update
    for (i <- 0 until 16384) {
      writer.flush()
      writer.write(Long.box(i), Long.box(i))
    }
    assert(writeMetrics.bytesWritten > 0)
    assert(writeMetrics.recordsWritten === 16385)
    writer.revertPartialWritesAndClose()
    assert(writeMetrics.bytesWritten == 0)
    assert(writeMetrics.recordsWritten == 0)
  }

  test("Reopening a closed block writer") {
    val (writer, _, _) = createWriter()

    writer.open()
    writer.close()
    intercept[SparkException] {
      writer.open()
    }
  }

  test("calling revertPartialWritesAndClose() on a partial write should truncate up to commit") {
    val (writer, file, writeMetrics) = createWriter()

    writer.write(Long.box(20), Long.box(30))
    val firstSegment = writer.commitAndGet()
    assert(firstSegment.length === file.length())
    assert(writeMetrics.bytesWritten === file.length())

    writer.write(Long.box(40), Long.box(50))

    writer.revertPartialWritesAndClose()
    assert(firstSegment.length === file.length())
    assert(writeMetrics.bytesWritten === file.length())
    assert(writeMetrics.recordsWritten == 1)
  }

  test("calling revertPartialWritesAndClose() after commit() should have no effect") {
    val (writer, file, writeMetrics) = createWriter()

    writer.write(Long.box(20), Long.box(30))
    val firstSegment = writer.commitAndGet()
    assert(firstSegment.length === file.length())
    assert(writeMetrics.bytesWritten === file.length())

    writer.revertPartialWritesAndClose()
    assert(firstSegment.length === file.length())
    assert(writeMetrics.bytesWritten === file.length())
  }

  test("calling revertPartialWritesAndClose() on a closed block writer should have no effect") {
    val (writer, file, writeMetrics) = createWriter()
    for (i <- 1 to 1000) {
      writer.write(i, i)
    }
    writer.commitAndGet()
    writer.close()
    val bytesWritten = writeMetrics.bytesWritten
    assert(writeMetrics.recordsWritten === 1000)
    writer.revertPartialWritesAndClose()
    assert(writeMetrics.recordsWritten === 1000)
    assert(writeMetrics.bytesWritten === bytesWritten)
  }

  test("commit() and close() should be idempotent") {
    val (writer, file, writeMetrics) = createWriter()
    for (i <- 1 to 1000) {
      writer.write(i, i)
    }
    writer.commitAndGet()
    writer.close()
    val bytesWritten = writeMetrics.bytesWritten
    val writeTime = writeMetrics.writeTime
    assert(writeMetrics.recordsWritten === 1000)
    writer.commitAndGet()
    writer.close()
    assert(writeMetrics.recordsWritten === 1000)
    assert(writeMetrics.bytesWritten === bytesWritten)
    assert(writeMetrics.writeTime === writeTime)
  }

  test("revertPartialWritesAndClose() should be idempotent") {
    val (writer, file, writeMetrics) = createWriter()
    for (i <- 1 to 1000) {
      writer.write(i, i)
    }
    writer.revertPartialWritesAndClose()
    val bytesWritten = writeMetrics.bytesWritten
    val writeTime = writeMetrics.writeTime
    assert(writeMetrics.recordsWritten === 0)
    writer.revertPartialWritesAndClose()
    assert(writeMetrics.recordsWritten === 0)
    assert(writeMetrics.bytesWritten === bytesWritten)
    assert(writeMetrics.writeTime === writeTime)
  }

  test("commit() and close() without ever opening or writing") {
    val (writer, _, _) = createWriter()
    val segment = writer.commitAndGet()
    writer.close()
    assert(segment.length === 0)
  }

  test("calling closeAndDelete() on a partial write file") {
    val (writer, file, writeMetrics) = createWriter()

    for (i <- 1 to 1000) {
      writer.write(i, i)
    }
    val firstSegment = writer.commitAndGet()
    assert(firstSegment.length === file.length())
    assert(writeMetrics.bytesWritten === file.length())

    for (i <- 1 to 500) {
      writer.write(i, i)
    }

    val bs = writer.getSerializerWrappedStream.asInstanceOf[OutputStreamWithCloseDetecting]
    val objOut = writer.getSerializationStream.asInstanceOf[SerializationStreamWithCloseDetecting]

    writer.closeAndDelete()
    assert(!file.exists())
    assert(writeMetrics.bytesWritten == 0)
    assert(writeMetrics.recordsWritten == 0)
    assert(bs.isClosed)
    assert(objOut.isClosed)
  }
}

trait CloseDetecting {
  var isClosed = false
}

class OutputStreamWithCloseDetecting(outputStream: OutputStream)
    extends OutputStream
    with CloseDetecting {
  override def write(b: Int): Unit = outputStream.write(b)

  override def close(): Unit = {
    isClosed = true
    outputStream.close()
  }
}

class CustomSerializerManager(
    defaultSerializer: Serializer,
    conf: SparkConf,
    encryptionKey: Option[Array[Byte]])
    extends SerializerManager(defaultSerializer, conf, encryptionKey) {
  override def wrapStream(blockId: BlockId, s: OutputStream): OutputStream = {
    new OutputStreamWithCloseDetecting(wrapForCompression(blockId, wrapForEncryption(s)))
  }
}

class CustomJavaSerializer(conf: SparkConf) extends JavaSerializer(conf) {

  override def newInstance(): SerializerInstance = {
    new CustomJavaSerializerInstance(super.newInstance())
  }
}

class SerializationStreamWithCloseDetecting(serializationStream: SerializationStream)
    extends SerializationStream with CloseDetecting {

  override def close(): Unit = {
    isClosed = true
    serializationStream.close()
  }

  override def writeObject[T: ClassTag](t: T): SerializationStream =
    serializationStream.writeObject(t)

  override def flush(): Unit = serializationStream.flush()
}

class CustomJavaSerializerInstance(instance: SerializerInstance) extends SerializerInstance {
  override def serializeStream(s: OutputStream): SerializationStream =
    new SerializationStreamWithCloseDetecting(instance.serializeStream(s))

  override def serialize[T: ClassTag](t: T): ByteBuffer = instance.serialize(t)

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = instance.deserialize(bytes)

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    instance.deserialize(bytes, loader)

  override def deserializeStream(s: InputStream): DeserializationStream =
    instance.deserializeStream(s)
}
