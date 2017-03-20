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

import java.io.{FileOutputStream, IOException, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode

import scala.reflect.ClassTag

import com.google.common.io.{ByteStreams, Closeables}
import org.apache.commons.io.IOUtils

import org.apache.spark.internal.Logging
import org.apache.spark.SparkConf
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.util.{ByteBufferInputStream, ByteBufferOutputStream, Utils}
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Stores BlockManager blocks on disk.
 */
private[spark] class DiskStore(
    conf: SparkConf,
    serializerManager: SerializerManager,
    diskManager: DiskBlockManager) extends Logging {

  private val minMemoryMapBytes = conf.getSizeAsBytes("spark.storage.memoryMapThreshold", "2m")

  def getSize(blockId: BlockId): Long = {
    diskManager.getFile(blockId.name).length
  }

  /**
   * Invokes the provided callback function to write the specific block.
   *
   * @throws IllegalStateException if the block already exists in the disk store.
   */
  def put(blockId: BlockId)(writeFunc: FileOutputStream => Unit): Unit = {
    if (contains(blockId)) {
      throw new IllegalStateException(s"Block $blockId is already present in the disk store")
    }
    logDebug(s"Attempting to put block $blockId")
    val startTime = System.currentTimeMillis
    val file = diskManager.getFile(blockId)
    val fileOutputStream = new FileOutputStream(file)
    var threwException: Boolean = true
    try {
      writeFunc(fileOutputStream)
      threwException = false
    } finally {
      try {
        Closeables.close(fileOutputStream, threwException)
      } finally {
         if (threwException) {
          remove(blockId)
        }
      }
    }
    val finishTime = System.currentTimeMillis
    logDebug("Block %s stored as %s file on disk in %d ms".format(
      file.getName,
      Utils.bytesToString(file.length()),
      finishTime - startTime))
  }

  def putBytes(blockId: BlockId, bytes: ChunkedByteBuffer): Unit = {
    val bytesToStore = if (serializerManager.encryptionEnabled) {
      try {
        val data = bytes.toByteBuffer
        val in = new ByteBufferInputStream(data, true)
        val byteBufOut = new ByteBufferOutputStream(data.remaining())
        val out = CryptoStreamUtils.createCryptoOutputStream(byteBufOut, conf,
          serializerManager.encryptionKey.get)
        try {
          ByteStreams.copy(in, out)
        } finally {
          in.close()
          out.close()
        }
        new ChunkedByteBuffer(byteBufOut.toByteBuffer)
      } finally {
        bytes.dispose()
      }
    } else {
      bytes
    }

    put(blockId) { fileOutputStream =>
      val channel = fileOutputStream.getChannel
      Utils.tryWithSafeFinally {
        bytesToStore.writeFully(channel)
      } {
        channel.close()
      }
    }
  }

  def getBytes(blockId: BlockId): ChunkedByteBuffer = {
    val bytes = readBytes(blockId)

    val in = serializerManager.wrapForEncryption(bytes.toInputStream(dispose = true))
    new ChunkedByteBuffer(ByteBuffer.wrap(IOUtils.toByteArray(in)))
  }

  def getBytesAsValues[T](blockId: BlockId, classTag: ClassTag[T]): Iterator[T] = {
    val bytes = readBytes(blockId)

    serializerManager
      .dataDeserializeStream(blockId, bytes.toInputStream(dispose = true))(classTag)
  }

  private[storage] def readBytes(blockId: BlockId): ChunkedByteBuffer = {
    val file = diskManager.getFile(blockId.name)
    val channel = new RandomAccessFile(file, "r").getChannel
    Utils.tryWithSafeFinally {
      // For small files, directly read rather than memory map
      if (file.length < minMemoryMapBytes) {
        val buf = ByteBuffer.allocate(file.length.toInt)
        channel.position(0)
        while (buf.remaining() != 0) {
          if (channel.read(buf) == -1) {
            throw new IOException("Reached EOF before filling buffer\n" +
              s"offset=0\nfile=${file.getAbsolutePath}\nbuf.remaining=${buf.remaining}")
          }
        }
        buf.flip()
        new ChunkedByteBuffer(buf)
      } else {
        new ChunkedByteBuffer(channel.map(MapMode.READ_ONLY, 0, file.length))
      }
    } {
      channel.close()
    }
  }

  def remove(blockId: BlockId): Boolean = {
    val file = diskManager.getFile(blockId.name)
    if (file.exists()) {
      val ret = file.delete()
      if (!ret) {
        logWarning(s"Error deleting ${file.getPath()}")
      }
      ret
    } else {
      false
    }
  }

  def contains(blockId: BlockId): Boolean = {
    val file = diskManager.getFile(blockId.name)
    file.exists()
  }
}
