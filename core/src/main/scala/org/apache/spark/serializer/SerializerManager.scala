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

package org.apache.spark.serializer

import java.io.{BufferedInputStream, BufferedOutputStream, InputStream, OutputStream}

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.network.buffer.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.storage._

/**
 * Component which configures serialization, compression and encryption for various Spark
 * components, including automatic selection of which [[Serializer]] to use for shuffles.
 */
private[spark] class SerializerManager(defaultSerializer: Serializer, conf: SparkConf) {

  private[this] val kryoSerializer = new KryoSerializer(conf)

  private[this] val stringClassTag: ClassTag[String] = implicitly[ClassTag[String]]
  private[this] val primitiveAndPrimitiveArrayClassTags: Set[ClassTag[_]] = {
    val primitiveClassTags = Set[ClassTag[_]](
      ClassTag.Boolean,
      ClassTag.Byte,
      ClassTag.Char,
      ClassTag.Double,
      ClassTag.Float,
      ClassTag.Int,
      ClassTag.Long,
      ClassTag.Null,
      ClassTag.Short
    )
    val arrayClassTags = primitiveClassTags.map(_.wrap)
    primitiveClassTags ++ arrayClassTags
  }

  // Whether to compress broadcast variables that are stored
  private[this] val compressBroadcast = conf.getBoolean("spark.broadcast.compress", true)
  // Whether to compress shuffle output that are stored
  private[this] val compressShuffle = conf.getBoolean("spark.shuffle.compress", true)
  // Whether to compress RDD partitions that are stored serialized
  private[this] val compressRdds = conf.getBoolean("spark.rdd.compress", false)
  // Whether to compress shuffle output temporarily spilled to disk
  private[this] val compressShuffleSpill = conf.getBoolean("spark.shuffle.spill.compress", true)

  // Whether to enable IO encryption
  private[this] val enableIOEncryption = conf.get(IO_ENCRYPTION_ENABLED)

  /* The compression codec to use. Note that the "lazy" val is necessary because we want to delay
   * the initialization of the compression codec until it is first used. The reason is that a Spark
   * program could be using a user-defined codec in a third party jar, which is loaded in
   * Executor.updateDependencies. When the BlockManager is initialized, user level jars hasn't been
   * loaded yet. */
  private lazy val compressionCodec: CompressionCodec = CompressionCodec.createCodec(conf)

  def canUseKryo(ct: ClassTag[_]): Boolean = {
    primitiveAndPrimitiveArrayClassTags.contains(ct) || ct == stringClassTag
  }

  def getSerializer(ct: ClassTag[_]): Serializer = {
    if (canUseKryo(ct)) {
      kryoSerializer
    } else {
      defaultSerializer
    }
  }

  /**
   * Pick the best serializer for shuffling an RDD of key-value pairs.
   */
  def getSerializer(keyClassTag: ClassTag[_], valueClassTag: ClassTag[_]): Serializer = {
    if (canUseKryo(keyClassTag) && canUseKryo(valueClassTag)) {
      kryoSerializer
    } else {
      defaultSerializer
    }
  }

  private def shouldCompress(blockId: BlockId): Boolean = {
    blockId match {
      case _: ShuffleBlockId => compressShuffle
      case _: BroadcastBlockId => compressBroadcast
      case _: RDDBlockId => compressRdds
      case _: TempLocalBlockId => compressShuffleSpill
      case _: TempShuffleBlockId => compressShuffle
      case _ => false
    }
  }

  /**
   * Wrap an input stream for encryption and compression
   */
  def wrapStream(blockId: BlockId, s: InputStream): InputStream = {
    wrapForCompression(blockId, wrapForEncryption(s))
  }

  /**
   * Wrap an output stream for encryption and compression
   */
  def wrapStream(blockId: BlockId, s: OutputStream): OutputStream = {
    wrapForCompression(blockId, wrapForEncryption(s))
  }

  /**
   * Wrap an input stream for encryption if shuffle encryption is enabled
   */
  private[this] def wrapForEncryption(s: InputStream): InputStream = {
    if (enableIOEncryption) CryptoStreamUtils.createCryptoInputStream(s, conf) else s
  }

  /**
   * Wrap an output stream for encryption if shuffle encryption is enabled
   */
  private[this] def wrapForEncryption(s: OutputStream): OutputStream = {
    if (enableIOEncryption) CryptoStreamUtils.createCryptoOutputStream(s, conf) else s
  }

  /**
   * Wrap an output stream for compression if block compression is enabled for its block type
   */
  private[this] def wrapForCompression(blockId: BlockId, s: OutputStream): OutputStream = {
    if (shouldCompress(blockId)) compressionCodec.compressedOutputStream(s) else s
  }

  /**
   * Wrap an input stream for compression if block compression is enabled for its block type
   */
  private[this] def wrapForCompression(blockId: BlockId, s: InputStream): InputStream = {
    if (shouldCompress(blockId)) compressionCodec.compressedInputStream(s) else s
  }

  /** Serializes into a stream. */
  def dataSerializeStream[T: ClassTag](
      blockId: BlockId,
      outputStream: OutputStream,
      values: Iterator[T]): Unit = {
    val byteStream = new BufferedOutputStream(outputStream)
    val ser = getSerializer(implicitly[ClassTag[T]]).newInstance()
    ser.serializeStream(wrapStream(blockId, byteStream)).writeAll(values).close()
  }

  /** Serializes into a chunked byte buffer. */
  def dataSerialize[T: ClassTag](blockId: BlockId, values: Iterator[T]): ChunkedByteBuffer = {
    dataSerializeWithExplicitClassTag(blockId, values, implicitly[ClassTag[T]])
  }

  /** Serializes into a chunked byte buffer. */
  def dataSerializeWithExplicitClassTag(
      blockId: BlockId,
      values: Iterator[_],
      classTag: ClassTag[_]): ChunkedByteBuffer = {
    val bbos = ChunkedByteBufferOutputStream.newInstance()
    val ser = getSerializer(classTag).newInstance()
    ser.serializeStream(wrapStream(blockId, bbos)).writeAll(values).close()
    bbos.toChunkedByteBuffer
  }

  /**
   * Deserializes an InputStream into an iterator of values and disposes of it when the end of
   * the iterator is reached.
   */
  def dataDeserializeStream[T: ClassTag](
      blockId: BlockId,
      inputStream: InputStream): Iterator[T] = {
    val stream = new BufferedInputStream(inputStream)
    getSerializer(implicitly[ClassTag[T]])
      .newInstance()
      .deserializeStream(wrapStream(blockId, stream))
      .asIterator.asInstanceOf[Iterator[T]]
  }
}
