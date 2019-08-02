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

package org.apache.spark.sql.kafka010

import java.{util => ju}
import java.io.{InputStream, OutputStream}
import java.nio.charset.StandardCharsets.UTF_8

import scala.io.{Source => IOSource}
import scala.reflect.ClassTag

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.HDFSMetadataLog

private[kafka010] class ProducerTransactionMetaDataLog(
    sparkSession: SparkSession,
    path: String,
    params: ju.Map[String, Object])
  extends HDFSMetadataLog[Array[ProducerTransactionMetaData]](sparkSession, path) {
  private val BATCHES_PURGE_INTERVAL_KEY = "batchesPurgeInterval"
  private val DEFAULT_BATCHES_PURGE_INTERVAL = 10

  private implicit val formats = Serialization.formats(NoTypeHints)

  /** Needed to serialize type T into JSON when using Jackson */
  private implicit val manifest = Manifest.classType[ProducerTransactionMetaData](
    implicitly[ClassTag[ProducerTransactionMetaData]].runtimeClass)

  private val minBatchesToRetain = sparkSession.sessionState.conf.minBatchesToRetain
  private val purgeInterval = if (params.containsKey(BATCHES_PURGE_INTERVAL_KEY)) {
    val interval = params.get(BATCHES_PURGE_INTERVAL_KEY).toString.toInt
    assert(interval > 0, s"Kafka sink option '$BATCHES_PURGE_INTERVAL_KEY' should greater than 0.")
    interval
  } else {
    DEFAULT_BATCHES_PURGE_INTERVAL
  }

  override def serialize(logData: Array[ProducerTransactionMetaData], out: OutputStream): Unit = {
    out.write(("v" + ProducerTransactionMetaData.VERSION).getBytes(UTF_8))
    logData.foreach { data =>
      out.write('\n')
      out.write(Serialization.write(data).getBytes(UTF_8))
    }
  }

  override def deserialize(in: InputStream): Array[ProducerTransactionMetaData] = {
    val lines = IOSource.fromInputStream(in, UTF_8.name()).getLines()
    if (!lines.hasNext) {
      throw new IllegalStateException("Invalid log file, find no line.")
    }
    validateVersion(lines.next(), ProducerTransactionMetaData.VERSION)
    lines.map(Serialization.read[ProducerTransactionMetaData]).toArray
  }

  /**
   * Store the metadata for the specified batchId and purge stored metadata with
   * `batchesPurgeInterval` if exceed minBatchesToRetain. Return `true` if successful.
   * If the batchId's metadata has already been stored, this method will return `false`.
   */
  override def add(batchId: Long, metadata: Array[ProducerTransactionMetaData]): Boolean = {
    val ret = super.add(batchId, metadata)
    if (ret &&
      batchId > minBatchesToRetain && (batchId - minBatchesToRetain) % purgeInterval == 0) {
      val threshold = batchId - minBatchesToRetain
      purge(threshold)
    }
    ret
  }
}