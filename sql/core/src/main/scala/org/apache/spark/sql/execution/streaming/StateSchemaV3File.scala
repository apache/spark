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

import java.io.{InputStream, OutputStream, StringReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}
import org.json4s.JValue
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

class StateSchemaV3File(
    hadoopConf: Configuration,
    path: String,
    metadataCacheEnabled: Boolean = false)
  extends HDFSMetadataLog[JValue](hadoopConf, path, metadataCacheEnabled) {

  final val MAX_UTF_CHUNK_SIZE = 65535
  def this(sparkSession: SparkSession, path: String) = {
    this(
      sparkSession.sessionState.newHadoopConf(),
      path,
      metadataCacheEnabled = sparkSession.sessionState.conf.getConf(
        SQLConf.STREAMING_METADATA_CACHE_ENABLED)
    )
  }

  override protected def serialize(schema: JValue, out: OutputStream): Unit = {
    val json = compact(render(schema))
    val buf = new Array[Char](MAX_UTF_CHUNK_SIZE)

    val outputStream = out.asInstanceOf[FSDataOutputStream]
    // DataOutputStream.writeUTF can't write a string at once
    // if the size exceeds 65535 (2^16 - 1) bytes.
    // Each metadata consists of multiple chunks in schema version 3.
    try {
      val numMetadataChunks = (json.length - 1) / MAX_UTF_CHUNK_SIZE + 1
      val metadataStringReader = new StringReader(json)
      outputStream.writeInt(numMetadataChunks)
      (0 until numMetadataChunks).foreach { _ =>
        val numRead = metadataStringReader.read(buf, 0, MAX_UTF_CHUNK_SIZE)
        outputStream.writeUTF(new String(buf, 0, numRead))
      }
      outputStream.close()
    } catch {
      case e: Throwable =>
        throw e
    }
  }

  override protected def deserialize(in: InputStream): JValue = {
    val buf = new StringBuilder
    val inputStream = in.asInstanceOf[FSDataInputStream]
    val numKeyChunks = inputStream.readInt()
    (0 until numKeyChunks).foreach(_ => buf.append(inputStream.readUTF()))
    val json = buf.toString()
   JsonMethods.parse(json)
  }

  override def add(batchId: Long, metadata: JValue): Boolean = {
    require(metadata != null, "'null' metadata cannot written to a metadata log")
    val batchMetadataFile = batchIdToPath(batchId)
    if (fileManager.exists(batchMetadataFile)) {
      fileManager.delete(batchMetadataFile)
    }
    val res = addNewBatchByStream(batchId) { output => serialize(metadata, output) }
    if (metadataCacheEnabled && res) batchCache.put(batchId, metadata)
    res
  }

  override def addNewBatchByStream(batchId: Long)(fn: OutputStream => Unit): Boolean = {
    val batchMetadataFile = batchIdToPath(batchId)

    if (metadataCacheEnabled && batchCache.containsKey(batchId)) {
      false
    } else {
      write(batchMetadataFile, fn)
      true
    }
  }
}
