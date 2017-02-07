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

import java.io.{InputStreamReader, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import scala.util.control.NonFatal

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, Path}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQuery

/**
 * Contains metadata associated with a [[StreamingQuery]]. This information is written
 * in the checkpoint location the first time a query is started and recovered every time the query
 * is restarted.
 *
 * @param id  unique id of the [[StreamingQuery]] that needs to be persisted across restarts
 */
case class StreamMetadata(id: String) {
  def json: String = Serialization.write(this)(StreamMetadata.format)
}

object StreamMetadata extends Logging {
  implicit val format = Serialization.formats(NoTypeHints)

  /** Read the metadata from file if it exists */
  def read(metadataFile: Path, hadoopConf: Configuration): Option[StreamMetadata] = {
    val fs = metadataFile.getFileSystem(hadoopConf)
    if (fs.exists(metadataFile)) {
      var input: FSDataInputStream = null
      try {
        input = fs.open(metadataFile)
        val reader = new InputStreamReader(input, StandardCharsets.UTF_8)
        val metadata = Serialization.read[StreamMetadata](reader)
        Some(metadata)
      } catch {
        case NonFatal(e) =>
          logError(s"Error reading stream metadata from $metadataFile", e)
          throw e
      } finally {
        IOUtils.closeQuietly(input)
      }
    } else None
  }

  /** Write metadata to file */
  def write(
      metadata: StreamMetadata,
      metadataFile: Path,
      hadoopConf: Configuration): Unit = {
    var output: FSDataOutputStream = null
    try {
      val fs = metadataFile.getFileSystem(hadoopConf)
      output = fs.create(metadataFile)
      val writer = new OutputStreamWriter(output)
      Serialization.write(metadata, writer)
      writer.close()
    } catch {
      case NonFatal(e) =>
        logError(s"Error writing stream metadata $metadata to $metadataFile", e)
        throw e
    } finally {
      IOUtils.closeQuietly(output)
    }
  }
}
