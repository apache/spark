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

import java.io.{BufferedReader, InputStream, InputStreamReader, OutputStream}
import java.nio.charset.StandardCharsets
import java.nio.charset.StandardCharsets._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataOutputStream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.state.{OperatorStateMetadata, OperatorStateMetadataUtils}
import org.apache.spark.sql.internal.SQLConf


class OperatorStateMetadataLog(
    hadoopConf: Configuration,
    path: String,
    metadataCacheEnabled: Boolean = false)
  extends HDFSMetadataLog[OperatorStateMetadata](hadoopConf, path, metadataCacheEnabled) {

  def this(sparkSession: SparkSession, path: String) = {
    this(
      sparkSession.sessionState.newHadoopConf(),
      path,
      metadataCacheEnabled = sparkSession.sessionState.conf.getConf(
        SQLConf.STREAMING_METADATA_CACHE_ENABLED)
    )
  }

  override protected def serialize(metadata: OperatorStateMetadata, out: OutputStream): Unit = {
    val fsDataOutputStream = out.asInstanceOf[FSDataOutputStream]
    fsDataOutputStream.write(s"v${metadata.version}\n".getBytes(StandardCharsets.UTF_8))
    OperatorStateMetadataUtils.serialize(fsDataOutputStream, metadata)
  }

  override protected def deserialize(in: InputStream): OperatorStateMetadata = {
    // called inside a try-finally where the underlying stream is closed in the caller
    // create buffered reader from input stream
    val bufferedReader = new BufferedReader(new InputStreamReader(in, UTF_8))
    // read first line for version number, in the format "v{version}"
    val version = bufferedReader.readLine()
    version match {
      case "v1" => OperatorStateMetadataUtils.deserialize(1, bufferedReader)
      case "v2" => OperatorStateMetadataUtils.deserialize(2, bufferedReader)
    }
  }
}
