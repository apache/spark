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
import java.nio.charset.StandardCharsets._

import org.apache.hadoop.fs.FSDataOutputStream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.state.{OperatorStateMetadata, OperatorStateMetadataV2}


class OperatorStateMetadataLog(sparkSession: SparkSession, path: String)
  extends HDFSMetadataLog[OperatorStateMetadata](sparkSession, path) {

  override protected def serialize(metadata: OperatorStateMetadata, out: OutputStream): Unit = {
    val fsDataOutputStream = out.asInstanceOf[FSDataOutputStream]
    OperatorStateMetadataV2.serialize(fsDataOutputStream, metadata)
  }

  override protected def deserialize(in: InputStream): OperatorStateMetadata = {
    // called inside a try-finally where the underlying stream is closed in the caller
    // create buffered reader from input stream
    val bufferedReader = new BufferedReader(new InputStreamReader(in, UTF_8))
    OperatorStateMetadataV2.deserialize(bufferedReader)
  }
}
