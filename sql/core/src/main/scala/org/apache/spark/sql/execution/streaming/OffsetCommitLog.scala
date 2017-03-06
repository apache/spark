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

import java.io.{InputStream, OutputStream}
import java.nio.charset.StandardCharsets._

import scala.io.{Source => IOSource}

import org.apache.spark.sql.SparkSession

class OffsetCommitLog(sparkSession: SparkSession, path: String)
  extends HDFSMetadataLog[Option[String]](sparkSession, path) {

  override protected def deserialize(in: InputStream): Option[String] = {
    // called inside a try-finally where the underlying stream is closed in the caller
    val lines = IOSource.fromInputStream(in, UTF_8.name()).getLines()
    if (!lines.hasNext) {
      throw new IllegalStateException("Incomplete log file")
    }
    val version = lines.next()
    if (version != OffsetCommitLog.VERSION) {
      throw new IllegalStateException(s"Unknown log version: ${version}")
    }
    // read metadata
    lines.next().trim match {
      case OffsetCommitLog.SERIALIZED_VOID => None
      case metadata => Some(metadata)
    }
  }

  override protected def serialize(metadata: Option[String], out: OutputStream): Unit = {
    // called inside a try-finally where the underlying stream is closed in the caller
    out.write(OffsetCommitLog.VERSION.getBytes(UTF_8))
    out.write('\n')

    // write metadata or void
    out.write(metadata.getOrElse(OffsetCommitLog.SERIALIZED_VOID).getBytes(UTF_8))
  }
}

object OffsetCommitLog {
  private val VERSION = "v1"
  private val SERIALIZED_VOID = "-"
}

