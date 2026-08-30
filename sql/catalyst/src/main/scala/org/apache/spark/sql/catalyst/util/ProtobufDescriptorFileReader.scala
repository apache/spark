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

package org.apache.spark.sql.catalyst.util

import java.io.FileNotFoundException

import scala.util.control.NonFatal

import com.google.common.io.ByteStreams
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkEnv
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.PATH
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.util.{ProtobufUtils => ApiProtobufUtils}
import org.apache.spark.util.Utils

/**
 * Reads a Protobuf descriptor file for the SQL `from_protobuf` / `to_protobuf` expressions via the
 * Hadoop `FileSystem`, so paths on distributed file systems (e.g. `hdfs:`, `s3:`, `abfss:`)
 * resolve in addition to local paths. Runs on the driver at plan time.
 */
object ProtobufDescriptorFileReader extends Logging {

  /**
   * Reads and returns the raw bytes of the descriptor file at `filePath`.
   *
   * The path is read through the Hadoop `FileSystem` resolved for its scheme, so paths on
   * distributed file systems work. A scheme-less path that fails the Hadoop read falls back to a
   * local-NIO read to preserve pre-existing behavior.
   *
   * Must be called on the driver: it relies on the active `SparkEnv` (`SparkEnv.get`), which is
   * unavailable on executors.
   *
   * @param filePath the descriptor file path (a cloud URI, or a local path)
   * @return the descriptor file contents as a byte array
   * @throws org.apache.spark.sql.AnalysisException with condition
   *         `PROTOBUF_DESCRIPTOR_FILE_NOT_FOUND` if the file does not exist, or
   *         `CANNOT_PARSE_PROTOBUF_DESCRIPTOR` if it cannot otherwise be read
   */
  def readDescriptorFileContent(filePath: String): Array[Byte] = {
    // A scheme-less path (including the malformed empty string, where `new Path` throws) may be a
    // local file the old implementation could read, so it is eligible for the local fallback; a
    // path with an explicit scheme is not.
    val schemeless = try {
      new Path(filePath).toUri.getScheme == null
    } catch {
      case NonFatal(_) => false
    }
    try {
      val path = new Path(filePath)
      val fs = path.getFileSystem(SparkHadoopUtil.get.newConfiguration(SparkEnv.get.conf))
      Utils.tryWithResource(fs.open(path))(ByteStreams.toByteArray)
    } catch {
      case NonFatal(hadoopError) if schemeless =>
        // The Hadoop route failed for a scheme-less path. Fall back to the local-NIO read.
        // If the local read also fails, throw its error with the Hadoop error attached as a
        // suppressed exception, so neither cause is lost.
        logWarning(log"Failed to read Protobuf descriptor ${MDC(PATH, filePath)} via the Hadoop " +
          log"FileSystem; falling back to a local filesystem read.", hadoopError)
        try {
          ApiProtobufUtils.readDescriptorFileContent(filePath)
        } catch {
          case NonFatal(localError) =>
            localError.addSuppressed(hadoopError)
            throw localError
        }
      case e: FileNotFoundException =>
        // Explicit-scheme path that does not exist: surface the not-found error class.
        logWarning(log"Protobuf descriptor file ${MDC(PATH, filePath)} was not found.", e)
        throw QueryCompilationErrors.cannotFindDescriptorFileError(filePath, e)
      case NonFatal(e) =>
        // Explicit-scheme path that failed for another reason (e.g. permission denied, IO error).
        // Do not fall back -- the local read could not serve this path either
        logWarning(
          log"Failed to read Protobuf descriptor ${MDC(PATH, filePath)} via the Hadoop FileSystem.",
          e)
        throw QueryCompilationErrors.descriptorParseError(e)
    }
  }
}
