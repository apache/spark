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

package org.apache.spark.sql.execution.streaming.state

import java.io.{InputStream, OutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

import scala.io.{Source => IOSource}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.MetadataVersionUtil.validateVersion

/**
 * The StateSchemaV3File is used to write the schema of multiple column families.
 * Right now, this is primarily used for the TransformWithState operator, which supports
 * multiple column families to keep the data for multiple state variables.
 * We only expect ColumnFamilySchemaV1 to be written and read from this file.
 * @param hadoopConf Hadoop configuration that is used to read / write metadata files.
 * @param path Path to the directory that will be used for writing metadata.
 */
class StateSchemaV3File(
    hadoopConf: Configuration,
    path: String) {

  val metadataPath = new Path(path)

  protected val fileManager: CheckpointFileManager =
    CheckpointFileManager.create(metadataPath, hadoopConf)

  if (!fileManager.exists(metadataPath)) {
    fileManager.mkdirs(metadataPath)
  }

  private def deserialize(in: InputStream): List[ColumnFamilySchema] = {
    val lines = IOSource.fromInputStream(in, UTF_8.name()).getLines()

    if (!lines.hasNext) {
      throw new IllegalStateException("Incomplete log file in the offset commit log")
    }

    val version = lines.next().trim
    validateVersion(version, StateSchemaV3File.VERSION)

    lines.map(ColumnFamilySchemaV1.fromJson).toList
  }

  private def serialize(schemas: List[ColumnFamilySchema], out: OutputStream): Unit = {
    out.write(s"v${StateSchemaV3File.VERSION}".getBytes(UTF_8))
    out.write('\n')
    out.write(schemas.map(_.json).mkString("\n").getBytes(UTF_8))
  }

  def addWithUUID(batchId: Long, metadata: List[ColumnFamilySchema]): Path = {
    val schemaFilePath = new Path(metadataPath, s"${batchId}_${UUID.randomUUID().toString}")
    write(schemaFilePath, out => serialize(metadata, out))
    schemaFilePath
  }

  def getWithPath(schemaFilePath: Path): List[ColumnFamilySchema] = {
    deserialize(fileManager.open(schemaFilePath))
  }

  protected def write(
      batchMetadataFile: Path,
      fn: OutputStream => Unit): Unit = {
    val output = fileManager.createAtomic(batchMetadataFile, overwriteIfPossible = false)
    try {
      fn(output)
      output.close()
    } catch {
      case e: Throwable =>
        output.cancel()
        throw e
    }
  }
}

object StateSchemaV3File {
  val VERSION = 3
}
