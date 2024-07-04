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

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.execution.streaming.{CheckpointFileManager, MetadataVersionUtil}

/**
 * Metadata for a state store instance.
 */
trait StateStoreMetadata {
  def storeName: String
  def numColsPrefixKey: Int
  def numPartitions: Int
}

case class StateStoreMetadataV1(storeName: String, numColsPrefixKey: Int, numPartitions: Int)
  extends StateStoreMetadata

/**
 * Information about a stateful operator.
 */
trait OperatorInfo {
  def operatorId: Long
  def operatorName: String
}

case class OperatorInfoV1(operatorId: Long, operatorName: String) extends OperatorInfo

trait OperatorStateMetadata {
  def version: Int
}

case class OperatorStateMetadataV1(
    operatorInfo: OperatorInfoV1,
    stateStoreInfo: Array[StateStoreMetadataV1]) extends OperatorStateMetadata {
  override def version: Int = 1
}

object OperatorStateMetadataUtils {

  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def metadataFilePath(stateCheckpointPath: Path): Path =
    new Path(new Path(stateCheckpointPath, "_metadata"), "metadata")

  def deserialize(
      version: Int,
      in: BufferedReader): OperatorStateMetadata = {
    version match {
      case 1 =>
        Serialization.read[OperatorStateMetadataV1](in)

      case _ =>
        throw new IllegalArgumentException(s"Failed to deserialize operator metadata with " +
          s"version=$version")
    }
  }

  def serialize(
      out: FSDataOutputStream,
      operatorStateMetadata: OperatorStateMetadata): Unit = {
    operatorStateMetadata.version match {
      case 1 =>
        Serialization.write(operatorStateMetadata.asInstanceOf[OperatorStateMetadataV1], out)

      case _ =>
        throw new IllegalArgumentException(s"Failed to serialize operator metadata with " +
          s"version=${operatorStateMetadata.version}")
    }
  }
}

/**
 * Write OperatorStateMetadata into the state checkpoint directory.
 */
class OperatorStateMetadataWriter(stateCheckpointPath: Path, hadoopConf: Configuration)
  extends Logging {

  private val metadataFilePath = OperatorStateMetadataUtils.metadataFilePath(stateCheckpointPath)

  private lazy val fm = CheckpointFileManager.create(stateCheckpointPath, hadoopConf)

  def write(operatorMetadata: OperatorStateMetadata): Unit = {
    if (fm.exists(metadataFilePath)) return

    fm.mkdirs(metadataFilePath.getParent)
    val outputStream = fm.createAtomic(metadataFilePath, overwriteIfPossible = false)
    try {
      outputStream.write(s"v${operatorMetadata.version}\n".getBytes(StandardCharsets.UTF_8))
      OperatorStateMetadataUtils.serialize(outputStream, operatorMetadata)
      outputStream.close()
    } catch {
      case e: Throwable =>
        logError(
          log"Fail to write state metadata file to ${MDC(LogKeys.META_FILE, metadataFilePath)}", e)
        outputStream.cancel()
        throw e
    }
  }
}

/**
 * Read OperatorStateMetadata from the state checkpoint directory.
 */
class OperatorStateMetadataReader(stateCheckpointPath: Path, hadoopConf: Configuration) {

  private val metadataFilePath = OperatorStateMetadataUtils.metadataFilePath(stateCheckpointPath)

  private lazy val fm = CheckpointFileManager.create(stateCheckpointPath, hadoopConf)

  def read(): OperatorStateMetadata = {
    val inputStream = fm.open(metadataFilePath)
    val inputReader =
      new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
    try {
      val versionStr = inputReader.readLine()
      val version = MetadataVersionUtil.validateVersion(versionStr, 1)
      OperatorStateMetadataUtils.deserialize(version, inputReader)
    } finally {
      inputStream.close()
    }
  }
}
