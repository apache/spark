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

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, Path}
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.execution.streaming.{CheckpointFileManager, MetadataVersionUtil}
import org.apache.spark.sql.execution.streaming.CheckpointFileManager.CancellableFSDataOutputStream
import org.apache.spark.sql.execution.streaming.state.OperatorStateMetadataUtils.{OperatorStateMetadataReader, OperatorStateMetadataWriter}

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

case class StateStoreMetadataV2(
    storeName: String,
    numColsPrefixKey: Int,
    numPartitions: Int,
    stateSchemaFilePath: String)
  extends StateStoreMetadata with Serializable

object StateStoreMetadataV2 {
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  @scala.annotation.nowarn
  private implicit val manifest = Manifest
    .classType[StateStoreMetadataV2](implicitly[ClassTag[StateStoreMetadataV2]].runtimeClass)
}

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

  def operatorInfo: OperatorInfo
}

case class OperatorStateMetadataV1(
    operatorInfo: OperatorInfoV1,
    stateStoreInfo: Array[StateStoreMetadataV1]) extends OperatorStateMetadata {
  override def version: Int = 1
}

case class OperatorStateMetadataV2(
    operatorInfo: OperatorInfoV1,
    stateStoreInfo: Array[StateStoreMetadataV2],
    operatorPropertiesJson: String) extends OperatorStateMetadata {
  override def version: Int = 2
}

object OperatorStateMetadataUtils extends Logging {

  sealed trait OperatorStateMetadataReader {
    def version: Int

    def read(): Option[OperatorStateMetadata]
  }

  sealed trait OperatorStateMetadataWriter {
    def version: Int
    def write(operatorMetadata: OperatorStateMetadata): Unit
  }

  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def readMetadata(inputStream: FSDataInputStream): Option[OperatorStateMetadata] = {
    val inputReader =
      new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
    try {
      val versionStr = inputReader.readLine()
      val version = MetadataVersionUtil.validateVersion(versionStr, 2)
      Some(deserialize(version, inputReader))
    } finally {
      inputStream.close()
    }
  }

  def writeMetadata(
      outputStream: CancellableFSDataOutputStream,
      operatorMetadata: OperatorStateMetadata,
      metadataFilePath: Path): Unit = {
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

  def deserialize(
      version: Int,
      in: BufferedReader): OperatorStateMetadata = {
    version match {
      case 1 =>
        Serialization.read[OperatorStateMetadataV1](in)
      case 2 =>
        Serialization.read[OperatorStateMetadataV2](in)

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
      case 2 =>
        Serialization.write(operatorStateMetadata.asInstanceOf[OperatorStateMetadataV2], out)
      case _ =>
        throw new IllegalArgumentException(s"Failed to serialize operator metadata with " +
          s"version=${operatorStateMetadata.version}")
    }
  }
}

object OperatorStateMetadataReader {
  def createReader(
      stateCheckpointPath: Path,
      hadoopConf: Configuration,
      version: Int): OperatorStateMetadataReader = {
    version match {
      case 1 =>
        new OperatorStateMetadataV1Reader(stateCheckpointPath, hadoopConf)
      case 2 =>
        new OperatorStateMetadataV2Reader(stateCheckpointPath, hadoopConf)
      case _ =>
        throw new IllegalArgumentException(s"Failed to create reader for operator metadata " +
          s"with version=$version")
    }
  }
}

object OperatorStateMetadataWriter {
  def createWriter(
      stateCheckpointPath: Path,
      hadoopConf: Configuration,
      version: Int,
      currentBatchId: Option[Long] = None): OperatorStateMetadataWriter = {
    version match {
      case 1 =>
        new OperatorStateMetadataV1Writer(stateCheckpointPath, hadoopConf)
      case 2 =>
        if (currentBatchId.isEmpty) {
          throw new IllegalArgumentException("currentBatchId is required for version 2")
        }
        new OperatorStateMetadataV2Writer(stateCheckpointPath, hadoopConf, currentBatchId.get)
      case _ =>
          throw new IllegalArgumentException(s"Failed to create writer for operator metadata " +
          s"with version=$version")
    }
  }
}

object OperatorStateMetadataV1 {
  def metadataFilePath(stateCheckpointPath: Path): Path =
    new Path(new Path(stateCheckpointPath, "_metadata"), "metadata")
}

object OperatorStateMetadataV2 {
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  @scala.annotation.nowarn
  private implicit val manifest = Manifest
    .classType[OperatorStateMetadataV2](implicitly[ClassTag[OperatorStateMetadataV2]].runtimeClass)

  def metadataDirPath(stateCheckpointPath: Path): Path =
    new Path(new Path(new Path(stateCheckpointPath, "_metadata"), "metadata"), "v2")

  def metadataFilePath(stateCheckpointPath: Path, currentBatchId: Long): Path =
    new Path(metadataDirPath(stateCheckpointPath), currentBatchId.toString)
}

/**
 * Write OperatorStateMetadata into the state checkpoint directory.
 */
class OperatorStateMetadataV1Writer(
    stateCheckpointPath: Path,
    hadoopConf: Configuration)
  extends OperatorStateMetadataWriter with Logging {

  private val metadataFilePath = OperatorStateMetadataV1.metadataFilePath(stateCheckpointPath)

  private lazy val fm = CheckpointFileManager.create(stateCheckpointPath, hadoopConf)

  override def version: Int = 1

  def write(operatorMetadata: OperatorStateMetadata): Unit = {
    if (fm.exists(metadataFilePath)) return

    fm.mkdirs(metadataFilePath.getParent)
    val outputStream = fm.createAtomic(metadataFilePath, overwriteIfPossible = false)
    OperatorStateMetadataUtils.writeMetadata(outputStream, operatorMetadata, metadataFilePath)
  }
}

/**
 * Read OperatorStateMetadata from the state checkpoint directory. This class will only be
 * used to read OperatorStateMetadataV1.
 * OperatorStateMetadataV2 will be read by the OperatorStateMetadataLog.
 */
class OperatorStateMetadataV1Reader(
    stateCheckpointPath: Path,
    hadoopConf: Configuration) extends OperatorStateMetadataReader {
  override def version: Int = 1

  private val metadataFilePath = OperatorStateMetadataV1.metadataFilePath(stateCheckpointPath)

  private lazy val fm = CheckpointFileManager.create(stateCheckpointPath, hadoopConf)

  def read(): Option[OperatorStateMetadata] = {
    val inputStream = fm.open(metadataFilePath)
    OperatorStateMetadataUtils.readMetadata(inputStream)
  }
}

class OperatorStateMetadataV2Writer(
    stateCheckpointPath: Path,
    hadoopConf: Configuration,
    currentBatchId: Long) extends OperatorStateMetadataWriter {

  private val metadataFilePath = OperatorStateMetadataV2.metadataFilePath(
    stateCheckpointPath, currentBatchId)

  private lazy val fm = CheckpointFileManager.create(stateCheckpointPath, hadoopConf)

  override def version: Int = 2

  override def write(operatorMetadata: OperatorStateMetadata): Unit = {
    if (fm.exists(metadataFilePath)) return

    fm.mkdirs(metadataFilePath.getParent)
    val outputStream = fm.createAtomic(metadataFilePath, overwriteIfPossible = false)
    OperatorStateMetadataUtils.writeMetadata(outputStream, operatorMetadata, metadataFilePath)
  }
}

class OperatorStateMetadataV2Reader(
    stateCheckpointPath: Path,
    hadoopConf: Configuration) extends OperatorStateMetadataReader {

  private val metadataDirPath = OperatorStateMetadataV2.metadataDirPath(stateCheckpointPath)
  private lazy val fm = CheckpointFileManager.create(metadataDirPath, hadoopConf)

  fm.mkdirs(metadataDirPath.getParent)
  override def version: Int = 2

  private def listBatches(): Array[Long] = {
    if (!fm.exists(metadataDirPath)) {
      return Array.empty
    }
    fm.list(metadataDirPath).map(_.getPath.getName.toLong).sorted
  }

  override def read(): Option[OperatorStateMetadata] = {
    val batches = listBatches()
    if (batches.isEmpty) {
      return None
    }
    val lastBatchId = batches.last
    val metadataFilePath = OperatorStateMetadataV2.metadataFilePath(
      stateCheckpointPath, lastBatchId)
    val inputStream = fm.open(metadataFilePath)
    OperatorStateMetadataUtils.readMetadata(inputStream)
  }
}
