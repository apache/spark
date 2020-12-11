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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.{CheckpointFileManager, MetadataVersionUtil}
import org.apache.spark.sql.types.StructType

class StateSchemaFileManager(storeId: StateStoreId, hadoopConf: Configuration) extends Logging {
  private val storeCpLocation = storeId.storeCheckpointLocation()
  private val fm = CheckpointFileManager.create(storeCpLocation, hadoopConf)
  private val schemaFileLocation = schemaFile(storeCpLocation)

  def fileExist(): Boolean = fm.exists(schemaFileLocation)

  def readSchema(): (StructType, StructType) = {
    val inStream = fm.open(schemaFileLocation)
    try {
      val versionStr = inStream.readUTF()
      // Currently we only support version 1, which we can simplify the version validation and
      // the parse logic.
      val version = MetadataVersionUtil.validateVersion(versionStr,
        StateSchemaFileManager.VERSION)
      require(version == 1)

      val keySchemaStr = inStream.readUTF()
      val valueSchemaStr = inStream.readUTF()

      (StructType.fromString(keySchemaStr), StructType.fromString(valueSchemaStr))
    } catch {
      case e: Throwable =>
        logError(s"Fail to read schema file from $schemaFileLocation", e)
        throw e
    } finally {
      inStream.close()
    }
  }

  def writeSchema(keySchema: StructType, valueSchema: StructType): Unit = {
    if (!fm.exists(schemaFileLocation.getParent)) {
      fm.mkdirs(schemaFileLocation.getParent)
    }

    val outStream = fm.createAtomic(schemaFileLocation, overwriteIfPossible = false)
    try {
      outStream.writeUTF(s"v${StateSchemaFileManager.VERSION}")
      outStream.writeUTF(keySchema.json)
      outStream.writeUTF(valueSchema.json)
      outStream.close()
    } catch {
      case e: Throwable =>
        logError(s"Fail to write schema file to $schemaFileLocation", e)
        outStream.cancel()
        throw e
    }
  }

  private def schemaFile(storeCpLocation: Path): Path =
    new Path(new Path(storeCpLocation, "_metadata"), "schema")
}

object StateSchemaFileManager {
  val VERSION = 1
}
