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
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

case class StateSchemaNotCompatible(message: String) extends Exception(message)

class StateSchemaCompatibilityChecker(
    providerId: StateStoreProviderId,
    hadoopConf: Configuration) extends Logging {

  private val storeCpLocation = providerId.storeId.storeCheckpointLocation()
  private val fm = CheckpointFileManager.create(storeCpLocation, hadoopConf)
  private val schemaFileLocation = schemaFile(storeCpLocation)

  fm.mkdirs(schemaFileLocation.getParent)

  def check(keySchema: StructType, valueSchema: StructType): Unit = {
    if (fm.exists(schemaFileLocation)) {
      logDebug(s"Schema file for provider $providerId exists. Comparing with provided schema.")
      val (storedKeySchema, storedValueSchema) = readSchemaFile()

      val errorMsg = "Provided schema doesn't match to the schema for existing state! " +
        "Please note that Spark allow difference of field name: check count of fields " +
        "and data type of each field.\n" +
        s"- provided schema: key $keySchema value $valueSchema\n" +
        s"- existing schema: key $storedKeySchema value $storedValueSchema\n" +
        s"If you want to force running query without schema validation, please set " +
        s"${SQLConf.STATE_SCHEMA_CHECK_ENABLED.key} to false."

      if (storedKeySchema.equals(keySchema) && storedValueSchema.equals(valueSchema)) {
        // schema is exactly same
      } else if (!schemasCompatible(storedKeySchema, keySchema) ||
        !schemasCompatible(storedValueSchema, valueSchema)) {
        logError(errorMsg)
        throw StateSchemaNotCompatible(errorMsg)
      } else {
        logInfo("Detected schema change which is compatible: will overwrite schema file to new.")
        // It tries best-effort to overwrite current schema file.
        // the schema validation doesn't break even it fails, though it might miss on detecting
        // change which is not a big deal.
        createSchemaFile(keySchema, valueSchema)
      }
    } else {
      // schema doesn't exist, create one now
      logDebug(s"Schema file for provider $providerId doesn't exist. Creating one.")
      createSchemaFile(keySchema, valueSchema)
    }
  }

  private def schemasCompatible(storedSchema: StructType, schema: StructType): Boolean =
    equalsIgnoreCompatibleNullability(storedSchema, schema)

  private def equalsIgnoreCompatibleNullability(from: DataType, to: DataType): Boolean = {
    // This implementations should be same with DataType.equalsIgnoreCompatibleNullability, except
    // this shouldn't check the name equality.
    (from, to) match {
      case (ArrayType(fromElement, fn), ArrayType(toElement, tn)) =>
        (tn || !fn) && equalsIgnoreCompatibleNullability(fromElement, toElement)

      case (MapType(fromKey, fromValue, fn), MapType(toKey, toValue, tn)) =>
        (tn || !fn) &&
          equalsIgnoreCompatibleNullability(fromKey, toKey) &&
          equalsIgnoreCompatibleNullability(fromValue, toValue)

      case (StructType(fromFields), StructType(toFields)) =>
        fromFields.length == toFields.length &&
          fromFields.zip(toFields).forall { case (fromField, toField) =>
              (toField.nullable || !fromField.nullable) &&
              equalsIgnoreCompatibleNullability(fromField.dataType, toField.dataType)
          }

      case (fromDataType, toDataType) => fromDataType == toDataType
    }
  }

  private def readSchemaFile(): (StructType, StructType) = {
    val inStream = fm.open(schemaFileLocation)
    try {
      val version = inStream.readInt()
      // Currently we only support version 1, which we can simplify the version validation and
      // the parse logic.
      require(version == StateSchemaCompatibilityChecker.VERSION,
        s"version $version is not supported.")

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

  private def createSchemaFile(keySchema: StructType, valueSchema: StructType): Unit = {
    val outStream = fm.createAtomic(schemaFileLocation, overwriteIfPossible = true)
    try {
      outStream.writeInt(StateSchemaCompatibilityChecker.VERSION)
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

object StateSchemaCompatibilityChecker {
  val VERSION = 1
}
