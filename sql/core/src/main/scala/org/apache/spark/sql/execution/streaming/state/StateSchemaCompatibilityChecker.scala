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

import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils
import org.apache.spark.sql.execution.streaming.{CheckpointFileManager, StatefulOperatorStateInfo}
import org.apache.spark.sql.execution.streaming.state.SchemaHelper.{SchemaReader, SchemaWriter}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.types.{DataType, StructType}

class StateSchemaCompatibilityChecker(
    providerId: StateStoreProviderId,
    hadoopConf: Configuration) extends Logging {

  private val storeCpLocation = providerId.storeId.storeCheckpointLocation()
  private val fm = CheckpointFileManager.create(storeCpLocation, hadoopConf)
  private val schemaFileLocation = schemaFile(storeCpLocation)
  private val schemaWriter =
    SchemaWriter.createSchemaWriter(StateSchemaCompatibilityChecker.VERSION)

  fm.mkdirs(schemaFileLocation.getParent)

  def check(keySchema: StructType, valueSchema: StructType): Unit = {
    check(keySchema, valueSchema, ignoreValueSchema = false)
  }

  def check(keySchema: StructType, valueSchema: StructType, ignoreValueSchema: Boolean): Unit = {
    if (fm.exists(schemaFileLocation)) {
      logDebug(s"Schema file for provider $providerId exists. Comparing with provided schema.")
      val (storedKeySchema, storedValueSchema) = readSchemaFile()
      if (storedKeySchema.equals(keySchema) &&
        (ignoreValueSchema || storedValueSchema.equals(valueSchema))) {
        // schema is exactly same
      } else if (!schemasCompatible(storedKeySchema, keySchema)) {
        throw StateStoreErrors.stateStoreKeySchemaNotCompatible(storedKeySchema.toString,
          keySchema.toString)
      } else if (!ignoreValueSchema && !schemasCompatible(storedValueSchema, valueSchema)) {
        throw StateStoreErrors.stateStoreValueSchemaNotCompatible(storedValueSchema.toString,
          valueSchema.toString)
      } else {
        logInfo("Detected schema change which is compatible. Allowing to put rows.")
      }
    } else {
      // schema doesn't exist, create one now
      logDebug(s"Schema file for provider $providerId doesn't exist. Creating one.")
      createSchemaFile(keySchema, valueSchema)
    }
  }

  def check(
      oldSchema: (StructType, StructType),
      newSchema: (StructType, StructType),
      ignoreValueSchema: Boolean) : Unit = {
    val (storedKeySchema, storedValueSchema) = oldSchema
    val (keySchema, valueSchema) = newSchema

    if (storedKeySchema.equals(keySchema) &&
      (ignoreValueSchema || storedValueSchema.equals(valueSchema))) {
      // schema is exactly same
    } else if (!schemasCompatible(storedKeySchema, keySchema)) {
      throw StateStoreErrors.stateStoreKeySchemaNotCompatible(storedKeySchema.toString,
        keySchema.toString)
    } else if (!ignoreValueSchema && !schemasCompatible(storedValueSchema, valueSchema)) {
      throw StateStoreErrors.stateStoreValueSchemaNotCompatible(storedValueSchema.toString,
        valueSchema.toString)
    } else {
      logInfo("Detected schema change which is compatible. Allowing to put rows.")
    }
  }

  private def schemasCompatible(storedSchema: StructType, schema: StructType): Boolean =
    DataType.equalsIgnoreNameAndCompatibleNullability(schema, storedSchema)

  def readSchemaFile(): (StructType, StructType) = {
    val inStream = fm.open(schemaFileLocation)
    try {
      val versionStr = inStream.readUTF()
      val schemaReader = SchemaReader.createSchemaReader(versionStr)
      schemaReader.read(inStream)
    } catch {
      case e: Throwable =>
        logError(s"Fail to read schema file from $schemaFileLocation", e)
        throw e
    } finally {
      inStream.close()
    }
  }

  private def getExistingKeyAndValueSchema(): Option[(StructType, StructType)] = {
    if (fm.exists(schemaFileLocation)) {
      Some(readSchemaFile())
    } else {
      None
    }
  }

  def createSchemaFile(keySchema: StructType, valueSchema: StructType): Unit = {
    createSchemaFile(keySchema, valueSchema, schemaWriter)
  }

  // Visible for testing
  private[sql] def createSchemaFile(
      keySchema: StructType,
      valueSchema: StructType,
      schemaWriter: SchemaWriter): Unit = {
    val outStream = fm.createAtomic(schemaFileLocation, overwriteIfPossible = false)
    try {
      schemaWriter.write(keySchema, valueSchema, outStream)
      outStream.close()
    } catch {
      case e: Throwable =>
        logError(s"Fail to write schema file to $schemaFileLocation", e)
        outStream.cancel()
        throw e
    }
  }

  def validateAndMaybeEvolveSchema(
      newKeySchema: StructType,
      newValueSchema: StructType,
      ignoreValueSchema: Boolean): Unit = {
    val existingSchema = getExistingKeyAndValueSchema()
    if (existingSchema.isEmpty) {
      createSchemaFile(newKeySchema, newValueSchema)
    } else {
      check(existingSchema.get, (newKeySchema, newValueSchema), ignoreValueSchema)
    }
  }

  private def schemaFile(storeCpLocation: Path): Path =
    new Path(new Path(storeCpLocation, "_metadata"), "schema")
}

object StateSchemaCompatibilityChecker {
  val VERSION = 2

  private def disallowBinaryInequalityColumn(schema: StructType): Unit = {
    if (!UnsafeRowUtils.isBinaryStable(schema)) {
      throw new SparkUnsupportedOperationException(
        errorClass = "STATE_STORE_UNSUPPORTED_OPERATION_BINARY_INEQUALITY",
        messageParameters = Map("schema" -> schema.json)
      )
    }
  }

  def validateAndMaybeEvolveSchema(
      stateInfo: StatefulOperatorStateInfo,
      hadoopConf: Configuration,
      newKeySchema: StructType,
      newValueSchema: StructType,
      sessionState: SessionState,
      extraOptions: Map[String, String] = Map.empty,
      storeName: String = StateStoreId.DEFAULT_STORE_NAME): Unit = {
    // SPARK-47776: collation introduces the concept of binary (in)equality, which means
    // in some collation we no longer be able to just compare the binary format of two
    // UnsafeRows to determine equality. For example, 'aaa' and 'AAA' can be "semantically"
    // same in case insensitive collation.
    // State store is basically key-value storage, and the most provider implementations
    // rely on the fact that all the columns in the key schema support binary equality.
    // We need to disallow using binary inequality column in the key schema, before we
    // could support this in majority of state store providers (or high-level of state
    // store.)
    disallowBinaryInequalityColumn(newKeySchema)

    val storeConf = new StateStoreConf(sessionState.conf, extraOptions)
    val providerId = StateStoreProviderId(StateStoreId(stateInfo.checkpointLocation,
      stateInfo.operatorId, 0, storeName), stateInfo.queryRunId)
    val checker = new StateSchemaCompatibilityChecker(providerId, hadoopConf)
    // regardless of configuration, we check compatibility to at least write schema file
    // if necessary
    // if the format validation for value schema is disabled, we also disable the schema
    // compatibility checker for value schema as well.
    val result = Try(
      checker.validateAndMaybeEvolveSchema(newKeySchema, newValueSchema,
        ignoreValueSchema = !storeConf.formatValidationCheckValue)
    ).toEither.fold(Some(_), _ => None)

    if (storeConf.stateSchemaCheckEnabled && result.isDefined) {
      throw result.get
    }
  }
}
