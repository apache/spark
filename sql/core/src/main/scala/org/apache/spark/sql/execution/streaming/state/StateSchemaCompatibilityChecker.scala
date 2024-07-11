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
import org.apache.spark.internal.{Logging, LogKeys, MDC}
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

  def readSchemaFile(): (StructType, StructType) = {
    val inStream = fm.open(schemaFileLocation)
    try {
      val versionStr = inStream.readUTF()
      val schemaReader = SchemaReader.createSchemaReader(versionStr)
      schemaReader.read(inStream)
    } catch {
      case e: Throwable =>
        logError(log"Fail to read schema file from ${MDC(LogKeys.PATH, schemaFileLocation)}", e)
        throw e
    } finally {
      inStream.close()
    }
  }

  /**
   * Function to read and return the existing key and value schema from the schema file, if it
   * exists
   * @return - Option of (keySchema, valueSchema) if the schema file exists, None otherwise
   */
  private def getExistingKeyAndValueSchema(): Option[(StructType, StructType)] = {
    if (fm.exists(schemaFileLocation)) {
      Some(readSchemaFile())
    } else {
      None
    }
  }

  private def createSchemaFile(keySchema: StructType, valueSchema: StructType): Unit = {
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
        logError(log"Fail to write schema file to ${MDC(LogKeys.PATH, schemaFileLocation)}", e)
        outStream.cancel()
        throw e
    }
  }

  def validateAndMaybeEvolveStateSchema(
      newKeySchema: StructType,
      newValueSchema: StructType,
      ignoreValueSchema: Boolean): Unit = {
    val existingSchema = getExistingKeyAndValueSchema()
    if (existingSchema.isEmpty) {
      // write the schema file if it doesn't exist
      createSchemaFile(newKeySchema, newValueSchema)
    } else {
      // validate if the new schema is compatible with the existing schema
      StateSchemaCompatibilityChecker.
        check(existingSchema.get, (newKeySchema, newValueSchema), ignoreValueSchema)
    }
  }

  private def schemaFile(storeCpLocation: Path): Path =
    new Path(new Path(storeCpLocation, "_metadata"), "schema")
}

object StateSchemaCompatibilityChecker extends Logging {
  val VERSION = 2

  /**
   * Function to check if new state store schema is compatible with the existing schema.
   * @param oldSchema - old state schema
   * @param newSchema - new state schema
   * @param ignoreValueSchema - whether to ignore value schema or not
   */
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

  private def disallowBinaryInequalityColumn(schema: StructType): Unit = {
    if (!UnsafeRowUtils.isBinaryStable(schema)) {
      throw new SparkUnsupportedOperationException(
        errorClass = "STATE_STORE_UNSUPPORTED_OPERATION_BINARY_INEQUALITY",
        messageParameters = Map("schema" -> schema.json)
      )
    }
  }

  /**
   * Function to validate the schema of the state store and maybe evolve it if needed.
   * We also verify for binary inequality columns in the schema and disallow them. We then perform
   * key and value schema validation. Depending on the passed configs, a warning might be logged
   * or an exception might be thrown if the schema is not compatible.
   *
   * @param stateInfo - StatefulOperatorStateInfo containing the state store information
   * @param hadoopConf - Hadoop configuration
   * @param newKeySchema - New key schema
   * @param newValueSchema - New value schema
   * @param sessionState - session state used to retrieve session config
   * @param extraOptions - any extra options to be passed for StateStoreConf creation
   * @param storeName - optional state store name
   */
  def validateAndMaybeEvolveStateSchema(
      stateInfo: StatefulOperatorStateInfo,
      hadoopConf: Configuration,
      newKeySchema: StructType,
      newValueSchema: StructType,
      sessionState: SessionState,
      extraOptions: Map[String, String] = Map.empty,
      storeName: String = StateStoreId.DEFAULT_STORE_NAME): Array[String] = {
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
      checker.validateAndMaybeEvolveStateSchema(newKeySchema, newValueSchema,
        ignoreValueSchema = !storeConf.formatValidationCheckValue)
    ).toEither.fold(Some(_), _ => None)

    // if schema validation is enabled and an exception is thrown, we re-throw it and fail the query
    if (storeConf.stateSchemaCheckEnabled && result.isDefined) {
      throw result.get
    }
    Array(checker.schemaFileLocation.toString)
  }
}
