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
import org.apache.spark.sql.avro.{AvroDeserializer, AvroSerializer}
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils
import org.apache.spark.sql.execution.streaming.{CheckpointFileManager, StatefulOperatorStateInfo}
import org.apache.spark.sql.execution.streaming.state.SchemaHelper.{SchemaReader, SchemaWriter}
import org.apache.spark.sql.execution.streaming.state.StateSchemaCompatibilityChecker.SCHEMA_FORMAT_V3
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.types.{DataType, StructType}

// Result returned after validating the schema of the state store for schema changes
case class StateSchemaValidationResult(
    evolvedSchema: Boolean,
    schemaPath: String
)

/**
 * An Avro-based encoder used for serializing between UnsafeRow and Avro
 *  byte arrays in RocksDB state stores.
 *
 * This encoder is primarily utilized by [[RocksDBStateStoreProvider]] and [[RocksDBStateEncoder]]
 * to handle serialization and deserialization of state store data.
 *
 * @param keySerializer Serializer for converting state store keys to Avro format
 * @param keyDeserializer Deserializer for converting Avro-encoded keys back to UnsafeRow
 * @param valueSerializer Serializer for converting state store values to Avro format
 * @param valueDeserializer Deserializer for converting Avro-encoded values back to UnsafeRow
 * @param suffixKeySerializer Optional serializer for handling suffix keys in Avro format
 * @param suffixKeyDeserializer Optional deserializer for converting Avro-encoded suffix
 *                              keys back to UnsafeRow
 */
case class AvroEncoder(
  keySerializer: AvroSerializer,
  keyDeserializer: AvroDeserializer,
  valueSerializer: AvroSerializer,
  valueDeserializer: AvroDeserializer,
  suffixKeySerializer: Option[AvroSerializer] = None,
  suffixKeyDeserializer: Option[AvroDeserializer] = None
) extends Serializable

// Used to represent the schema of a column family in the state store
case class StateStoreColFamilySchema(
    colFamilyName: String,
    keySchema: StructType,
    valueSchema: StructType,
    keyStateEncoderSpec: Option[KeyStateEncoderSpec] = None,
    userKeyEncoderSchema: Option[StructType] = None
)

class StateSchemaCompatibilityChecker(
    providerId: StateStoreProviderId,
    hadoopConf: Configuration,
    oldSchemaFilePath: Option[Path] = None,
    newSchemaFilePath: Option[Path] = None) extends Logging {

  private val schemaFileLocation = if (oldSchemaFilePath.isEmpty) {
    val storeCpLocation = providerId.storeId.storeCheckpointLocation()
    schemaFile(storeCpLocation)
  } else {
    oldSchemaFilePath.get
  }

  private val fm = CheckpointFileManager.create(schemaFileLocation, hadoopConf)

  fm.mkdirs(schemaFileLocation.getParent)

  def readSchemaFile(): List[StateStoreColFamilySchema] = {
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
   * Function to read and return the list of existing state store column family schemas from the
   * schema file, if it exists
   * @return - List of state store column family schemas if the schema file exists and empty l
   *         otherwise
   */
  private def getExistingKeyAndValueSchema(): List[StateStoreColFamilySchema] = {
    if (fm.exists(schemaFileLocation)) {
      readSchemaFile()
    } else {
      List.empty
    }
  }

  private def createSchemaFile(
      stateStoreColFamilySchema: List[StateStoreColFamilySchema],
      stateSchemaVersion: Int): Unit = {
    // Ensure that schema file path is passed explicitly for schema version 3
    if (stateSchemaVersion == SCHEMA_FORMAT_V3 && newSchemaFilePath.isEmpty) {
      throw new IllegalStateException("Schema file path is required for schema version 3")
    }

    val schemaWriter = SchemaWriter.createSchemaWriter(stateSchemaVersion)
    createSchemaFile(stateStoreColFamilySchema, schemaWriter)
  }

  // Visible for testing
  private[sql] def createSchemaFile(
      stateStoreColFamilySchema: List[StateStoreColFamilySchema],
      schemaWriter: SchemaWriter): Unit = {
    val schemaFilePath = newSchemaFilePath match {
      case Some(path) =>
        fm.mkdirs(path.getParent)
        path
      case None => schemaFileLocation
    }
    val outStream = fm.createAtomic(schemaFilePath, overwriteIfPossible = false)
    try {
      schemaWriter.write(stateStoreColFamilySchema, outStream)
      outStream.close()
    } catch {
      case e: Throwable =>
        logError(log"Fail to write schema file to ${MDC(LogKeys.PATH, schemaFilePath)}", e)
        outStream.cancel()
        throw e
    }
  }

  private def schemasCompatible(storedSchema: StructType, schema: StructType): Boolean =
    DataType.equalsIgnoreNameAndCompatibleNullability(schema, storedSchema)

  /**
   * Function to check if new state store schema is compatible with the existing schema.
   * @param oldSchema - old state schema
   * @param newSchema - new state schema
   * @param ignoreValueSchema - whether to ignore value schema or not
   */
  private def check(
      oldSchema: StateStoreColFamilySchema,
      newSchema: StateStoreColFamilySchema,
      ignoreValueSchema: Boolean) : Unit = {
    val (storedKeySchema, storedValueSchema) = (oldSchema.keySchema,
      oldSchema.valueSchema)
    val (keySchema, valueSchema) = (newSchema.keySchema, newSchema.valueSchema)

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

  /**
   * Function to validate the new state store schema and evolve the schema if required.
   * @param newStateSchema - proposed new state store schema by the operator
   * @param ignoreValueSchema - whether to ignore value schema compatibility checks or not
   * @param stateSchemaVersion - version of the state schema to be used
   * @return - true if the schema has evolved, false otherwise
   */
  def validateAndMaybeEvolveStateSchema(
      newStateSchema: List[StateStoreColFamilySchema],
      ignoreValueSchema: Boolean,
      stateSchemaVersion: Int): Boolean = {
    val existingStateSchemaList = getExistingKeyAndValueSchema()
    val newStateSchemaList = newStateSchema

    if (existingStateSchemaList.isEmpty) {
      // write the schema file if it doesn't exist
      createSchemaFile(newStateSchemaList.sortBy(_.colFamilyName), stateSchemaVersion)
      true
    } else {
      // validate if the new schema is compatible with the existing schema
      val existingSchemaMap = existingStateSchemaList.map { schema =>
        schema.colFamilyName -> schema
      }.toMap
      // For each new state variable, we want to compare it to the old state variable
      // schema with the same name
      newStateSchemaList.foreach { newSchema =>
        existingSchemaMap.get(newSchema.colFamilyName).foreach { existingStateSchema =>
          check(existingStateSchema, newSchema, ignoreValueSchema)
        }
      }
      val colFamiliesAddedOrRemoved =
        (newStateSchemaList.map(_.colFamilyName).toSet != existingSchemaMap.keySet)
      if (stateSchemaVersion == SCHEMA_FORMAT_V3 && colFamiliesAddedOrRemoved) {
        createSchemaFile(newStateSchemaList.sortBy(_.colFamilyName), stateSchemaVersion)
      }
      // TODO: [SPARK-49535] Write Schema files after schema has changed for StateSchemaV3
      colFamiliesAddedOrRemoved
    }
  }

  private def schemaFile(storeCpLocation: Path): Path =
    new Path(new Path(storeCpLocation, "_metadata"), "schema")
}

object StateSchemaCompatibilityChecker {

  val SCHEMA_FORMAT_V3: Int = 3

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
   * @param newStateSchema - Array of new schema for state store column families
   * @param sessionState - session state used to retrieve session config
   * @param stateSchemaVersion - version of the state schema to be used
   * @param extraOptions - any extra options to be passed for StateStoreConf creation
   * @param storeName - optional state store name
   * @param oldSchemaFilePath - optional path to the old schema file. If not provided, will default
   *                          to the schema file location
   * @param newSchemaFilePath - optional path to the destination schema file.
   *                          Needed for schema version 3
   * @return - StateSchemaValidationResult containing the result of the schema validation
   */
  def validateAndMaybeEvolveStateSchema(
      stateInfo: StatefulOperatorStateInfo,
      hadoopConf: Configuration,
      newStateSchema: List[StateStoreColFamilySchema],
      sessionState: SessionState,
      stateSchemaVersion: Int,
      extraOptions: Map[String, String] = Map.empty,
      storeName: String = StateStoreId.DEFAULT_STORE_NAME,
      oldSchemaFilePath: Option[Path] = None,
      newSchemaFilePath: Option[Path] = None): StateSchemaValidationResult = {
    // SPARK-47776: collation introduces the concept of binary (in)equality, which means
    // in some collation we no longer be able to just compare the binary format of two
    // UnsafeRows to determine equality. For example, 'aaa' and 'AAA' can be "semantically"
    // same in case insensitive collation.
    // State store is basically key-value storage, and the most provider implementations
    // rely on the fact that all the columns in the key schema support binary equality.
    // We need to disallow using binary inequality column in the key schema, before we
    // could support this in majority of state store providers (or high-level of state
    // store.)
    newStateSchema.foreach { schema =>
      disallowBinaryInequalityColumn(schema.keySchema)
    }

    val storeConf = new StateStoreConf(sessionState.conf, extraOptions)
    val providerId = StateStoreProviderId(StateStoreId(stateInfo.checkpointLocation,
      stateInfo.operatorId, 0, storeName), stateInfo.queryRunId)
    val checker = new StateSchemaCompatibilityChecker(providerId, hadoopConf,
      oldSchemaFilePath = oldSchemaFilePath, newSchemaFilePath = newSchemaFilePath)
    // regardless of configuration, we check compatibility to at least write schema file
    // if necessary
    // if the format validation for value schema is disabled, we also disable the schema
    // compatibility checker for value schema as well.

    // Currently - schema evolution can happen only once per query. Basically for the initial batch
    // there is no previous schema. So we classify that case under schema evolution. In the future,
    // newer stateSchemaVersions will support evolution through the lifetime of the query as well.
    var evolvedSchema = false
    val result = Try(
      checker.validateAndMaybeEvolveStateSchema(newStateSchema,
        ignoreValueSchema = !storeConf.formatValidationCheckValue,
        stateSchemaVersion = stateSchemaVersion)
    ).toEither.fold(Some(_),
      hasEvolvedSchema => {
        evolvedSchema = hasEvolvedSchema
        None
      })

    // if schema validation is enabled and an exception is thrown, we re-throw it and fail the query
    if (storeConf.stateSchemaCheckEnabled && result.isDefined) {
      throw result.get
    }
    val schemaFileLocation = if (evolvedSchema) {
      // if we are using the state schema v3, and we have
      // evolved schema, this newSchemaFilePath should be defined
      // and we want to populate the metadata with this file
      if (stateSchemaVersion == SCHEMA_FORMAT_V3) {
        newSchemaFilePath.get.toString
      } else {
        // if we are using any version less than v3, we have written
        // the schema to this static location, which we will return
        checker.schemaFileLocation.toString
      }
    } else {
      // if we have not evolved schema (there has been a previous schema)
      // and we are using state schema v3, this file path would be defined
      // so we would just populate the next run's metadata file with this
      // file path
      if (stateSchemaVersion == SCHEMA_FORMAT_V3) {
        oldSchemaFilePath.get.toString
      } else {
        // if we are using any version less than v3, we have written
        // the schema to this static location, which we will return
        checker.schemaFileLocation.toString
      }
    }

    StateSchemaValidationResult(evolvedSchema, schemaFileLocation)
  }
}
