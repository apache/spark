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

import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.util.Try

import org.apache.avro.{SchemaValidationException, SchemaValidatorBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.{AvroDeserializer, AvroSerializer, SchemaConverters}
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils
import org.apache.spark.sql.execution.streaming.{CheckpointFileManager, StatefulOperatorStateInfo}
import org.apache.spark.sql.execution.streaming.state.SchemaHelper.{SchemaReader, SchemaWriter}
import org.apache.spark.sql.execution.streaming.state.StateSchemaCompatibilityChecker.SCHEMA_FORMAT_V3
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.types._

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
    keySchemaId: Short,
    keySchema: StructType,
    valueSchemaId: Short,
    valueSchema: StructType,
    keyStateEncoderSpec: Option[KeyStateEncoderSpec] = None,
    userKeyEncoderSchema: Option[StructType] = None
)

class StateSchemaCompatibilityChecker(
    providerId: StateStoreProviderId,
    hadoopConf: Configuration,
    oldSchemaFilePaths: List[Path] = List.empty,
    newSchemaFilePath: Option[Path] = None) extends Logging {

  // For OperatorStateMetadataV1: Only one schema file present per operator
  // per query
  // For OperatorStateMetadataV2: Multiple schema files present per operator
  // per query. This variable is the latest one
  private val schemaFileLocation = if (oldSchemaFilePaths.isEmpty) {
    val storeCpLocation = providerId.storeId.storeCheckpointLocation()
    schemaFile(storeCpLocation)
  } else {
    oldSchemaFilePaths.last
  }

  private val fm = CheckpointFileManager.create(schemaFileLocation, hadoopConf)

  fm.mkdirs(schemaFileLocation.getParent)

  private val conf = SparkSession.getActiveSession.get.sessionState.conf

  // Read most recent schema file
  def readSchemaFile(): List[StateStoreColFamilySchema] = {
    val inStream = fm.open(schemaFileLocation)
    StateSchemaCompatibilityChecker.readSchemaFile(inStream)
  }

  // Read all old schema files, group by column family name
  // This method is used for OperatorStateMetadataV2 when schema evolution
  // is supported, to read all active schemas in the StateStore for this operator
  def readSchemaFiles(): Map[String, List[StateStoreColFamilySchema]] = {
    val stateSchemaFilePaths = (oldSchemaFilePaths ++ List(schemaFileLocation)).distinct
    stateSchemaFilePaths.flatMap { schemaFile =>
        if (fm.exists(schemaFile)) {
          val inStream = fm.open(schemaFile)
          StateSchemaCompatibilityChecker.readSchemaFile(inStream)
        } else {
          List.empty
        }
      }
      .groupBy(_.colFamilyName)
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

  private def schemasCompatible(storedSchema: StructType, schema: StructType): Boolean =
    DataType.equalsIgnoreNameAndCompatibleNullability(schema, storedSchema)

  /**
   * Function to check if new state store schema is compatible with the existing schema.
   * @param oldSchemas - old state schemas
   * @param newSchema - new state schema
   * @param ignoreValueSchema - whether to ignore value schema or not
   */
  private def check(
      oldSchemas: List[StateStoreColFamilySchema],
      newSchema: StateStoreColFamilySchema,
      ignoreValueSchema: Boolean,
      schemaEvolutionEnabled: Boolean): (StateStoreColFamilySchema, Boolean) = {

    def incrementSchemaId(id: Short): Short = (id + 1).toShort

    val mostRecentSchema = oldSchemas.last
    // Initialize with old schema IDs
    val resultSchema = newSchema.copy(
      keySchemaId = mostRecentSchema.keySchemaId,
      valueSchemaId = mostRecentSchema.valueSchemaId
    )
    val (storedKeySchema, storedValueSchema) = (mostRecentSchema.keySchema,
      mostRecentSchema.valueSchema)
    val (keySchema, valueSchema) = (newSchema.keySchema, newSchema.valueSchema)

    if (storedKeySchema.equals(keySchema) &&
      (ignoreValueSchema || storedValueSchema.equals(valueSchema))) {
      // schema is exactly same
      (mostRecentSchema, false)
    } else if (!schemasCompatible(storedKeySchema, keySchema)) {
      throw StateStoreErrors.stateStoreKeySchemaNotCompatible(storedKeySchema.toString,
        keySchema.toString)
    } else if (!ignoreValueSchema && schemaEvolutionEnabled) {
      // Check value schema evolution
      // Sort schemas by most recent to least recent
      val oldStateSchemas = oldSchemas.sortBy(_.valueSchemaId).reverse.map { oldSchema =>
        StateSchemaMetadataValue(
          oldSchema.valueSchema, SchemaConverters.toAvroTypeWithDefaults(oldSchema.valueSchema))
      }.asJava

      val newAvroSchema = SchemaConverters.toAvroTypeWithDefaults(valueSchema)

      val validator = new SchemaValidatorBuilder().canReadStrategy.validateAll()
      oldStateSchemas.forEach { oldStateSchema =>
        try {
          validator.validate(newAvroSchema, List(oldStateSchema.avroSchema).asJava)
        } catch {
          case _: SchemaValidationException =>
            throw StateStoreErrors.stateStoreInvalidValueSchemaEvolution(
              oldStateSchema.sqlSchema.toString, valueSchema.toString)
          case e: Throwable => throw e
        }
      }

      if (resultSchema.valueSchemaId + 1 >=
        conf.streamingValueStateSchemaEvolutionThreshold) {
        throw StateStoreErrors.stateStoreValueSchemaEvolutionThresholdExceeded(
          resultSchema.valueSchemaId + 1,
          conf.streamingValueStateSchemaEvolutionThreshold,
          newSchema.colFamilyName
        )
      }
      // Schema evolved - increment value schema ID
      (resultSchema.copy(valueSchemaId = incrementSchemaId(mostRecentSchema.valueSchemaId)), true)
    } else if (!ignoreValueSchema && !schemasCompatible(storedValueSchema, valueSchema)) {
      throw StateStoreErrors.stateStoreValueSchemaNotCompatible(storedValueSchema.toString,
        valueSchema.toString)
    } else {
      logInfo("Detected schema change which is compatible. Allowing to put rows.")
      (mostRecentSchema, true)
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
      stateSchemaVersion: Int,
      schemaEvolutionEnabled: Boolean): Boolean = {
    val existingStateSchemaMap = readSchemaFiles()
    val mostRecentColFamilies = getExistingKeyAndValueSchema().map(_.colFamilyName)
    if (mostRecentColFamilies.isEmpty) {
      // Initialize schemas with ID 0 when no existing schema
      val initializedSchemas = newStateSchema.map { schema =>
        schema.copy(keySchemaId = 0, valueSchemaId = 0)
      }
      createSchemaFile(initializedSchemas.sortBy(_.colFamilyName), stateSchemaVersion)
      true
    } else {
      // Process each new schema and track if any have evolved
      val (evolvedSchemas, hasEvolutions) = newStateSchema.foldLeft(
        (List.empty[StateStoreColFamilySchema], false)) {
        case ((schemas, evolved), newSchema) =>
          existingStateSchemaMap.get(newSchema.colFamilyName) match {
            case Some(existingSchemas) =>
              val (updatedSchema, hasEvolved) = check(
                existingSchemas, newSchema, ignoreValueSchema, schemaEvolutionEnabled)
              (updatedSchema :: schemas, evolved || hasEvolved)
            case None =>
              // New column family - initialize with schema ID 0
              val newSchemaWithIds = newSchema.copy(keySchemaId = 0, valueSchemaId = 0)
              (newSchemaWithIds :: schemas, true)
          }
      }

      val newColFamilies = newStateSchema.map(_.colFamilyName).toSet
      val oldColFamilies = mostRecentColFamilies.toSet
      val colFamiliesAddedOrRemoved = newColFamilies != oldColFamilies
      val newSchemaFileWritten = hasEvolutions || colFamiliesAddedOrRemoved

      if (oldSchemaFilePaths.size == conf.streamingStateSchemaFilesThreshold &&
        colFamiliesAddedOrRemoved) {
        throw StateStoreErrors.streamingStateSchemaFilesThresholdExceeded(
          oldSchemaFilePaths.size + 1,
          conf.streamingStateSchemaFilesThreshold,
          newColFamilies.diff(oldColFamilies).toList,
          oldColFamilies.diff(newColFamilies).toList)
      }
      if (stateSchemaVersion == SCHEMA_FORMAT_V3 && newSchemaFileWritten) {
        createSchemaFile(evolvedSchemas.sortBy(_.colFamilyName), stateSchemaVersion)
      }

      newSchemaFileWritten
    }
  }

  private def schemaFile(storeCpLocation: Path): Path =
    new Path(new Path(storeCpLocation, "_metadata"), "schema")
}

object StateSchemaCompatibilityChecker extends Logging {

  val SCHEMA_FORMAT_V3: Int = 3

  private def disallowBinaryInequalityColumn(schema: StructType): Unit = {
    if (!UnsafeRowUtils.isBinaryStable(schema)) {
      throw new SparkUnsupportedOperationException(
        errorClass = "STATE_STORE_UNSUPPORTED_OPERATION_BINARY_INEQUALITY",
        messageParameters = Map("schema" -> schema.json)
      )
    }
  }

  def readSchemaFile(inStream: FSDataInputStream): List[StateStoreColFamilySchema] = {
    try {
      val versionStr = inStream.readUTF()
      val schemaReader = SchemaReader.createSchemaReader(versionStr)
      schemaReader.read(inStream)
    } catch {
      case e: Throwable =>
        logError(log"Fail to read schema file", e)
        throw e
    } finally {
      inStream.close()
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
      oldSchemaFilePaths: List[Path] = List.empty,
      newSchemaFilePath: Option[Path] = None,
      schemaEvolutionEnabled: Boolean = false): StateSchemaValidationResult = {
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
      oldSchemaFilePaths = oldSchemaFilePaths, newSchemaFilePath = newSchemaFilePath)
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
        stateSchemaVersion = stateSchemaVersion, schemaEvolutionEnabled)
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
        oldSchemaFilePaths.last.toString
      } else {
        // if we are using any version less than v3, we have written
        // the schema to this static location, which we will return
        checker.schemaFileLocation.toString
      }
    }

    StateSchemaValidationResult(evolvedSchema, schemaFileLocation)
  }
}
