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

package org.apache.spark.sql.pipelines.graph

import scala.util.control.NonFatal

import org.json4s.JsonAST.{JArray, JString}
import org.json4s.jackson.JsonMethods.{compact, parse}

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Table => CatalogTable, TableCatalog}
import org.apache.spark.sql.pipelines.autocdc.{AutoCdcReservedNames, ScdType}
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Helpers to construct and validate an AutoCDC flow's auxiliary table within the context of a
 * dataflow graph.
 */
object AutoCdcAuxiliaryTable {
  /**
   * Helper for deriving the auxiliary AutoCDC catalog table identifier from a target table. If a
   * table exists with a name matching the name derived here, it is assumed to be an AutoCDC
   * auxiliary table that should be managed by the pipeline.
   */
  def identifier(destination: TableIdentifier): TableIdentifier = TableIdentifier(
    table = s"${AutoCdcReservedNames.prefix}aux_state_${destination.table}",
    database = destination.database,
    catalog = destination.catalog
  )

  /**
   * Reserved table property key set on the auxiliary table to record which SCD strategy it
   * serves.
   */
  val scdTypePropertyKey: String = s"${PipelinesTableProperties.pipelinesPrefix}autocdc.scdType"

  /**
   * Table property recording the auxiliary table's unquoted AutoCDC key column names as a JSON
   * string array (e.g. `["id","region"]`). Written once when the auxiliary table is created and is
   * considered immutable; full-refresh is the only way to change it.
   */
  val keyColumnNamesProperty: String =
    s"${PipelinesTableProperties.pipelinesPrefix}autocdc.keyColumnNames"

  /**
   * Serialize key column names to the JSON form stored at [[keyColumnNamesProperty]].
   * Round-trips an empty list as `[]`; callers are expected to enforce a non-empty key set
   * upstream.
   */
  private[graph] def serializeKeyColumnNames(names: Seq[String]): String = {
    compact(JArray(names.map(JString(_)).toList))
  }

  /**
   * Parse a [[keyColumnNamesProperty]] value. `None` if it is not a JSON array of strings.
   * Round-trips an empty list as `[]`; callers are expected to enforce a non-empty key set
   * upstream.
   */
  private[graph] def parseKeyColumnNames(raw: String): Option[Seq[String]] = {
    val parsed = try Some(parse(raw)) catch { case NonFatal(_) => None }
    parsed.flatMap {
      case JArray(elems) =>
        val names = elems.collect { case JString(s) => s }
        if (names.size == elems.size) Some(names) else None
      case _ => None
    }
  }

  /**
   * Build the auxiliary table spec given an AutoCdc flow and the target table it writes to.
   *
   * @param targetTable the dataset that owns the auxiliary table
   * @param targetTableSchema the AutoCDC target's evolved schema as of the latest pipeline run
   *                          (the union of all flows writing to the target after schema
   *                          evolution, NOT the target's `specifiedSchema`)
   * @param inputAutoCdcFlow the AutoCDC flow writing to `targetTable`
   * @return the auxiliary-table spec
   */
  def buildAuxiliaryTableSpecFor(
      targetTable: Table,
      targetTableSchema: StructType,
      inputAutoCdcFlow: AutoCdcMergeFlow): AuxiliaryTableSpec = {
    inputAutoCdcFlow.changeArgs.storedAsScdType match {
      case ScdType.Type1 =>
        buildScd1AuxiliaryTableSpecFor(
          targetTable,
          targetTableSchema,
          inputAutoCdcFlow
        )
      case ScdType.Type2 =>
        // SCD2 auxiliary derivation lands with SCD2 support. AutoCdcMergeFlow rejects SCD2 at
        // construction today, so a resolved SCD2 flow cannot exist and this branch is unreachable.
        throw SparkException.internalError(
          "SCD2 auxiliary table derivation is not yet implemented."
        )
    }
  }

  /**
   * Build the SCD1 auxiliary table spec given the AutoCdc flow's declared keys and the target
   * table it writes to.
   *
   * @param targetTable the dataset that owns the SCD1 auxiliary table
   * @param targetTableSchema the AutoCDC target's evolved schema as of the latest pipeline run
   *                          (the union of all flows writing to the target after schema
   *                          evolution), from which the key and CDC metadata fields are
   *                          resolved
   * @param inputAutoCdcFlow the AutoCDC flow writing to `targetTable`
   * @return the SCD1 auxiliary-table spec
   */
  private def buildScd1AuxiliaryTableSpecFor(
      targetTable: Table,
      targetTableSchema: StructType,
      inputAutoCdcFlow: AutoCdcMergeFlow
  ): AuxiliaryTableSpec = {
    val scd1AuxiliaryTableIdentifier = identifier(targetTable.identifier)

    val resolver = inputAutoCdcFlow.df.sparkSession.sessionState.conf.resolver
    val autoCdcKeyColumnNames = inputAutoCdcFlow.changeArgs.keys.map(_.name)

    // The auxiliary table should derive its schema from the exact same key/CDC metadata column
    // schema in its corresponding target table. Retrieve those column schemas.
    val keyFields = autoCdcKeyColumnNames.map { keyColumnName =>
      findFieldInTargetSchema(
        targetTableSchema = targetTableSchema,
        targetTableIdentifier = targetTable.identifier,
        autoCdcFlowIdentifier = inputAutoCdcFlow.identifier,
        fieldName = keyColumnName,
        resolver = resolver
      )
    }
    val cdcMetadataField = findFieldInTargetSchema(
      targetTableSchema = targetTableSchema,
      targetTableIdentifier = targetTable.identifier,
      autoCdcFlowIdentifier = inputAutoCdcFlow.identifier,
      fieldName = AutoCdcReservedNames.cdcMetadataColName,
      resolver = resolver
    )

    val scd1AuxiliaryTableSchema = StructType(keyFields :+ cdcMetadataField)

    val scd1AuxiliaryTableProperties =
      // Record which SCD strategy this auxiliary table serves so downstream readers can identify it
      // without inspecting the schema.
      Map(scdTypePropertyKey -> ScdType.Type1.label) ++
      // Persist the AutoCDC key column names as a JSON list; immutable post-creation (full-refresh
      // is the only way to change it).
      Map(keyColumnNamesProperty -> serializeKeyColumnNames(keyFields.map(_.name))) ++
      // Inherit the target's format so MERGE semantics line up. When unspecified, omit the provider
      // so the catalog falls back to its default.
      targetTable.format.map(TableCatalog.PROP_PROVIDER -> _)

    AutoCdcAuxiliaryTableSpec(
      identifier = scd1AuxiliaryTableIdentifier,
      schema = scd1AuxiliaryTableSchema,
      properties = scd1AuxiliaryTableProperties,
      targetTableIdentifier = targetTable.identifier,
      expectedKeyFields = keyFields,
      expectedScdType = ScdType.Type1
    )
  }

  /**
   * Resolve the [[StructField]] named `fieldName` in `targetTableSchema` (the AutoCDC target's
   * evolved schema). The key columns and the CDC metadata column are always present in that schema,
   * so a miss is an implementation invariant and surfaces as an internal error.
   *
   * @param targetTableSchema the AutoCDC target's evolved schema to resolve against
   * @param fieldName the column name to resolve
   * @param resolver the session resolver used for case-sensitivity-aware field lookups
   * @param targetTableIdentifier the AutoCDC target's identifier, named in the error message
   * @param autoCdcFlowIdentifier the AutoCDC flow writing to the target, named in the error message
   * @return the matching field
   */
  private def findFieldInTargetSchema(
      targetTableSchema: StructType,
      fieldName: String,
      resolver: Resolver,
      targetTableIdentifier: TableIdentifier,
      autoCdcFlowIdentifier: TableIdentifier): StructField = {
    targetTableSchema.fields
      .find(field => resolver(field.name, fieldName))
      .getOrElse(
        throw SparkException.internalError(
          s"Expected but unable to find column $fieldName in target table " +
          s"$targetTableIdentifier written to by AutoCDC flow $autoCdcFlowIdentifier."
        )
      )
  }

  /**
   * Reject an existing auxiliary table whose key columns have drifted from `expectedKeyFields` as a
   * set: same arity, same set of names (per `resolver`), same per-name `dataType`s. Nullability and
   * metadata changes are intentionally tolerated.
   *
   * AutoCDC cannot change keys across incremental runs; a changed key set would otherwise be
   * silently unioned into the schema by the additive evolve. The remedy is a full refresh, which
   * recreates the auxiliary table. Errors name the AutoCDC target table rather than any single
   * flow, since one auxiliary table is shared by every flow writing to that target.
   */
  private[graph] def validateNoKeyColumnDrift(
      existingAuxiliaryTable: CatalogTable,
      targetTableIdentifier: TableIdentifier,
      expectedKeyFields: Seq[StructField],
      resolver: Resolver): Unit = {
    val existingAuxSchema = CatalogV2Util.v2ColumnsToStructType(existingAuxiliaryTable.columns())
    val recordedKeyNames =
      parseRecordedKeyColumnNames(existingAuxiliaryTable, targetTableIdentifier)
    // First validate the existing auxiliary table is internally consistent: every key column name
    // recorded in its table property must still resolve to a field in its schema. A missing key
    // column means the table was corrupted or modified externally, and is rejected before any
    // drift comparison against the expected key fields.
    val recordedKeyFields: Seq[StructField] = recordedKeyNames.map { name =>
      existingAuxSchema.fields
        .find(field => resolver(field.name, name))
        .getOrElse(
          // Either an implementation bug or, more likely, the user has corrupted the auxiliary
          // table schema (e.g. dropped the key column). The remedy is full-refresh in either case.
          throw new AnalysisException(
            errorClass = "AUTOCDC_INVALID_STATE.AUXILIARY_TABLE_KEY_COLUMN_MISSING",
            messageParameters = Map(
              "tableName" -> targetTableIdentifier.unquotedString,
              "keyColumnName" -> name,
              "propertyName" -> keyColumnNamesProperty
            )
          )
        )
    }

    val drifted =
      // Arity drift (added or dropped keys).
      recordedKeyFields.length != expectedKeyFields.length ||
      // Name or dataType drift: every expected key must have a same-name (resolver-aware) recorded
      // counterpart with an equivalent dataType. Columns changing nullability and metadata in the
      // schema are intentionally tolerated, although null key values during microbatch execution
      // will be invalidated regardless.
      expectedKeyFields.exists { expected =>
        recordedKeyFields.find(rf => resolver(rf.name, expected.name)) match {
          case None => true
          case Some(recorded) => !recorded.dataType.sameType(expected.dataType)
        }
      }

    if (drifted) {
      throw new AnalysisException(
        errorClass = "AUTOCDC_INVALID_STATE.KEY_SCHEMA_DRIFT",
        messageParameters = Map(
          "tableName" -> targetTableIdentifier.unquotedString,
          "expectedKeySchema" -> StructType(expectedKeyFields).toDDL,
          "recordedKeySchema" -> StructType(recordedKeyFields).toDDL
        )
      )
    }
  }

  /**
   * Reject an existing auxiliary table whose recorded `scdType` differs from the expected one.
   * SCD1 and SCD2 auxiliary tables carry different state shapes, so an in-place flip is
   * incompatible; the remedy is a full refresh, which recreates the auxiliary table.
   */
  private[graph] def validateNoScdTypeDrift(
      existingAuxiliaryTable: CatalogTable,
      targetTableIdentifier: TableIdentifier,
      expectedScdType: ScdType): Unit = {
    val recordedScdType = Option(
      existingAuxiliaryTable.properties().get(scdTypePropertyKey)
    ).getOrElse {
      throw new AnalysisException(
        errorClass = "AUTOCDC_INVALID_STATE.AUXILIARY_TABLE_PROPERTY_MISSING",
        messageParameters = Map(
          "tableName" -> targetTableIdentifier.unquotedString,
          "propertyName" -> scdTypePropertyKey
        )
      )
    }
    if (recordedScdType != expectedScdType.label) {
      throw new AnalysisException(
        errorClass = "AUTOCDC_INVALID_STATE.SCD_TYPE_DRIFT",
        messageParameters = Map(
          "tableName" -> targetTableIdentifier.unquotedString,
          "expectedScdType" -> expectedScdType.label,
          "recordedScdType" -> recordedScdType
        )
      )
    }
  }

  /**
   * Read [[keyColumnNamesProperty]] off an existing auxiliary table and parse it into the ordered
   * list of recorded AutoCDC key column names.
   */
  private def parseRecordedKeyColumnNames(
      existingAuxiliaryTable: CatalogTable,
      targetTableIdentifier: TableIdentifier): Seq[String] = {
    val rawKeyColumnNamesStr = Option(
      existingAuxiliaryTable.properties().get(keyColumnNamesProperty)
    ).getOrElse {
      throw new AnalysisException(
        errorClass = "AUTOCDC_INVALID_STATE.AUXILIARY_TABLE_PROPERTY_MISSING",
        messageParameters = Map(
          "tableName" -> targetTableIdentifier.unquotedString,
          "propertyName" -> keyColumnNamesProperty
        )
      )
    }
    parseKeyColumnNames(rawKeyColumnNamesStr).getOrElse {
      throw new AnalysisException(
        errorClass = "AUTOCDC_INVALID_STATE.AUXILIARY_TABLE_PROPERTY_MALFORMED",
        messageParameters = Map(
          "tableName" -> targetTableIdentifier.unquotedString,
          "propertyName" -> keyColumnNamesProperty,
          "rawValue" -> rawKeyColumnNamesStr
        )
      )
    }
  }
}
