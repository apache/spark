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
import org.apache.spark.sql.pipelines.autocdc.{AutoCdcReservedNames, Scd1BatchProcessor,
  Scd2BatchProcessor, ScdType}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

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
   * Table property recording the resolved SCD2 track-history column names as a JSON string array.
   * These columns define an SCD2 run (a change in any of them opens a new historical record), so
   * changing the set would reinterpret already-reconciled history. SCD2-only; absent for SCD1.
   * Full-refresh is the only way to change it.
   */
  val trackHistoryColumnNamesProperty: String =
    s"${PipelinesTableProperties.pipelinesPrefix}autocdc.trackHistoryColumnNames"

  /**
   * A JSON string-array codec for column-name lists persisted as auxiliary-table properties (used
   * for both [[keyColumnNamesProperty]] and [[trackHistoryColumnNamesProperty]]). Round-trips an
   * empty list as `[]` -- both an empty key set and an empty track-history set are meaningful to
   * some caller, so no non-empty invariant is assumed here.
   */
  private[graph] def serializeColumnNames(names: Seq[String]): String = {
    compact(JArray(names.map(JString(_)).toList))
  }

  /**
   * Parse a value written by [[serializeColumnNames]]. `None` if it is not a JSON array of strings.
   * Round-trips an empty list as `[]` (see [[serializeColumnNames]]).
   */
  private[graph] def parseColumnNames(raw: String): Option[Seq[String]] = {
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
        buildScd2AuxiliaryTableSpecFor(
          targetTable,
          targetTableSchema,
          inputAutoCdcFlow
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

    val resolver = inputAutoCdcFlow.effectiveResolver
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
      Map(keyColumnNamesProperty -> serializeColumnNames(keyFields.map(_.name))) ++
      // Inherit the target's format so MERGE semantics line up. When unspecified, omit the provider
      // so the catalog falls back to its default.
      targetTable.format.map(TableCatalog.PROP_PROVIDER -> _)

    AutoCdcAuxiliaryTableSpec(
      identifier = scd1AuxiliaryTableIdentifier,
      schema = scd1AuxiliaryTableSchema,
      properties = scd1AuxiliaryTableProperties,
      targetTableIdentifier = targetTable.identifier,
      expectedKeyFields = keyFields,
      expectedScdType = ScdType.Type1,
      expectedSequencingType = inputAutoCdcFlow.sequencingType,
      expectedTrackHistoryColumnNames = None
    )
  }

  /**
   * Build the SCD2 auxiliary table spec given the AutoCdc flow's declared keys and the target
   * table it writes to.
   *
   * Unlike the SCD1 auxiliary table (which stores only keys + CDC metadata), the SCD2 auxiliary
   * table stores full hidden rows -- tombstones and coalesced no-op upserts that may later be
   * promoted into the visible target -- so its schema is the entire SCD2 target row schema plus
   * the aux-only [[Scd2BatchProcessor.deletedByBatchIdColName]] logical-delete marker. This
   * matches what [[Scd2BatchProcessor.findAffectedRowsFromAuxiliaryTable]] reads and the merges
   * write: "[[Scd2BatchProcessor.deletedByBatchIdColName]] in addition to all of the columns in
   * the target table".
   *
   * @param targetTable the dataset that owns the SCD2 auxiliary table
   * @param targetTableSchema the AutoCDC target's evolved schema as of the latest pipeline run
   *                          (the union of all flows writing to the target after schema
   *                          evolution), which already carries the SCD2 framework columns
   *                          (`__START_AT`, `__END_AT`, and the CDC metadata column)
   * @param inputAutoCdcFlow the AutoCDC flow writing to `targetTable`
   * @return the SCD2 auxiliary-table spec
   */
  private def buildScd2AuxiliaryTableSpecFor(
      targetTable: Table,
      targetTableSchema: StructType,
      inputAutoCdcFlow: AutoCdcMergeFlow
  ): AuxiliaryTableSpec = {
    val scd2AuxiliaryTableIdentifier = identifier(targetTable.identifier)

    val resolver = inputAutoCdcFlow.effectiveResolver
    val autoCdcKeyColumnNames = inputAutoCdcFlow.changeArgs.keys.map(_.name)

    // Resolve the key fields from the (evolved) target schema, exactly as SCD1 does, so the
    // recorded key names/types used for drift detection come from the same source of truth.
    val keyFields = autoCdcKeyColumnNames.map { keyColumnName =>
      findFieldInTargetSchema(
        targetTableSchema = targetTableSchema,
        targetTableIdentifier = targetTable.identifier,
        autoCdcFlowIdentifier = inputAutoCdcFlow.identifier,
        fieldName = keyColumnName,
        resolver = resolver
      )
    }

    // The SCD2 auxiliary table holds full rows in the SCD2 target row schema, plus the aux-only
    // logical-delete marker column appended last. The marker holds the batchId whose MERGE
    // logically deleted a row (null on live rows), so it is a nullable Long.
    val deletedByBatchIdField =
      StructField(Scd2BatchProcessor.deletedByBatchIdColName, LongType, nullable = true)
    val scd2AuxiliaryTableSchema = StructType(targetTableSchema.fields :+ deletedByBatchIdField)

    // The effective track-history column set, resolved by the flow from its user-selected source
    // schema (see [[AutoCdcMergeFlow.trackHistoryColumnNames]]) -- NOT recomputed here from the
    // evolved target schema, which would keep tracking columns the flow no longer selects and would
    // miss implicit (default / `* EXCEPT`) tracked-set changes. A change in this set reinterprets
    // which transitions open a new historical record, so it is drift-checked.
    val trackHistoryColumnNames = inputAutoCdcFlow.trackHistoryColumnNames.getOrElse(
      throw SparkException.internalError(
        "SCD2 AutoCDC flow is missing its resolved track-history column set."
      )
    )

    val scd2AuxiliaryTableProperties =
      // Record which SCD strategy this auxiliary table serves so downstream readers can identify it
      // without inspecting the schema.
      Map(scdTypePropertyKey -> ScdType.Type2.label) ++
      // Persist the AutoCDC key column names as a JSON list; immutable post-creation (full-refresh
      // is the only way to change it).
      Map(keyColumnNamesProperty -> serializeColumnNames(keyFields.map(_.name))) ++
      // Persist the resolved track-history column names; a change reinterprets already-reconciled
      // history, so it is immutable post-creation (full-refresh is the only way to change it).
      Map(trackHistoryColumnNamesProperty -> serializeColumnNames(trackHistoryColumnNames)) ++
      // Inherit the target's format so MERGE semantics line up. When unspecified, omit the provider
      // so the catalog falls back to its default.
      targetTable.format.map(TableCatalog.PROP_PROVIDER -> _)

    AutoCdcAuxiliaryTableSpec(
      identifier = scd2AuxiliaryTableIdentifier,
      schema = scd2AuxiliaryTableSchema,
      properties = scd2AuxiliaryTableProperties,
      targetTableIdentifier = targetTable.identifier,
      expectedKeyFields = keyFields,
      expectedScdType = ScdType.Type2,
      expectedSequencingType = inputAutoCdcFlow.sequencingType,
      expectedTrackHistoryColumnNames = Some(trackHistoryColumnNames)
    )
  }

  /**
   * Resolve the [[StructField]] named `fieldName` in `targetTableSchema` (the AutoCDC target's
   * evolved schema). The key columns and the CDC metadata column are always present in that schema,
   * so a miss is an implementation invariant and surfaces as an internal error.
   *
   * @param targetTableSchema the AutoCDC target's evolved schema to resolve against
   * @param fieldName the column name to resolve
   * @param resolver the effective resolver used for case-sensitivity-aware field lookups
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
   * Reject an incremental update to an existing AutoCDC target table whose sequencing type has
   * drifted. The AutoCDC sequencing *expression* may legitimately change across runs (e.g. a new
   * timestamp parse format), but its resolved result type must not: the target persists the
   * sequencing type inside its `_cdc_metadata` struct (and, for SCD2, in the interval columns), so
   * a changed type would make new events incomparable with the persisted history and would
   * otherwise surface only as a generic CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE during schema
   * evolution. Runs against the target table (before its schema is evolved), not the auxiliary
   * table. The remedy is a full refresh.
   *
   * @param existingTargetSchema the schema of the already-materialized target table.
   * @param expectedScdType the SCD type of the incoming AutoCDC flow, which determines which inner
   *                        `_cdc_metadata` field carries the recorded sequencing type.
   * @param expectedSequencingType the resolved sequencing type of the incoming AutoCDC flow.
   * @param resolver the effective resolver, used to match the reserved column and inner field names
   *                 the same case-aware way as every other schema lookup in this file.
   */
  private[graph] def validateNoTargetSequencingTypeDrift(
      existingTargetSchema: StructType,
      targetTableIdentifier: TableIdentifier,
      expectedScdType: ScdType,
      expectedSequencingType: DataType,
      resolver: Resolver): Unit = {
    // The sequencing type is embedded as an inner field of the reserved _cdc_metadata struct: for
    // SCD1 the delete/upsert sequence fields, for SCD2 the recordStartAt field. Look the field up
    // by name (not by position) so a future metadata field added at position 0 cannot silently
    // shift this to an unrelated type, and via the resolver so a case-differing hand-written target
    // DDL resolves the same way it does everywhere else. If the metadata column is absent, not a
    // struct, or lacks the expected inner field, this is not a recognizable AutoCDC target state;
    // skip rather than misreport (schema evolution will surface any genuine incompatibility).
    val sequencingFieldName = expectedScdType match {
      case ScdType.Type1 => Scd1BatchProcessor.cdcUpsertSequenceFieldName
      case ScdType.Type2 => Scd2BatchProcessor.recordStartAtFieldName
    }
    val recordedSequencingType: Option[DataType] = existingTargetSchema.fields
      .find(f => resolver(f.name, AutoCdcReservedNames.cdcMetadataColName))
      .map(_.dataType)
      .collect { case s: StructType => s }
      .flatMap(_.fields.find(f => resolver(f.name, sequencingFieldName)))
      .map(_.dataType)

    recordedSequencingType.foreach { recordedType =>
      if (!recordedType.sameType(expectedSequencingType)) {
        throw new AnalysisException(
          errorClass = "AUTOCDC_INVALID_STATE.SEQUENCING_TYPE_DRIFT",
          messageParameters = Map(
            "tableName" -> targetTableIdentifier.unquotedString,
            "expectedSequencingType" -> expectedSequencingType.sql,
            "recordedSequencingType" -> recordedType.sql
          )
        )
      }
    }
  }

  /**
   * Reject an existing SCD2 auxiliary table whose recorded track-history column set differs from
   * `expected` (order-insensitive, resolver-aware). These columns define an SCD2 run - a change in
   * any of them opens a new historical record - so changing the set would reinterpret already
   * reconciled history. The remedy is a full refresh.
   *
   * `expected` is `None` for SCD1 (no track-history concept); the check is then a no-op.
   */
  private[graph] def validateNoTrackHistoryDrift(
      existingAuxiliaryTable: CatalogTable,
      targetTableIdentifier: TableIdentifier,
      expectedTrackHistoryColumnNames: Option[Seq[String]],
      resolver: Resolver): Unit = {
    expectedTrackHistoryColumnNames.foreach { expectedNames =>
      val rawRecorded = Option(
        existingAuxiliaryTable.properties().get(trackHistoryColumnNamesProperty)
      ).getOrElse {
        throw new AnalysisException(
          errorClass = "AUTOCDC_INVALID_STATE.AUXILIARY_TABLE_PROPERTY_MISSING",
          messageParameters = Map(
            "tableName" -> targetTableIdentifier.unquotedString,
            "propertyName" -> trackHistoryColumnNamesProperty
          )
        )
      }
      val recordedNames = parseColumnNames(rawRecorded).getOrElse {
        throw new AnalysisException(
          errorClass = "AUTOCDC_INVALID_STATE.AUXILIARY_TABLE_PROPERTY_MALFORMED",
          messageParameters = Map(
            "tableName" -> targetTableIdentifier.unquotedString,
            "propertyName" -> trackHistoryColumnNamesProperty,
            "rawValue" -> rawRecorded
          )
        )
      }
      // Set equality, resolver-aware: same arity and every expected name has a recorded
      // counterpart. Order is irrelevant to run semantics.
      val drifted =
        recordedNames.length != expectedNames.length ||
        expectedNames.exists(e => !recordedNames.exists(r => resolver(r, e)))
      if (drifted) {
        throw new AnalysisException(
          errorClass = "AUTOCDC_INVALID_STATE.TRACK_HISTORY_DRIFT",
          messageParameters = Map(
            "tableName" -> targetTableIdentifier.unquotedString,
            "expectedTrackHistoryColumns" -> expectedNames.mkString(", "),
            "recordedTrackHistoryColumns" -> recordedNames.mkString(", ")
          )
        )
      }
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
    parseColumnNames(rawKeyColumnNamesStr).getOrElse {
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
