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

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{caseInsensitiveResolution, caseSensitiveResolution}
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.pipelines.autocdc.{
  AutoCdcReservedNames,
  Scd1BatchProcessor,
  Scd2BatchProcessor,
  ScdType
}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

/**
 * Unit tests for the [[AutoCdcAuxiliaryTable]] companion object.
 *
 * These tests are intentionally session-less: the helpers are pure functions on `String` and
 * `Seq[String]`, and verifying their byte-for-byte round-trip contract requires no Spark
 * runtime. End-to-end persistence (DDL -> catalog -> SHOW TBLPROPERTIES) is covered by
 * `AutoCdcScd1AuxiliaryTableDurabilitySuite`; drift-validator behavior over the parsed
 * property is covered by `AutoCdcScd1KeyDriftSuite`.
 */
class AutoCdcAuxiliaryTableSuite extends SparkFunSuite {

  // The drift validator stores key column names in a table property as a JSON array of strings.
  // These round-trip tests verify that identifier text is preserved verbatim through
  // serialize -> parse, including characters that JSON itself must escape (`"`, `\`, control
  // chars) and characters that JSON does not touch but that downstream interpolation might
  // (`'`, ` `, `.`, backtick). Storage at the table property level is solely the JSON layer's
  // concern -- SQL identifier quoting (backticks) is never part of the stored bytes.

  private def assertKeyColumnNamesRoundTrip(names: Seq[String]): Unit = {
    val json = AutoCdcAuxiliaryTable.serializeColumnNames(names)
    assert(
      AutoCdcAuxiliaryTable.parseColumnNames(json).contains(names),
      s"round-trip failed: input=${names}, serialized=${json}"
    )
  }

  /** Minimal [[Table]] stub exposing only the properties map the SCD-type validator reads. */
  private def auxTableWithProperties(props: Map[String, String]): Table = new Table {
    override def name(): String = "aux"
    override def capabilities(): java.util.Set[TableCapability] =
      Set.empty[TableCapability].asJava
    override def properties(): java.util.Map[String, String] = props.asJava
  }

  test("serializeColumnNames/parseColumnNames round-trip preserves plain ASCII names") {
    assertKeyColumnNamesRoundTrip(Seq("id"))
    assertKeyColumnNamesRoundTrip(Seq("id", "region"))
    assertKeyColumnNamesRoundTrip(Seq("id", "region", "country"))
  }

  test("serializeColumnNames/parseColumnNames round-trip preserves the empty list") {
    // Empty key sets are not user-reachable (AutoCdcMergeFlow rejects them upstream), but the
    // helpers themselves must round-trip a `[]` JSON array faithfully.
    assertKeyColumnNamesRoundTrip(Seq.empty)
  }

  test("serializeColumnNames/parseColumnNames preserves names containing JSON-escaped " +
    "characters (quote, backslash, control chars)") {
    // JSON serializer must escape `"` -> `\"`, `\` -> `\\`, and control chars; the parser
    // must invert those escapes and yield the original literal bytes.
    assertKeyColumnNamesRoundTrip(Seq("a\"b"))
    assertKeyColumnNamesRoundTrip(Seq("a\\b"))
    assertKeyColumnNamesRoundTrip(Seq("a\nb"))
    assertKeyColumnNamesRoundTrip(Seq("a\tb"))
    // Mixed: every JSON-escaped class in a single name.
    assertKeyColumnNamesRoundTrip(Seq("a\"b\\c\nd"))
  }

  test("serializeColumnNames/parseColumnNames preserves names containing characters " +
    "that JSON does not escape (single quote, dot, space, backtick)") {
    // JSON does not escape these, but they are common in real-world identifiers (especially
    // when users backtick-quote at the API boundary). They must flow through verbatim.
    assertKeyColumnNamesRoundTrip(Seq("it's"))
    assertKeyColumnNamesRoundTrip(Seq("a.b"))
    assertKeyColumnNamesRoundTrip(Seq("name with spaces"))
    assertKeyColumnNamesRoundTrip(Seq("a`b"))
    // Mixed: a single composite key whose pieces collectively touch every "passes verbatim"
    // class.
    assertKeyColumnNamesRoundTrip(Seq("it's", "name with spaces", "a.b.c", "back`tick"))
  }

  test("parseColumnNames returns None for inputs that are not a JSON array of strings") {
    // None of these are a top-level JSON array of strings; the parser must reject every shape
    // with `None` so callers can surface a structured INTERNAL_ERROR with consistent wording.
    assert(AutoCdcAuxiliaryTable.parseColumnNames("not-json").isEmpty)
    assert(AutoCdcAuxiliaryTable.parseColumnNames("").isEmpty)
    assert(AutoCdcAuxiliaryTable.parseColumnNames("\"id\"").isEmpty)        // bare string
    assert(AutoCdcAuxiliaryTable.parseColumnNames("null").isEmpty)
    assert(AutoCdcAuxiliaryTable.parseColumnNames("{\"id\": 1}").isEmpty)   // object
    assert(AutoCdcAuxiliaryTable.parseColumnNames("[1, 2, 3]").isEmpty)     // numbers
    assert(AutoCdcAuxiliaryTable.parseColumnNames("[\"id\", 1]").isEmpty)   // mixed types
    assert(AutoCdcAuxiliaryTable.parseColumnNames("[\"id\", null]").isEmpty)
    assert(AutoCdcAuxiliaryTable.parseColumnNames("[[\"id\"]]").isEmpty)    // nested array
  }

  test("validateNoScdTypeDrift accepts an auxiliary table whose recorded SCD type matches") {
    val existing =
      auxTableWithProperties(Map(AutoCdcAuxiliaryTable.scdTypePropertyKey -> ScdType.Type1.label))
    // Must not throw.
    AutoCdcAuxiliaryTable.validateNoScdTypeDrift(
      existingAuxiliaryTable = existing,
      targetTableIdentifier = TableIdentifier("target", Some("ns"), Some("cat")),
      expectedScdType = ScdType.Type1)
  }

  test("validateNoScdTypeDrift throws SCD_TYPE_DRIFT when the recorded SCD type differs") {
    val existing =
      auxTableWithProperties(Map(AutoCdcAuxiliaryTable.scdTypePropertyKey -> ScdType.Type2.label))
    val ex = intercept[AnalysisException] {
      AutoCdcAuxiliaryTable.validateNoScdTypeDrift(
        existingAuxiliaryTable = existing,
        targetTableIdentifier = TableIdentifier("target", Some("ns"), Some("cat")),
        expectedScdType = ScdType.Type1)
    }
    checkError(
      exception = ex,
      condition = "AUTOCDC_INVALID_STATE.SCD_TYPE_DRIFT",
      sqlState = "42000",
      parameters = Map(
        "tableName" -> TableIdentifier("target", Some("ns"), Some("cat")).unquotedString,
        "expectedScdType" -> ScdType.Type1.label,
        "recordedScdType" -> ScdType.Type2.label))
  }

  test("validateNoScdTypeDrift throws AUXILIARY_TABLE_PROPERTY_MISSING when scdType is absent") {
    // Simulates corrupt/externally-modified metadata (e.g. `ALTER TABLE ... UNSET TBLPROPERTIES`).
    val existing = auxTableWithProperties(Map.empty)
    val ex = intercept[AnalysisException] {
      AutoCdcAuxiliaryTable.validateNoScdTypeDrift(
        existingAuxiliaryTable = existing,
        targetTableIdentifier = TableIdentifier("target", Some("ns"), Some("cat")),
        expectedScdType = ScdType.Type1)
    }
    checkError(
      exception = ex,
      condition = "AUTOCDC_INVALID_STATE.AUXILIARY_TABLE_PROPERTY_MISSING",
      sqlState = "42000",
      parameters = Map(
        "tableName" -> TableIdentifier("target", Some("ns"), Some("cat")).unquotedString,
        "propertyName" -> AutoCdcAuxiliaryTable.scdTypePropertyKey))
  }

  private val targetIdent = TableIdentifier("target", Some("ns"), Some("cat"))

  /** An auxiliary table stub recording the given track-history column names as JSON. */
  private def auxTableWithTrackHistory(names: Seq[String]): Table =
    auxTableWithProperties(Map(
      AutoCdcAuxiliaryTable.trackHistoryColumnNamesProperty ->
        AutoCdcAuxiliaryTable.serializeColumnNames(names)))

  test("validateNoTrackHistoryDrift is a no-op when the expected column set is None") {
    // A None expected set means the flow does not constrain track-history (SCD1, or an SCD2 flow
    // whose default resolution has not been computed here); the validator must not even read the
    // property. Passing an empty-properties table proves nothing is dereferenced.
    AutoCdcAuxiliaryTable.validateNoTrackHistoryDrift(
      existingAuxiliaryTable = auxTableWithProperties(Map.empty),
      targetTableIdentifier = targetIdent,
      expectedTrackHistoryColumnNames = None,
      resolver = caseInsensitiveResolution)
  }

  test("validateNoTrackHistoryDrift accepts a recorded set that matches regardless of order") {
    val existing = auxTableWithTrackHistory(Seq("name", "amount", "seq"))
    // Same set, different order: must not throw.
    AutoCdcAuxiliaryTable.validateNoTrackHistoryDrift(
      existingAuxiliaryTable = existing,
      targetTableIdentifier = targetIdent,
      expectedTrackHistoryColumnNames = Some(Seq("seq", "name", "amount")),
      resolver = caseInsensitiveResolution)
  }

  test("validateNoTrackHistoryDrift compares case-insensitively under the default resolver, " +
    "even when the stored property names differ only in case") {
    // Isolates the resolver-aware comparison: the stored property holds `Name`/`AMOUNT` while the
    // expected set holds `name`/`amount`. In the end-to-end path both sides are normalized to
    // actual schema field names before comparison, so only a direct unit test can exercise a
    // genuine case difference reaching the resolver. Under the default resolver, no drift.
    val existing = auxTableWithTrackHistory(Seq("Name", "AMOUNT"))
    AutoCdcAuxiliaryTable.validateNoTrackHistoryDrift(
      existingAuxiliaryTable = existing,
      targetTableIdentifier = targetIdent,
      expectedTrackHistoryColumnNames = Some(Seq("name", "amount")),
      resolver = caseInsensitiveResolution)
  }

  test("validateNoTrackHistoryDrift throws TRACK_HISTORY_DRIFT under the case-sensitive resolver " +
    "when the stored property names differ only in case") {
    // The mirror of the case-insensitive test: with the case-sensitive resolver, `Name` and
    // `name` are distinct, so the same-cardinality sets do not match and the validator drifts.
    val existing = auxTableWithTrackHistory(Seq("Name", "amount"))
    val ex = intercept[AnalysisException] {
      AutoCdcAuxiliaryTable.validateNoTrackHistoryDrift(
        existingAuxiliaryTable = existing,
        targetTableIdentifier = targetIdent,
        expectedTrackHistoryColumnNames = Some(Seq("name", "amount")),
        resolver = caseSensitiveResolution)
    }
    checkError(
      exception = ex,
      condition = "AUTOCDC_INVALID_STATE.TRACK_HISTORY_DRIFT",
      sqlState = "42000",
      parameters = Map(
        "tableName" -> targetIdent.unquotedString,
        "expectedTrackHistoryColumns" -> "name, amount",
        "recordedTrackHistoryColumns" -> "Name, amount"))
  }

  test("validateNoTrackHistoryDrift throws TRACK_HISTORY_DRIFT when the recorded set differs") {
    val existing = auxTableWithTrackHistory(Seq("name"))
    val ex = intercept[AnalysisException] {
      AutoCdcAuxiliaryTable.validateNoTrackHistoryDrift(
        existingAuxiliaryTable = existing,
        targetTableIdentifier = targetIdent,
        expectedTrackHistoryColumnNames = Some(Seq("amount")),
        resolver = caseInsensitiveResolution)
    }
    checkError(
      exception = ex,
      condition = "AUTOCDC_INVALID_STATE.TRACK_HISTORY_DRIFT",
      sqlState = "42000",
      parameters = Map(
        "tableName" -> targetIdent.unquotedString,
        "expectedTrackHistoryColumns" -> "amount",
        "recordedTrackHistoryColumns" -> "name"))
  }

  test("validateNoTrackHistoryDrift throws AUXILIARY_TABLE_PROPERTY_MISSING when the " +
    "track-history property is absent") {
    // An SCD2 aux table created before this property existed: the validator must surface a
    // structured error (remedy: full refresh) rather than skipping the check.
    val ex = intercept[AnalysisException] {
      AutoCdcAuxiliaryTable.validateNoTrackHistoryDrift(
        existingAuxiliaryTable = auxTableWithProperties(Map.empty),
        targetTableIdentifier = targetIdent,
        expectedTrackHistoryColumnNames = Some(Seq("name")),
        resolver = caseInsensitiveResolution)
    }
    checkError(
      exception = ex,
      condition = "AUTOCDC_INVALID_STATE.AUXILIARY_TABLE_PROPERTY_MISSING",
      sqlState = "42000",
      parameters = Map(
        "tableName" -> targetIdent.unquotedString,
        "propertyName" -> AutoCdcAuxiliaryTable.trackHistoryColumnNamesProperty))
  }

  test("validateNoTrackHistoryDrift throws AUXILIARY_TABLE_PROPERTY_MALFORMED when the " +
    "track-history property is not a JSON array of strings") {
    val existing = auxTableWithProperties(Map(
      AutoCdcAuxiliaryTable.trackHistoryColumnNamesProperty -> "not-a-json-array"))
    val ex = intercept[AnalysisException] {
      AutoCdcAuxiliaryTable.validateNoTrackHistoryDrift(
        existingAuxiliaryTable = existing,
        targetTableIdentifier = targetIdent,
        expectedTrackHistoryColumnNames = Some(Seq("name")),
        resolver = caseInsensitiveResolution)
    }
    checkError(
      exception = ex,
      condition = "AUTOCDC_INVALID_STATE.AUXILIARY_TABLE_PROPERTY_MALFORMED",
      sqlState = "42000",
      parameters = Map(
        "tableName" -> targetIdent.unquotedString,
        "propertyName" -> AutoCdcAuxiliaryTable.trackHistoryColumnNamesProperty,
        "rawValue" -> "not-a-json-array"))
  }

  // ===========================================================================================
  // validateNoTargetSequencingTypeDrift
  // ===========================================================================================

  private val meta = AutoCdcReservedNames.cdcMetadataColName

  /** An SCD1 target schema whose `_cdc_metadata` carries sequence fields of `seqType`. */
  private def scd1TargetSchema(seqType: org.apache.spark.sql.types.DataType): StructType =
    new StructType()
      .add("id", IntegerType, nullable = false)
      .add(meta, new StructType()
        .add(Scd1BatchProcessor.cdcDeleteSequenceFieldName, seqType)
        .add(Scd1BatchProcessor.cdcUpsertSequenceFieldName, seqType))

  /** An SCD2 target schema whose `_cdc_metadata` carries a recordStartAt field of `seqType`. */
  private def scd2TargetSchema(seqType: org.apache.spark.sql.types.DataType): StructType =
    new StructType()
      .add("id", IntegerType, nullable = false)
      .add(meta, new StructType()
        .add(Scd2BatchProcessor.recordStartAtFieldName, seqType))

  test("validateNoTargetSequencingTypeDrift accepts a matching SCD1 sequencing type") {
    AutoCdcAuxiliaryTable.validateNoTargetSequencingTypeDrift(
      existingTargetSchema = scd1TargetSchema(LongType),
      targetTableIdentifier = targetIdent,
      expectedScdType = ScdType.Type1,
      expectedSequencingType = LongType,
      resolver = caseInsensitiveResolution)
  }

  test("validateNoTargetSequencingTypeDrift accepts a matching SCD2 sequencing type") {
    AutoCdcAuxiliaryTable.validateNoTargetSequencingTypeDrift(
      existingTargetSchema = scd2TargetSchema(LongType),
      targetTableIdentifier = targetIdent,
      expectedScdType = ScdType.Type2,
      expectedSequencingType = LongType,
      resolver = caseInsensitiveResolution)
  }

  test("validateNoTargetSequencingTypeDrift throws SEQUENCING_TYPE_DRIFT when the recorded " +
    "type differs (SCD1)") {
    val ex = intercept[AnalysisException] {
      AutoCdcAuxiliaryTable.validateNoTargetSequencingTypeDrift(
        existingTargetSchema = scd1TargetSchema(LongType),
        targetTableIdentifier = targetIdent,
        expectedScdType = ScdType.Type1,
        expectedSequencingType = IntegerType,
        resolver = caseInsensitiveResolution)
    }
    checkError(
      exception = ex,
      condition = "AUTOCDC_INVALID_STATE.SEQUENCING_TYPE_DRIFT",
      sqlState = "42000",
      parameters = Map(
        "tableName" -> targetIdent.unquotedString,
        "expectedSequencingType" -> IntegerType.sql,
        "recordedSequencingType" -> LongType.sql))
  }

  test("validateNoTargetSequencingTypeDrift throws SEQUENCING_TYPE_DRIFT when the recorded " +
    "type differs (SCD2)") {
    val ex = intercept[AnalysisException] {
      AutoCdcAuxiliaryTable.validateNoTargetSequencingTypeDrift(
        existingTargetSchema = scd2TargetSchema(LongType),
        targetTableIdentifier = targetIdent,
        expectedScdType = ScdType.Type2,
        expectedSequencingType = IntegerType,
        resolver = caseInsensitiveResolution)
    }
    checkError(
      exception = ex,
      condition = "AUTOCDC_INVALID_STATE.SEQUENCING_TYPE_DRIFT",
      sqlState = "42000",
      parameters = Map(
        "tableName" -> targetIdent.unquotedString,
        "expectedSequencingType" -> IntegerType.sql,
        "recordedSequencingType" -> LongType.sql))
  }

  test("validateNoTargetSequencingTypeDrift is a silent no-op when _cdc_metadata is absent") {
    // Not a recognizable AutoCDC target state: skip rather than misreport. A genuine
    // incompatibility would surface later during schema evolution.
    AutoCdcAuxiliaryTable.validateNoTargetSequencingTypeDrift(
      existingTargetSchema = new StructType().add("id", IntegerType, nullable = false),
      targetTableIdentifier = targetIdent,
      expectedScdType = ScdType.Type2,
      expectedSequencingType = IntegerType,
      resolver = caseInsensitiveResolution)
  }

  test("validateNoTargetSequencingTypeDrift is a silent no-op when _cdc_metadata is not a " +
    "struct") {
    AutoCdcAuxiliaryTable.validateNoTargetSequencingTypeDrift(
      existingTargetSchema = new StructType()
        .add("id", IntegerType, nullable = false)
        .add(meta, StringType),
      targetTableIdentifier = targetIdent,
      expectedScdType = ScdType.Type2,
      expectedSequencingType = IntegerType,
      resolver = caseInsensitiveResolution)
  }

  test("validateNoTargetSequencingTypeDrift is a silent no-op when _cdc_metadata is an empty " +
    "struct") {
    AutoCdcAuxiliaryTable.validateNoTargetSequencingTypeDrift(
      existingTargetSchema = new StructType()
        .add("id", IntegerType, nullable = false)
        .add(meta, new StructType()),
      targetTableIdentifier = targetIdent,
      expectedScdType = ScdType.Type2,
      expectedSequencingType = IntegerType,
      resolver = caseInsensitiveResolution)
  }

  test("validateNoTargetSequencingTypeDrift resolves _cdc_metadata and the inner field via the " +
    "resolver (case-insensitive)") {
    // A hand-written target DDL may differ in case; under the default resolver the check still
    // finds the metadata column and the recordStartAt field, so a real type drift is caught.
    val upperCasedSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add(meta.toUpperCase(java.util.Locale.ROOT), new StructType()
        .add(Scd2BatchProcessor.recordStartAtFieldName.toLowerCase(java.util.Locale.ROOT),
          LongType))
    val ex = intercept[AnalysisException] {
      AutoCdcAuxiliaryTable.validateNoTargetSequencingTypeDrift(
        existingTargetSchema = upperCasedSchema,
        targetTableIdentifier = targetIdent,
        expectedScdType = ScdType.Type2,
        expectedSequencingType = IntegerType,
        resolver = caseInsensitiveResolution)
    }
    assert(ex.getCondition == "AUTOCDC_INVALID_STATE.SEQUENCING_TYPE_DRIFT")
  }
}
