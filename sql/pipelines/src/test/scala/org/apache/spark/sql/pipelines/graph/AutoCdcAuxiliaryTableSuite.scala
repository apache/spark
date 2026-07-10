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
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.pipelines.autocdc.ScdType

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
    val json = AutoCdcAuxiliaryTable.serializeKeyColumnNames(names)
    assert(
      AutoCdcAuxiliaryTable.parseKeyColumnNames(json).contains(names),
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

  test("serializeKeyColumnNames/parseKeyColumnNames round-trip preserves plain ASCII names") {
    assertKeyColumnNamesRoundTrip(Seq("id"))
    assertKeyColumnNamesRoundTrip(Seq("id", "region"))
    assertKeyColumnNamesRoundTrip(Seq("id", "region", "country"))
  }

  test("serializeKeyColumnNames/parseKeyColumnNames round-trip preserves the empty list") {
    // Empty key sets are not user-reachable (AutoCdcMergeFlow rejects them upstream), but the
    // helpers themselves must round-trip a `[]` JSON array faithfully.
    assertKeyColumnNamesRoundTrip(Seq.empty)
  }

  test("serializeKeyColumnNames/parseKeyColumnNames preserves names containing JSON-escaped " +
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

  test("serializeKeyColumnNames/parseKeyColumnNames preserves names containing characters " +
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

  test("parseKeyColumnNames returns None for inputs that are not a JSON array of strings") {
    // None of these are a top-level JSON array of strings; the parser must reject every shape
    // with `None` so callers can surface a structured INTERNAL_ERROR with consistent wording.
    assert(AutoCdcAuxiliaryTable.parseKeyColumnNames("not-json").isEmpty)
    assert(AutoCdcAuxiliaryTable.parseKeyColumnNames("").isEmpty)
    assert(AutoCdcAuxiliaryTable.parseKeyColumnNames("\"id\"").isEmpty)        // bare string
    assert(AutoCdcAuxiliaryTable.parseKeyColumnNames("null").isEmpty)
    assert(AutoCdcAuxiliaryTable.parseKeyColumnNames("{\"id\": 1}").isEmpty)   // object
    assert(AutoCdcAuxiliaryTable.parseKeyColumnNames("[1, 2, 3]").isEmpty)     // numbers
    assert(AutoCdcAuxiliaryTable.parseKeyColumnNames("[\"id\", 1]").isEmpty)   // mixed types
    assert(AutoCdcAuxiliaryTable.parseKeyColumnNames("[\"id\", null]").isEmpty)
    assert(AutoCdcAuxiliaryTable.parseKeyColumnNames("[[\"id\"]]").isEmpty)    // nested array
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
}
