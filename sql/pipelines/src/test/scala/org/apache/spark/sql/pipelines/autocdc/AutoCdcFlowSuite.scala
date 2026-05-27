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

package org.apache.spark.sql.pipelines.autocdc

import java.util.Locale

import org.scalatest.BeforeAndAfterEach

import scala.util.Success

import org.apache.spark.sql.{functions => F, AnalysisException, Column, QueryTest}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.connector.catalog.SharedTablesInMemoryRowLevelOperationTableCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.pipelines.graph.{
  AutoCdcAuxiliaryTable,
  AutoCdcFlow,
  AutoCdcMergeFlow,
  FlowFunction,
  FlowFunctionResult,
  Input,
  QueryContext,
  QueryOrigin
}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructField, StructType}

/**
 * Unit tests for the [[AutoCdcFlow]] and [[AutoCdcMergeFlow]] that do not execute graph analysis
 * or execution.
 */
class AutoCdcFlowSuite extends QueryTest with SharedSparkSession with BeforeAndAfterEach {

  private val testIdentifier = TableIdentifier("cdc_target", Some("db"))

  // V2 catalog used by drift-validation tests below to pre-create an auxiliary table that the
  // [[AutoCdcMergeFlow]] constructor will read at construction time. Keep separate from the
  // default `testIdentifier` (which has no catalog) so non-drift tests don't need the catalog
  // setup.
  private val driftTestCatalog: String = "cat"
  private val driftTestNamespace: String = "ns1"

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    spark.conf.set(
      s"spark.sql.catalog.$driftTestCatalog",
      classOf[SharedTablesInMemoryRowLevelOperationTableCatalog].getName
    )
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $driftTestCatalog.$driftTestNamespace")
  }

  override protected def afterEach(): Unit = {
    SharedTablesInMemoryRowLevelOperationTableCatalog.reset()
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.unsetConf(s"spark.sql.catalog.$driftTestCatalog")
    super.afterEach()
  }

  /** A no-op [[FlowFunction]] that throws if invoked; AutoCdcFlow tests should never call it. */
  private val noOpFlowFunction: FlowFunction = new FlowFunction {
    override def call(
        allInputs: Set[TableIdentifier],
        availableInputs: Seq[Input],
        configuration: Map[String, String],
        queryContext: QueryContext,
        queryOrigin: QueryOrigin): FlowFunctionResult =
      throw new UnsupportedOperationException(
        "noOpFlowFunction.call should not be invoked from AutoCdcFlowSuite tests"
      )
  }

  private val testQueryContext =
    QueryContext(currentCatalog = Some("test_catalog"), currentDatabase = Some("test_db"))

  private val testChangeArgs = ChangeArgs(
    keys = Seq(UnqualifiedColumnName("id")),
    sequencing = F.col("seq"),
    storedAsScdType = ScdType.Type1
  )

  private def newAutoCdcFlow(
      identifier: TableIdentifier = testIdentifier,
      destinationIdentifier: TableIdentifier = testIdentifier,
      func: FlowFunction = noOpFlowFunction,
      queryContext: QueryContext = testQueryContext,
      sqlConf: Map[String, String] = Map.empty,
      comment: Option[String] = None,
      origin: QueryOrigin = QueryOrigin.empty,
      changeArgs: ChangeArgs = testChangeArgs): AutoCdcFlow = {
    AutoCdcFlow(
      identifier = identifier,
      destinationIdentifier = destinationIdentifier,
      func = func,
      queryContext = queryContext,
      sqlConf = sqlConf,
      comment = comment,
      origin = origin,
      changeArgs = changeArgs
    )
  }

  test("AutoCdcFlow exposes its constructor fields") {
    val flow = newAutoCdcFlow(
      sqlConf = Map("spark.sql.shuffle.partitions" -> "8"),
      comment = Some("my CDC flow")
    )

    assert(flow.identifier == testIdentifier)
    assert(flow.destinationIdentifier == testIdentifier)
    assert(flow.func eq noOpFlowFunction)
    assert(flow.queryContext == testQueryContext)
    assert(flow.sqlConf == Map("spark.sql.shuffle.partitions" -> "8"))
    assert(flow.comment.contains("my CDC flow"))
    assert(flow.origin == QueryOrigin.empty)
    assert(flow.changeArgs == testChangeArgs)
  }

  test("AutoCdcFlow defaults sqlConf to empty and comment to None") {
    // Confirms the case-class default values match the documented contract; downstream
    // registration code relies on `sqlConf` being a non-null empty map by default so that
    // `defaultSqlConf ++ flowDef.sqlConf` is well-defined in [[GraphRegistrationContext]].
    val flow = AutoCdcFlow(
      identifier = testIdentifier,
      destinationIdentifier = testIdentifier,
      func = noOpFlowFunction,
      queryContext = testQueryContext,
      origin = QueryOrigin.empty,
      changeArgs = testChangeArgs
    )

    assert(flow.sqlConf.isEmpty)
    assert(flow.comment.isEmpty)
  }

  test("AutoCdcFlow.once is always false") {
    // AutoCDC flows are streaming-only and must run on every batch trigger, never as a
    // one-shot full-refresh-style flow. Locking this in so a future refactor doesn't
    // accidentally make `once` configurable.

    // In the future we may intentionally add [[once]] support for AutoCDC flows, at which point
    // this test can safely be removed.
    val flow = newAutoCdcFlow()
    assert(!flow.once)
  }

  test("AutoCdcFlow.withSqlConf returns a new instance with the updated sqlConf") {
    val original = newAutoCdcFlow(sqlConf = Map("a" -> "1"))
    val updated = original.withSqlConf(Map("b" -> "2"))

    assert(updated.sqlConf == Map("b" -> "2"))
    // All other fields should be preserved verbatim.
    assert(updated.identifier == original.identifier)
    assert(updated.destinationIdentifier == original.destinationIdentifier)
    assert(updated.func eq original.func)
    assert(updated.queryContext == original.queryContext)
    assert(updated.comment == original.comment)
    assert(updated.origin == original.origin)
    assert(updated.changeArgs == original.changeArgs)
    // The original must not be mutated.
    assert(original.sqlConf == Map("a" -> "1"))
  }

  // ===========================================================================================
  // AutoCdcMergeFlow.schema tests
  // ===========================================================================================

  /** Materializes a successful [[FlowFunctionResult]] backed by the given source dataframe. */
  private def successfulFuncResult(sourceDf: DataFrame): FlowFunctionResult =
    FlowFunctionResult(
      requestedInputs = Set.empty,
      batchInputs = Set.empty,
      streamingInputs = Set.empty,
      usedExternalInputs = Set.empty,
      dataFrame = Success(sourceDf),
      sqlConf = Map.empty
    )

  /** Builds a [[AutoCdcMergeFlow]] over the given source dataframe + change args. */
  private def newAutoCdcMergeFlow(
      sourceDf: DataFrame,
      keys: Seq[UnqualifiedColumnName] = Seq(UnqualifiedColumnName("id")),
      sequencing: Column = F.col("seq"),
      storedAsScdType: ScdType = ScdType.Type1,
      columnSelection: Option[ColumnSelection] = None): AutoCdcMergeFlow = {
    val flow = newAutoCdcFlow(
      changeArgs = ChangeArgs(
        keys = keys,
        sequencing = sequencing,
        storedAsScdType = storedAsScdType,
        columnSelection = columnSelection
      )
    )
    new AutoCdcMergeFlow(flow, successfulFuncResult(sourceDf))
  }

  /** A stable 3-column source CDF schema used across most schema tests. */
  private def threeColumnSourceDf(): DataFrame = {
    val schema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType)
      .add("seq", LongType)
    spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], schema)
  }

  /** Convenience to extract the [[StructType]] of the projected `_cdc_metadata` column. */
  private def cdcMetadataStruct(schema: StructType): StructType =
    schema(Scd1BatchProcessor.cdcMetadataColName).dataType.asInstanceOf[StructType]

  test(
    "AutoCdcMergeFlow.schema appends _cdc_metadata to the source schema when no " +
    "columnSelection is set"
  ) {
    val resolvedFlow = newAutoCdcMergeFlow(threeColumnSourceDf())

    val expected = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType)
      .add("seq", LongType)
      .add(
        StructField(
          Scd1BatchProcessor.cdcMetadataColName,
          Scd1BatchProcessor.cdcMetadataColSchema(LongType),
          nullable = false
        )
      )
    assert(resolvedFlow.schema == expected)
  }

  test("AutoCdcMergeFlow.schema applies an IncludeColumns selection") {
    val resolvedFlow = newAutoCdcMergeFlow(
      sourceDf = threeColumnSourceDf(),
      columnSelection = Some(
        ColumnSelection.IncludeColumns(
          Seq(UnqualifiedColumnName("id"), UnqualifiedColumnName("seq"))
        )
      )
    )

    val expected = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("seq", LongType)
      .add(
        StructField(
          Scd1BatchProcessor.cdcMetadataColName,
          Scd1BatchProcessor.cdcMetadataColSchema(LongType),
          nullable = false
        )
      )
    assert(resolvedFlow.schema == expected)
  }

  test("AutoCdcMergeFlow.schema applies an ExcludeColumns selection") {
    val resolvedFlow = newAutoCdcMergeFlow(
      sourceDf = threeColumnSourceDf(),
      columnSelection = Some(
        ColumnSelection.ExcludeColumns(Seq(UnqualifiedColumnName("name")))
      )
    )

    val expected = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("seq", LongType)
      .add(
        StructField(
          Scd1BatchProcessor.cdcMetadataColName,
          Scd1BatchProcessor.cdcMetadataColSchema(LongType),
          nullable = false
        )
      )
    assert(resolvedFlow.schema == expected)
  }

  test(
    "AutoCdcMergeFlow.schema's _cdc_metadata struct uses the resolved sequencing data type"
  ) {
    // Source has a Long `seq` column; sequencing is `cast(seq as int)`, so the projected
    // `_cdc_metadata` fields should be Int (not Long), demonstrating that the sequencing
    // expression's *resolved* type drives the metadata schema.
    val resolvedFlow = newAutoCdcMergeFlow(
      sourceDf = threeColumnSourceDf(),
      sequencing = F.col("seq").cast(IntegerType)
    )

    val metaStruct = cdcMetadataStruct(resolvedFlow.schema)
    assert(metaStruct == Scd1BatchProcessor.cdcMetadataColSchema(IntegerType))
  }

  test("AutoCdcMergeFlow.schema's _cdc_metadata field is non-null with nullable inner fields") {
    val resolvedFlow = newAutoCdcMergeFlow(threeColumnSourceDf())

    val metaField = resolvedFlow.schema(Scd1BatchProcessor.cdcMetadataColName)
    assert(!metaField.nullable, "_cdc_metadata column itself must be non-null")

    val metaStruct = metaField.dataType.asInstanceOf[StructType]
    assert(metaStruct(Scd1BatchProcessor.cdcDeleteSequenceFieldName).nullable)
    assert(metaStruct(Scd1BatchProcessor.cdcUpsertSequenceFieldName).nullable)
  }

  test("AutoCdcMergeFlow.schema is stable across reads") {
    // The schema computation calls `df.select(sequencing).schema`, which triggers Spark
    // analysis. The eagerly-initialized `val` caches the result so downstream consumers get
    // a stable schema instance across reads.
    val resolvedFlow = newAutoCdcMergeFlow(threeColumnSourceDf())
    val first = resolvedFlow.schema
    val second = resolvedFlow.schema
    assert(first eq second, "schema should be cached as a val and return the same instance")
  }

  test("AutoCdcMergeFlow rejects SCD2 at construction with AUTOCDC_SCD2_NOT_SUPPORTED") {
    // Constructing the flow forces the resolved schema, which is unsupported for SCD2 today.
    // Failing eagerly (rather than deferring to the first downstream `schema` read) is the
    // intended UX -- pipeline graph analysis should not be able to register an SCD2 AutoCDC
    // flow at all.
    checkError(
      exception = intercept[AnalysisException] {
        newAutoCdcMergeFlow(
          sourceDf = threeColumnSourceDf(),
          storedAsScdType = ScdType.Type2
        )
      },
      condition = "AUTOCDC_SCD2_NOT_SUPPORTED",
      sqlState = "0A000",
      parameters = Map.empty
    )
  }

  // ===========================================================================================
  // AutoCdcMergeFlow.load() contract tests
  // ===========================================================================================

  test("AutoCdcMergeFlow.load() schema matches AutoCdcMergeFlow.schema") {
    val resolvedFlow = newAutoCdcMergeFlow(threeColumnSourceDf())
    val loadedDf = resolvedFlow.load(readOptions = null)
    assert(loadedDf.schema == resolvedFlow.schema)
  }

  test("AutoCdcMergeFlow.load() respects an IncludeColumns selection") {
    val resolvedFlow = newAutoCdcMergeFlow(
      sourceDf = threeColumnSourceDf(),
      columnSelection = Some(
        ColumnSelection.IncludeColumns(
          Seq(UnqualifiedColumnName("id"), UnqualifiedColumnName("seq"))
        )
      )
    )
    val loadedDf = resolvedFlow.load(readOptions = null)
    assert(loadedDf.schema == resolvedFlow.schema)
    // The user-selected portion drops `name`; the trailing column is the SCD1 metadata.
    assert(
      loadedDf.schema.fieldNames.toSeq ==
      Seq("id", "seq", Scd1BatchProcessor.cdcMetadataColName)
    )
  }

  test("AutoCdcMergeFlow.load() respects an ExcludeColumns selection") {
    val resolvedFlow = newAutoCdcMergeFlow(
      sourceDf = threeColumnSourceDf(),
      columnSelection = Some(
        ColumnSelection.ExcludeColumns(Seq(UnqualifiedColumnName("name")))
      )
    )
    val loadedDf = resolvedFlow.load(readOptions = null)
    assert(loadedDf.schema == resolvedFlow.schema)
    assert(
      loadedDf.schema.fieldNames.toSeq ==
      Seq("id", "seq", Scd1BatchProcessor.cdcMetadataColName)
    )
  }

  test("AutoCdcMergeFlow.load() materializes the CDC metadata column as null-filled") {
    // The merge engine fills in the metadata at execution time; at planning time we synthesize
    // a typed null placeholder so that load().schema matches schema. This test pins down the
    // placeholder shape: outer struct non-null, inner fields null-filled.
    val schema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType)
      .add("seq", LongType)
    val sourceRows = java.util.Arrays.asList(
      org.apache.spark.sql.Row(1, "a", 100L),
      org.apache.spark.sql.Row(2, "b", 200L)
    )
    val sourceDf = spark.createDataFrame(sourceRows, schema)
    val resolvedFlow = newAutoCdcMergeFlow(sourceDf)

    val loadedDf = resolvedFlow.load(readOptions = null)
    val collected = loadedDf.collect()
    assert(collected.length == 2)

    val metaIdx = loadedDf.schema.fieldIndex(Scd1BatchProcessor.cdcMetadataColName)
    collected.foreach { row =>
      assert(!row.isNullAt(metaIdx), "_cdc_metadata struct itself should be non-null")
      val metaRow = row.getStruct(metaIdx)
      assert(metaRow.isNullAt(0), "deleteSequence placeholder should be null")
      assert(metaRow.isNullAt(1), "upsertSequence placeholder should be null")
    }
  }

  // ===========================================================================================
  // AutoCdcMergeFlow reserved-prefix validation tests
  //
  // The two "contract:" tests below lock in the high-level invariant that no reserved-prefix
  // column name can be referenced anywhere -- not in the source change-data feed schema, and
  // not in user-supplied [[ChangeArgs]] (keys or columnSelection). Together they ensure that
  // (a) users cannot opt out of the reserved CDC metadata column by omitting it from the
  // selected schema, and (b) users cannot opt in to (or out of) any other reserved-prefix
  // name we may reserve in the future for an internal CDC concern.
  //
  // The remaining tests pin down case-sensitivity nuances of the source-schema validator.
  // ===========================================================================================

  /** Builds an empty source df with `id` + `seq` + the supplied extra columns. */
  private def sourceDfWithExtraColumns(extraColumns: (String, DataType)*): DataFrame = {
    val schema = extraColumns.foldLeft(
      new StructType().add("id", IntegerType, nullable = false).add("seq", LongType)
    ) { case (acc, (name, dt)) => acc.add(name, dt) }
    spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], schema)
  }

  test(
    "Contract: a source df column with the reserved AutoCDC prefix is rejected at flow " +
    "construction"
  ) {
    val conflictingName = s"${AutoCdcReservedNames.prefix}foo"
    val sourceDf = sourceDfWithExtraColumns(conflictingName -> StringType)

    checkError(
      exception = intercept[AnalysisException] {
        newAutoCdcMergeFlow(sourceDf)
      },
      condition = "AUTOCDC_RESERVED_COLUMN_NAME_PREFIX_CONFLICT",
      sqlState = "42710",
      parameters = Map(
        "caseSensitivity" -> CaseSensitivityLabels.CaseInsensitive,
        "columnName" -> conflictingName,
        "schemaName" -> "changeDataFeed",
        "reservedColumnNamePrefix" -> AutoCdcReservedNames.prefix
      )
    )
  }

  test(
    "Contract: ChangeArgs referencing a reserved-prefix column is rejected even when the " +
    "source df is clean"
  ) {
    // The source df has no reserved-prefix columns, but referencing a reserved-prefix column
    // from any ChangeArgs path still fails at construction with a different error. The
    // reservation is on the name itself, not on its presence in the source feed.
    val cleanSourceDf = threeColumnSourceDf()
    val reservedName = s"${AutoCdcReservedNames.prefix}foo"

    val keysEx = intercept[AnalysisException] {
      newAutoCdcMergeFlow(
        sourceDf = cleanSourceDf,
        keys = Seq(UnqualifiedColumnName(reservedName))
      )
    }
    assert(keysEx.getCondition == "AUTOCDC_KEY_NOT_IN_SELECTED_SCHEMA")

    val includeEx = intercept[AnalysisException] {
      newAutoCdcMergeFlow(
        sourceDf = cleanSourceDf,
        columnSelection = Some(
          ColumnSelection.IncludeColumns(
            Seq(UnqualifiedColumnName("id"), UnqualifiedColumnName(reservedName))
          )
        )
      )
    }
    assert(includeEx.getCondition == "AUTOCDC_COLUMNS_NOT_FOUND_IN_SCHEMA")

    val excludeEx = intercept[AnalysisException] {
      newAutoCdcMergeFlow(
        sourceDf = cleanSourceDf,
        columnSelection = Some(
          ColumnSelection.ExcludeColumns(Seq(UnqualifiedColumnName(reservedName)))
        )
      )
    }
    assert(excludeEx.getCondition == "AUTOCDC_COLUMNS_NOT_FOUND_IN_SCHEMA")
  }

  test(
    "AutoCdcMergeFlow rejects a source df column whose name equals the reserved CDC " +
    "metadata column"
  ) {
    // Locks in the previous engine-level guard (Scd1BatchProcessor.extendMicrobatchRowsWith
    // CdcMetadata) at flow-construction time. Any future regression where a user-supplied
    // CDC stream carries the reserved metadata column name should fail eagerly here.
    val sourceDf = sourceDfWithExtraColumns(Scd1BatchProcessor.cdcMetadataColName -> StringType)

    checkError(
      exception = intercept[AnalysisException] {
        newAutoCdcMergeFlow(sourceDf)
      },
      condition = "AUTOCDC_RESERVED_COLUMN_NAME_PREFIX_CONFLICT",
      sqlState = "42710",
      parameters = Map(
        "caseSensitivity" -> CaseSensitivityLabels.CaseInsensitive,
        "columnName" -> Scd1BatchProcessor.cdcMetadataColName,
        "schemaName" -> "changeDataFeed",
        "reservedColumnNamePrefix" -> AutoCdcReservedNames.prefix
      )
    )
  }

  test(
    "AutoCdcMergeFlow rejects an uppercase reserved-prefix column when caseSensitive=false"
  ) {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val conflictingName =
        s"${AutoCdcReservedNames.prefix}foo".toUpperCase(Locale.ROOT)
      val sourceDf = sourceDfWithExtraColumns(conflictingName -> StringType)

      checkError(
        exception = intercept[AnalysisException] {
          newAutoCdcMergeFlow(sourceDf)
        },
        condition = "AUTOCDC_RESERVED_COLUMN_NAME_PREFIX_CONFLICT",
        sqlState = "42710",
        parameters = Map(
          "caseSensitivity" -> CaseSensitivityLabels.CaseInsensitive,
          "columnName" -> conflictingName,
          "schemaName" -> "changeDataFeed",
          "reservedColumnNamePrefix" -> AutoCdcReservedNames.prefix
        )
      )
    }
  }

  test(
    "AutoCdcMergeFlow allows an uppercase reserved-prefix column when caseSensitive=true"
  ) {
    // Under case-sensitive analysis, the uppercase variant is a distinct identifier and does
    // not collide with the lowercase reserved namespace. Locks in that the validation respects
    // `spark.sql.caseSensitive`, consistent with the schema-augmentation logic in this class.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val nonConflictingName =
        s"${AutoCdcReservedNames.prefix}foo".toUpperCase(Locale.ROOT)
      val sourceDf = sourceDfWithExtraColumns(nonConflictingName -> StringType)

      // No exception expected: construction succeeds.
      newAutoCdcMergeFlow(sourceDf)
    }
  }

  // ===========================================================================================
  // AutoCdcMergeFlow keys-presence validation tests (requireKeysPresentInSelectedSchema)
  // ===========================================================================================

  test("AutoCdcMergeFlow rejects a key that is not present in the source change-data feed") {
    // No columnSelection: the post-selection schema equals the source schema. The key `id`
    // is absent from the source df entirely, so the validator must surface a CDC-specific
    // error rather than deferring to Spark's generic UNRESOLVED_COLUMN.
    val schema = new StructType()
      .add("name", StringType)
      .add("seq", LongType)
    val sourceDf =
      spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], schema)

    checkError(
      exception = intercept[AnalysisException] {
        newAutoCdcMergeFlow(sourceDf)
      },
      condition = "AUTOCDC_KEY_NOT_IN_SELECTED_SCHEMA",
      sqlState = "22023",
      parameters = Map(
        "caseSensitivity" -> CaseSensitivityLabels.CaseInsensitive,
        "keyColumnName" -> "id"
      )
    )
  }

  test("AutoCdcMergeFlow rejects a key dropped by an IncludeColumns selection") {
    checkError(
      exception = intercept[AnalysisException] {
        newAutoCdcMergeFlow(
          sourceDf = threeColumnSourceDf(),
          columnSelection = Some(
            ColumnSelection.IncludeColumns(
              Seq(UnqualifiedColumnName("name"), UnqualifiedColumnName("seq"))
            )
          )
        )
      },
      condition = "AUTOCDC_KEY_NOT_IN_SELECTED_SCHEMA",
      sqlState = "22023",
      parameters = Map(
        "caseSensitivity" -> CaseSensitivityLabels.CaseInsensitive,
        "keyColumnName" -> "id"
      )
    )
  }

  test("AutoCdcMergeFlow rejects a key dropped by an ExcludeColumns selection") {
    checkError(
      exception = intercept[AnalysisException] {
        newAutoCdcMergeFlow(
          sourceDf = threeColumnSourceDf(),
          columnSelection = Some(
            ColumnSelection.ExcludeColumns(Seq(UnqualifiedColumnName("id")))
          )
        )
      },
      condition = "AUTOCDC_KEY_NOT_IN_SELECTED_SCHEMA",
      sqlState = "22023",
      parameters = Map(
        "caseSensitivity" -> CaseSensitivityLabels.CaseInsensitive,
        "keyColumnName" -> "id"
      )
    )
  }

  // ===========================================================================================
  // AutoCdcAuxiliaryTable.{serialize,parse}KeyColumnNames round-trip tests
  // ===========================================================================================

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

  // ===========================================================================================
  // AutoCdcMergeFlow KEY_SCHEMA_DRIFT validation tests
  // ===========================================================================================

  /** DDL fragment for the AutoCDC metadata column appended to every SCD1 auxiliary table. */
  private val cdcMetadataDdl: String = {
    val col = Scd1BatchProcessor.cdcMetadataColName
    val del = Scd1BatchProcessor.cdcDeleteSequenceFieldName
    val ups = Scd1BatchProcessor.cdcUpsertSequenceFieldName
    s"$col STRUCT<$del:BIGINT,$ups:BIGINT> NOT NULL"
  }

  /** Fully-qualified destination identifier under the test V2 catalog/namespace. */
  private def driftTargetIdent(name: String): TableIdentifier =
    TableIdentifier(name, Some(driftTestNamespace), Some(driftTestCatalog))

  /**
   * Pre-create the auxiliary table for a drift test. `auxSchemaDdl` is the full key portion of
   * the aux schema in the order the test wants those columns persisted (e.g. `"id INT NOT
   * NULL"`); the CDC metadata column is appended automatically. `keyColumnNamesProperty`
   * controls the [[AutoCdcAuxiliaryTable.keyColumnNamesProperty]] value:
   *   - `Some(jsonArrayString)` writes the property verbatim. Pass a JSON array of strings
   *     (e.g. `"""["id","region"]"""`) for healthy tables, or any other shape to simulate a
   *     malformed property.
   *   - `None` omits the table property entirely to simulate a corrupt aux table.
   */
  private def createAuxTableForDriftTest(
      targetName: String,
      auxSchemaDdl: String,
      keyColumnNamesProperty: Option[String]): TableIdentifier = {
    val auxIdent = AutoCdcAuxiliaryTable.identifier(driftTargetIdent(targetName))
    val tblPropsClause = keyColumnNamesProperty match {
      case Some(value) =>
        // The raw JSON contains double-quotes; SQL string-literal escape is doubled single
        // quotes. We use a backtick-quoted property key and a single-quoted string value with
        // each embedded `'` escaped to `''` (none today, since the JSON only uses `"`).
        s"TBLPROPERTIES ('${AutoCdcAuxiliaryTable.keyColumnNamesProperty}' = " +
        s"'${value.replace("'", "''")}')"
      case None => ""
    }
    spark.sql(
      s"CREATE TABLE ${auxIdent.unquotedString} ($auxSchemaDdl, $cdcMetadataDdl) $tblPropsClause"
    )
    auxIdent
  }

  /**
   * Construct an [[AutoCdcMergeFlow]] targeting the given destination identifier, with the
   * supplied keys and source df. Mirrors [[newAutoCdcMergeFlow]] but parameterizes destination
   * so drift tests can point at an aux table they pre-created in the V2 test catalog.
   */
  private def newAutoCdcMergeFlowAt(
      destination: TableIdentifier,
      sourceDf: DataFrame,
      keys: Seq[UnqualifiedColumnName],
      sequencing: Column = F.col("version")): AutoCdcMergeFlow = {
    val flow = newAutoCdcFlow(
      identifier = destination,
      destinationIdentifier = destination,
      changeArgs = ChangeArgs(
        keys = keys,
        sequencing = sequencing,
        storedAsScdType = ScdType.Type1
      )
    )
    new AutoCdcMergeFlow(flow, successfulFuncResult(sourceDf))
  }

  /** Build an empty source df with the given top-level fields plus a `version BIGINT` column. */
  private def driftSourceDf(fields: StructField*): DataFrame = {
    val schema = fields.foldLeft(new StructType()) { case (acc, f) => acc.add(f) }
      .add("version", LongType, nullable = false)
    spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], schema)
  }

  test("AutoCdcMergeFlow rejects expanding the key set vs the recorded auxiliary keys") {
    // Aux table was created with a single key column `id`; the flow now declares two keys
    // [region, id] - key arity drift -> KEY_SCHEMA_DRIFT.
    val targetName = "target"
    val auxIdent = createAuxTableForDriftTest(
      targetName = targetName,
      auxSchemaDdl = "id INT NOT NULL",
      keyColumnNamesProperty = Some("""["id"]""")
    )
    val sourceDf = driftSourceDf(
      StructField("region", StringType),
      StructField("id", IntegerType, nullable = false)
    )

    checkError(
      exception = intercept[AnalysisException] {
        newAutoCdcMergeFlowAt(
          destination = driftTargetIdent(targetName),
          sourceDf = sourceDf,
          keys = Seq(UnqualifiedColumnName("region"), UnqualifiedColumnName("id"))
        )
      },
      condition = "AUTOCDC_INVALID_STATE.KEY_SCHEMA_DRIFT",
      sqlState = "42000",
      parameters = Map(
        "flowName" -> driftTargetIdent(targetName).unquotedString,
        "auxTableName" -> auxIdent.unquotedString,
        "expectedKeySchema" -> "region STRING,id INT NOT NULL",
        "recordedKeySchema" -> "id INT NOT NULL"
      )
    )
  }

  test("AutoCdcMergeFlow rejects shrinking the key set vs the recorded auxiliary keys") {
    // Aux table was created with two keys [region, id]; the flow now declares only [id] - key
    // arity drift -> KEY_SCHEMA_DRIFT. Without strict-equality, `id` would silently match at
    // position 0 of the existing aux schema and the dropped `region` key would slip through.
    val targetName = "target"
    val auxIdent = createAuxTableForDriftTest(
      targetName = targetName,
      auxSchemaDdl = "region STRING NOT NULL, id INT NOT NULL",
      keyColumnNamesProperty = Some("""["region","id"]""")
    )
    val sourceDf = driftSourceDf(
      StructField("region", StringType, nullable = false),
      StructField("id", IntegerType, nullable = false)
    )

    checkError(
      exception = intercept[AnalysisException] {
        newAutoCdcMergeFlowAt(
          destination = driftTargetIdent(targetName),
          sourceDf = sourceDf,
          keys = Seq(UnqualifiedColumnName("id"))
        )
      },
      condition = "AUTOCDC_INVALID_STATE.KEY_SCHEMA_DRIFT",
      sqlState = "42000",
      parameters = Map(
        "flowName" -> driftTargetIdent(targetName).unquotedString,
        "auxTableName" -> auxIdent.unquotedString,
        "expectedKeySchema" -> "id INT NOT NULL",
        "recordedKeySchema" -> "region STRING NOT NULL,id INT NOT NULL"
      )
    )
  }

  test("AutoCdcMergeFlow rejects swapping a key column for a different one of the same arity") {
    // Aux table records keys [id, region]; the flow now declares [id, country]. Same arity but
    // the recorded key set {id, region} does not equal the expected key set {id, country} ->
    // KEY_SCHEMA_DRIFT. This exercises the by-name set comparison that an arity-only check
    // would miss.
    val targetName = "target"
    val auxIdent = createAuxTableForDriftTest(
      targetName = targetName,
      auxSchemaDdl = "id INT NOT NULL, region STRING",
      keyColumnNamesProperty = Some("""["id","region"]""")
    )
    val sourceDf = driftSourceDf(
      StructField("id", IntegerType, nullable = false),
      StructField("region", StringType),
      StructField("country", StringType)
    )

    checkError(
      exception = intercept[AnalysisException] {
        newAutoCdcMergeFlowAt(
          destination = driftTargetIdent(targetName),
          sourceDf = sourceDf,
          keys = Seq(UnqualifiedColumnName("id"), UnqualifiedColumnName("country"))
        )
      },
      condition = "AUTOCDC_INVALID_STATE.KEY_SCHEMA_DRIFT",
      sqlState = "42000",
      parameters = Map(
        "flowName" -> driftTargetIdent(targetName).unquotedString,
        "auxTableName" -> auxIdent.unquotedString,
        "expectedKeySchema" -> "id INT NOT NULL,country STRING",
        "recordedKeySchema" -> "id INT NOT NULL,region STRING"
      )
    )
  }

  test("AutoCdcMergeFlow rejects a same-named key whose dataType differs from the recorded key") {
    // Aux table recorded key `id INT NOT NULL`; the flow's source df now exposes `id BIGINT
    // NOT NULL`. Per-position name and arity both line up but dataType comparison fails.
    val targetName = "target"
    val auxIdent = createAuxTableForDriftTest(
      targetName = targetName,
      auxSchemaDdl = "id INT NOT NULL",
      keyColumnNamesProperty = Some("""["id"]""")
    )
    val sourceDf = driftSourceDf(
      StructField("id", LongType, nullable = false)
    )

    checkError(
      exception = intercept[AnalysisException] {
        newAutoCdcMergeFlowAt(
          destination = driftTargetIdent(targetName),
          sourceDf = sourceDf,
          keys = Seq(UnqualifiedColumnName("id"))
        )
      },
      condition = "AUTOCDC_INVALID_STATE.KEY_SCHEMA_DRIFT",
      sqlState = "42000",
      parameters = Map(
        "flowName" -> driftTargetIdent(targetName).unquotedString,
        "auxTableName" -> auxIdent.unquotedString,
        "expectedKeySchema" -> "id BIGINT NOT NULL",
        "recordedKeySchema" -> "id INT NOT NULL"
      )
    )
  }

  test("AutoCdcMergeFlow allows a composite key reorder ([a,b] -> [b,a])") {
    // Aux table recorded keys [a, b] (in that order, in the property); the flow now declares
    // the same key set but in declaration order [b, a]. Drift validation is order-independent:
    // the recorded ordering is purely cosmetic for human-readable error messages and must not
    // gate semantic equivalence, since the merge semantics depend only on the *set* of key
    // columns and their dataTypes. Reordering must therefore NOT throw.
    val targetName = "target"
    createAuxTableForDriftTest(
      targetName = targetName,
      auxSchemaDdl = "a INT NOT NULL, b STRING NOT NULL",
      keyColumnNamesProperty = Some("""["a","b"]""")
    )
    val sourceDf = driftSourceDf(
      StructField("a", IntegerType, nullable = false),
      StructField("b", StringType, nullable = false)
    )

    newAutoCdcMergeFlowAt(
      destination = driftTargetIdent(targetName),
      sourceDf = sourceDf,
      keys = Seq(UnqualifiedColumnName("b"), UnqualifiedColumnName("a"))
    )
  }

  test("AutoCdcMergeFlow surfaces AUXILIARY_TABLE_PROPERTY_MISSING when the auxiliary table is " +
    "missing the keyColumnNames property") {
    // Pre-create an aux table without the `keyColumnNames` property to simulate corrupt
    // metadata (e.g. user ran `ALTER TABLE ... UNSET TBLPROPERTIES`). The flow constructor must
    // surface a structured AUTOCDC_INVALID_STATE error rather than silently mis-validating keys.
    val targetName = "target"
    val auxIdent = createAuxTableForDriftTest(
      targetName = targetName,
      auxSchemaDdl = "id INT NOT NULL",
      keyColumnNamesProperty = None
    )
    val sourceDf = driftSourceDf(StructField("id", IntegerType, nullable = false))

    checkError(
      exception = intercept[org.apache.spark.SparkException] {
        newAutoCdcMergeFlowAt(
          destination = driftTargetIdent(targetName),
          sourceDf = sourceDf,
          keys = Seq(UnqualifiedColumnName("id"))
        )
      },
      condition = "AUTOCDC_INVALID_STATE.AUXILIARY_TABLE_PROPERTY_MISSING",
      sqlState = "42000",
      parameters = Map(
        "flowName" -> driftTargetIdent(targetName).unquotedString,
        "auxTableName" -> auxIdent.unquotedString,
        "propertyName" -> AutoCdcAuxiliaryTable.keyColumnNamesProperty
      )
    )
  }

  test("AutoCdcMergeFlow surfaces AUXILIARY_TABLE_PROPERTY_MALFORMED when the auxiliary table's " +
    "keyColumnNames property is malformed") {
    // Non-JSON-array property value simulates corrupt aux metadata. The flow constructor must
    // surface a structured AUTOCDC_INVALID_STATE error rather than letting a parse exception leak.
    val targetName = "target"
    val malformedValue = "not-a-json-array"
    val auxIdent = createAuxTableForDriftTest(
      targetName = targetName,
      auxSchemaDdl = "id INT NOT NULL",
      keyColumnNamesProperty = Some(malformedValue)
    )
    val sourceDf = driftSourceDf(StructField("id", IntegerType, nullable = false))

    checkError(
      exception = intercept[org.apache.spark.SparkException] {
        newAutoCdcMergeFlowAt(
          destination = driftTargetIdent(targetName),
          sourceDf = sourceDf,
          keys = Seq(UnqualifiedColumnName("id"))
        )
      },
      condition = "AUTOCDC_INVALID_STATE.AUXILIARY_TABLE_PROPERTY_MALFORMED",
      sqlState = "42000",
      parameters = Map(
        "flowName" -> driftTargetIdent(targetName).unquotedString,
        "auxTableName" -> auxIdent.unquotedString,
        "propertyName" -> AutoCdcAuxiliaryTable.keyColumnNamesProperty,
        "rawValue" -> malformedValue
      )
    )
  }

  test("AutoCdcMergeFlow surfaces AUXILIARY_TABLE_KEY_COLUMN_MISSING when a recorded key is " +
    "absent from the aux table schema") {
    // Pre-create an aux table whose recorded keyColumnNames property names a column the schema
    // does not contain. This is either a write-path implementation bug or external user
    // tampering (e.g. dropping the key column); both surface as a structured AUTOCDC_INVALID_STATE
    // error rather than KEY_SCHEMA_DRIFT, because the drift validator never gets a chance to run.
    val targetName = "target"
    val auxIdent = createAuxTableForDriftTest(
      targetName = targetName,
      auxSchemaDdl = "id INT NOT NULL",
      keyColumnNamesProperty = Some("""["region"]""")
    )
    val sourceDf = driftSourceDf(StructField("region", StringType, nullable = false))

    checkError(
      exception = intercept[org.apache.spark.SparkException] {
        newAutoCdcMergeFlowAt(
          destination = driftTargetIdent(targetName),
          sourceDf = sourceDf,
          keys = Seq(UnqualifiedColumnName("region"))
        )
      },
      condition = "AUTOCDC_INVALID_STATE.AUXILIARY_TABLE_KEY_COLUMN_MISSING",
      sqlState = "42000",
      parameters = Map(
        "flowName" -> driftTargetIdent(targetName).unquotedString,
        "auxTableName" -> auxIdent.unquotedString,
        "keyColumnName" -> "region",
        "propertyName" -> AutoCdcAuxiliaryTable.keyColumnNamesProperty
      )
    )
  }

  test("AutoCdcMergeFlow validates drift by name lookup, not by aux schema position") {
    // Future-proofing: the aux table layout is allowed to interleave non-key columns or place
    // keys after auxiliary-only columns (e.g. a hypothetical SCD2 aux schema with current/
    // history flags up front). Today's SCD1 layout is keys-first, but the validator must still
    // locate recorded keys *by name*, not by position. This test pre-creates an aux table whose
    // schema places the CDC metadata column ahead of the key column and confirms drift
    // validation still passes when the user-declared key matches the recorded key by name.
    val targetName = "target"
    val auxIdent = AutoCdcAuxiliaryTable.identifier(driftTargetIdent(targetName))
    spark.sql(
      s"CREATE TABLE ${auxIdent.unquotedString} ($cdcMetadataDdl, id INT NOT NULL) " +
      s"TBLPROPERTIES ('${AutoCdcAuxiliaryTable.keyColumnNamesProperty}' = '[\"id\"]')"
    )
    val sourceDf = driftSourceDf(StructField("id", IntegerType, nullable = false))

    // Should NOT throw -- the recorded key name `id` is found in the aux schema regardless of
    // its position, and its dataType matches the flow's expected key dataType.
    newAutoCdcMergeFlowAt(
      destination = driftTargetIdent(targetName),
      sourceDf = sourceDf,
      keys = Seq(UnqualifiedColumnName("id"))
    )
  }
}
