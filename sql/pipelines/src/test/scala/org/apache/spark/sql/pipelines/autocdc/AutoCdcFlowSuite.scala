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

import scala.util.Success

import org.apache.spark.sql.{functions => F, AnalysisException, Column, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.pipelines.graph.{
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
class AutoCdcFlowSuite extends QueryTest with SharedSparkSession {

  private val testIdentifier = TableIdentifier("cdc_target", Some("db"))

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
      origin: QueryOrigin = QueryOrigin.empty,
      changeArgs: ChangeArgs = testChangeArgs): AutoCdcFlow = {
    AutoCdcFlow(
      identifier = identifier,
      destinationIdentifier = destinationIdentifier,
      func = func,
      queryContext = queryContext,
      sqlConf = sqlConf,
      origin = origin,
      changeArgs = changeArgs
    )
  }

  test("AutoCdcFlow exposes its constructor fields") {
    val flow = newAutoCdcFlow(
      sqlConf = Map("spark.sql.shuffle.partitions" -> "8")
    )

    assert(flow.identifier == testIdentifier)
    assert(flow.destinationIdentifier == testIdentifier)
    assert(flow.func eq noOpFlowFunction)
    assert(flow.queryContext == testQueryContext)
    assert(flow.sqlConf == Map("spark.sql.shuffle.partitions" -> "8"))
    assert(flow.origin == QueryOrigin.empty)
    assert(flow.changeArgs == testChangeArgs)
  }

  test("AutoCdcFlow defaults sqlConf to empty") {
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

  /** Builds an [[AutoCdcMergeFlow]] over the given source dataframe + change args. */
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

  /** A stable 3-column source streaming dataframe used across most schema tests. */
  private def threeColumnSourceDf(): DataFrame = {
    val session = spark
    import session.implicits._
    MemoryStream[(Int, String, Option[Long])].toDS().toDF("id", "name", "seq")
  }

  /** Convenience to extract the [[StructType]] of the projected `_cdc_metadata` column. */
  private def cdcMetadataStruct(schema: StructType): StructType =
    schema(AutoCdcReservedNames.cdcMetadataColName).dataType.asInstanceOf[StructType]

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
          AutoCdcReservedNames.cdcMetadataColName,
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
          AutoCdcReservedNames.cdcMetadataColName,
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
          AutoCdcReservedNames.cdcMetadataColName,
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

    val metaField = resolvedFlow.schema(AutoCdcReservedNames.cdcMetadataColName)
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
    val loadedDf = resolvedFlow.load(asStreaming = true)
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
    val loadedDf = resolvedFlow.load(asStreaming = true)
    assert(loadedDf.schema == resolvedFlow.schema)
    // The user-selected portion drops `name`; the trailing column is the SCD1 metadata.
    assert(
      loadedDf.schema.fieldNames.toSeq ==
      Seq("id", "seq", AutoCdcReservedNames.cdcMetadataColName)
    )
  }

  test("AutoCdcMergeFlow.load() respects an ExcludeColumns selection") {
    val resolvedFlow = newAutoCdcMergeFlow(
      sourceDf = threeColumnSourceDf(),
      columnSelection = Some(
        ColumnSelection.ExcludeColumns(Seq(UnqualifiedColumnName("name")))
      )
    )
    val loadedDf = resolvedFlow.load(asStreaming = true)
    assert(loadedDf.schema == resolvedFlow.schema)
    assert(
      loadedDf.schema.fieldNames.toSeq ==
      Seq("id", "seq", AutoCdcReservedNames.cdcMetadataColName)
    )
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
    val session = spark
    import session.implicits._
    val baseStream = MemoryStream[(Int, Option[Long])].toDS().toDF("id", "seq")
    extraColumns.foldLeft(baseStream) { case (acc, (name, dt)) =>
      acc.withColumn(name, F.lit(null).cast(dt))
    }
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
    // Locks in the previous engine-level guard at flow-construction time. Any future
    // regression where a user-supplied CDC stream carries the reserved metadata column name
    // should fail eagerly here.
    val sourceDf = sourceDfWithExtraColumns(AutoCdcReservedNames.cdcMetadataColName -> StringType)

    checkError(
      exception = intercept[AnalysisException] {
        newAutoCdcMergeFlow(sourceDf)
      },
      condition = "AUTOCDC_RESERVED_COLUMN_NAME_PREFIX_CONFLICT",
      sqlState = "42710",
      parameters = Map(
        "caseSensitivity" -> CaseSensitivityLabels.CaseInsensitive,
        "columnName" -> AutoCdcReservedNames.cdcMetadataColName,
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
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

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
}
