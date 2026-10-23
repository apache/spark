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

package org.apache.spark.sql

import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.collection.mutable

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.analysis.{TableOutputResolver, UnresolvedIdentifier,
  UnresolvedInsertTarget}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{CreateFlowCommand, LogicalPlan, UnresolvedInsert}
import org.apache.spark.sql.catalyst.util.QuotingUtils.quoteIdentifier
import org.apache.spark.sql.connector.catalog.{V1Table, V2TableWithV1Fallback}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException, Trigger}
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.util.Utils

// scalastyle:off line.size.limit
/**
 * SQL golden tests for Structured Streaming flows.
 *
 * CREATE FLOW registers a named flow definition without running it. REFRESH FLOW runs the
 * remembered definition with Trigger.AvailableNow. The flow must use INSERT INTO <table> BY NAME,
 * and the target table must already exist. Runs of the same named flow reuse its checkpoint. The
 * target table must be written only by the flow because batch writes are not tracked by the
 * streaming sink's metadata log, and each target table may be written by only one flow. Dropping
 * and recreating the target table while reusing a flow is unsupported and should not be tested.
 * REFRESH FLOW is test-harness syntax; it is not part of the Spark SQL parser.
 *
 * To run the suite:
 * {{{
 *   build/sbt "sql/testOnly org.apache.spark.sql.StreamingSQLQueryTestSuite"
 * }}}
 *
 * To run one input file:
 * {{{
 *   build/sbt "sql/testOnly org.apache.spark.sql.StreamingSQLQueryTestSuite -- -z available-now-to-table.sql"
 * }}}
 *
 * For example:
 * {{{
 *   CREATE TABLE source (id INT, value STRING) USING parquet;
 *   INSERT INTO source VALUES (1, 'a'), (2, 'b');
 *   CREATE TABLE sink (id INT, value STRING) USING parquet;
 *   CREATE FLOW copy_source AS
 *   INSERT INTO sink BY NAME
 *   SELECT * FROM STREAM(source);
 *   REFRESH FLOW copy_source;
 *   SELECT * FROM sink ORDER BY id;
 * }}}
 */
// scalastyle:on line.size.limit
@ExtendedSQLTest
class StreamingSQLQueryTestSuite extends SQLQueryTestSuite {

  override protected def ignoreList: Set[String] = super.ignoreList - "streaming/"

  override protected lazy val listTestCases: Seq[TestCase] =
    createTestCases().filter(_.name.startsWith("streaming/"))

  private def triggeredFlowQueryName(
      testCase: TestCase,
      sparkConfigSet: Seq[(String, String)],
      flowName: Seq[String]): String = {
    val identity =
      Seq(testCase.name, sparkConfigSet.mkString(","), formatFlowName(flowName)).mkString("\u0000")
    "sql-query-test-" + UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8))
  }

  private val triggeredFlowTimeoutMs = TimeUnit.MINUTES.toMillis(10)

  private case class ResolvedTableName(
      catalog: String,
      namespace: Seq[String],
      table: String) {
    def sql: String = (catalog +: namespace :+ table).map(quoteIdentifier).mkString(".")
  }

  private case class TriggeredFlow(
      flowName: Seq[String],
      query: LogicalPlan,
      targetTable: String,
      resolvedTargetTable: ResolvedTableName,
      checkpointLocation: String)

  private sealed trait TriggeredFlowStatement
  private case class CreateTriggeredFlow(command: CreateFlowCommand)
    extends TriggeredFlowStatement
  private case class RefreshTriggeredFlow(flowName: Seq[String])
    extends TriggeredFlowStatement

  private def formatFlowName(flowName: Seq[String]): String =
    flowName.map(quoteIdentifier).mkString(".")

  private def sameFlowName(
      session: SparkSession,
      left: Seq[String],
      right: Seq[String]): Boolean = {
    left.length == right.length && left.zip(right).forall { case (l, r) =>
      session.sessionState.conf.resolver(l, r)
    }
  }

  private def loadV1Table(session: SparkSession, tableName: String): Option[CatalogTable] = {
    val analyzer = session.sessionState.analyzer
    import analyzer.CatalogAndIdentifier

    val multipartIdentifier = session.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    val CatalogAndIdentifier(catalog, identifier) = multipartIdentifier
    catalog.asTableCatalog.loadTable(identifier) match {
      case table: V1Table => Some(table.v1Table)
      case table: V2TableWithV1Fallback => Some(table.v1Table)
      case _ => None
    }
  }

  private def resolveTableName(
      session: SparkSession,
      multipartIdentifier: Seq[String]): ResolvedTableName = {
    val analyzer = session.sessionState.analyzer
    import analyzer.CatalogAndIdentifier

    val CatalogAndIdentifier(catalog, identifier) = multipartIdentifier
    ResolvedTableName(catalog.name(), identifier.namespace().toSeq, identifier.name())
  }

  private def sameTableName(
      session: SparkSession,
      left: ResolvedTableName,
      right: ResolvedTableName): Boolean = {
    val resolver = session.sessionState.conf.resolver
    resolver(left.catalog, right.catalog) &&
      left.namespace.length == right.namespace.length &&
      left.namespace.zip(right.namespace).forall { case (l, r) => resolver(l, r) } &&
      resolver(left.table, right.table)
  }

  private def parseCreateTriggeredFlow(
      session: SparkSession,
      command: CreateFlowCommand): TriggeredFlow = {
    val flowName = command.name match {
      case identifier: UnresolvedIdentifier => identifier.nameParts
      case _ =>
        throw new IllegalArgumentException(
          "SQL golden triggered flows require a static flow name.")
    }
    val insert = command.flowOperation match {
      case insert: UnresolvedInsert => insert
      case _ =>
        throw new IllegalArgumentException(
          "SQL golden triggered flows require CREATE FLOW AS " +
            "INSERT INTO ... BY NAME SELECT.")
    }
    require(insert.byName && !insert.overwrite && insert.partitionSpec.isEmpty &&
        insert.userSpecifiedCols.isEmpty && insert.replaceCriteriaOpt.isEmpty &&
        !insert.withSchemaEvolution,
      "SQL golden triggered flows require CREATE FLOW AS INSERT INTO ... BY NAME SELECT.")

    val (targetTable, resolvedTargetTable) = insert.table match {
      case target: UnresolvedInsertTarget =>
        val targetTable = target.multipartIdentifier.map(quoteIdentifier).mkString(".")
        targetTable -> resolveTableName(session, target.multipartIdentifier)
      case _ =>
        throw new IllegalArgumentException(
          "SQL golden triggered flows require a static table identifier as the INSERT target.")
    }
    val checkpointLocation = new Path(
      session.sessionState.conf.warehousePath,
      s"sql-query-test-checkpoint-${UUID.randomUUID()}").toString
    TriggeredFlow(
      flowName,
      insert.query,
      targetTable,
      resolvedTargetTable,
      checkpointLocation)
  }

  private def registerTriggeredFlow(
      session: SparkSession,
      command: CreateFlowCommand,
      flows: mutable.Map[Seq[String], TriggeredFlow]): TriggeredFlow = {
    session.withActive {
      val flow = parseCreateTriggeredFlow(session, command)
      require(!flows.keys.exists(sameFlowName(session, _, flow.flowName)),
        s"Triggered flow ${formatFlowName(flow.flowName)} is already defined in this test run.")
      flows.values.find { registeredFlow =>
        sameTableName(session, registeredFlow.resolvedTargetTable, flow.resolvedTargetTable)
      }.foreach { registeredFlow =>
        throw new IllegalArgumentException(
          s"SQL golden triggered flow target table '${flow.targetTable}' is already written by " +
            s"flow ${formatFlowName(registeredFlow.flowName)}.")
      }
      flows += flow.flowName -> flow
      flow
    }
  }

  private def resolveTriggeredFlow(
      session: SparkSession,
      flowName: Seq[String],
      flows: mutable.Map[Seq[String], TriggeredFlow]): TriggeredFlow = {
    flows.collectFirst {
      case (registeredName, flow) if sameFlowName(session, registeredName, flowName) => flow
    }.getOrElse {
      throw new IllegalArgumentException(
        s"Triggered flow ${formatFlowName(flowName)} has not been defined in this test run.")
    }
  }

  private def parseRefreshTriggeredFlowStatement(
      session: SparkSession,
      name: String): RefreshTriggeredFlow = {
    try {
      RefreshTriggeredFlow(session.sessionState.sqlParser.parseMultipartIdentifier(name))
    } catch {
      case e: ParseException =>
        throw new IllegalArgumentException(
          s"REFRESH FLOW requires a valid flow name, but '$name' is invalid.", e)
    }
  }

  private def parseTriggeredFlowStatement(
      session: SparkSession,
      sql: String,
      parsedPlan: => Option[LogicalPlan]): Option[TriggeredFlowStatement] = {
    session.withActive {
      val tokens = sql.trim.split("\\s+", 3)
      if (tokens.length >= 2 && tokens(1).equalsIgnoreCase("FLOW")) {
        if (tokens(0).equalsIgnoreCase("REFRESH")) {
          if (tokens.length == 2) {
            throw new IllegalArgumentException("REFRESH FLOW requires a flow name.")
          }
          Some(parseRefreshTriggeredFlowStatement(session, tokens(2)))
        } else if (tokens(0).equalsIgnoreCase("CREATE")) {
          parsedPlan.collect {
            case command: CreateFlowCommand => CreateTriggeredFlow(command)
          }
        } else {
          None
        }
      } else {
        None
      }
    }
  }

  private def parseTriggeredFlowStatement(
      session: SparkSession,
      sql: String): Option[TriggeredFlowStatement] = {
    parseTriggeredFlowStatement(session, sql, parsePlan(session, sql))
  }

  /** Runs one REFRESH FLOW statement to completion. */
  private def runTriggeredFlow(
      session: SparkSession,
      flowName: Seq[String],
      testCase: TestCase,
      sparkConfigSet: Seq[(String, String)],
      flows: mutable.Map[Seq[String], TriggeredFlow]): (String, Seq[String]) = {
    session.withActive {
      val flow = resolveTriggeredFlow(session, flowName, flows)
      val targetTableName = flow.resolvedTargetTable.sql
      val df = classic.Dataset.ofRows(
        session.asInstanceOf[classic.SparkSession], flow.query.clone())
      require(df.isStreaming,
        s"SQL golden triggered flow ${formatFlowName(flow.flowName)} requires a streaming query, " +
          "but its CREATE FLOW query reads no streaming source.")
      require(session.catalog.tableExists(targetTableName),
        s"SQL golden triggered flow target table '${flow.targetTable}' does not exist.")
      val queryName = triggeredFlowQueryName(testCase, sparkConfigSet, flow.flowName)
      val targetTable = loadV1Table(session, targetTableName)
      val alignedQuery = TableOutputResolver.resolveOutputColumns(
        targetTableName,
        session.table(targetTableName).queryExecution.analyzed.output,
        df.queryExecution.analyzed,
        byName = true,
        session.sessionState.conf,
        TableOutputResolver.DefaultValueFillMode.FILL)
      val alignedDf = classic.Dataset.ofRows(
        session.asInstanceOf[classic.SparkSession], alignedQuery)

      val writer = alignedDf.writeStream
        .outputMode(OutputMode.Append())
        .trigger(Trigger.AvailableNow())
        .queryName(queryName)
        .option("checkpointLocation", flow.checkpointLocation)
      targetTable.foreach { table =>
        writer.format(table.provider.getOrElse {
          throw new IllegalArgumentException(
            s"SQL golden triggered flow target table '${flow.targetTable}' has no provider.")
        })
        writer.options(table.storage.properties)
        if (table.partitionColumnNames.nonEmpty) {
          writer.partitionBy(table.partitionColumnNames: _*)
        }
      }
      val query = writer.toTable(targetTableName)
      Utils.tryWithSafeFinally {
        try {
          if (!query.awaitTermination(triggeredFlowTimeoutMs)) {
            throw new IllegalStateException(
              s"SQL golden triggered flow ${formatFlowName(flow.flowName)} did not terminate " +
                s"within ${TimeUnit.MILLISECONDS.toMinutes(triggeredFlowTimeoutMs)} minutes.")
          }
        } catch {
          case e: StreamingQueryException if e.cause != null => throw e.cause
        }
      } {
        query.stop()
      }

      (emptySchema, Seq.empty)
    }
  }

  override protected def createCustomQueryExecutor(
      session: SparkSession,
      testCase: TestCase,
      sparkConfigSet: Seq[(String, String)],
      analyzerOnly: Boolean):
      (String, () => Option[LogicalPlan]) => Option[(String, Seq[String])] = {
    val flows = mutable.Map.empty[Seq[String], TriggeredFlow]
    (sql, getParsedPlan) => parseTriggeredFlowStatement(session, sql, getParsedPlan()).map {
      case CreateTriggeredFlow(command) =>
        val flow = registerTriggeredFlow(session, command, flows)
        if (analyzerOnly) {
          val df = classic.Dataset.ofRows(
            session.asInstanceOf[classic.SparkSession], flow.query)
          getNormalizedQueryAnalysisResult(df)
        } else {
          (emptySchema, Seq.empty)
        }
      case RefreshTriggeredFlow(flowName) =>
        if (analyzerOnly) {
          resolveTriggeredFlow(session, flowName, flows)
          (emptySchema, Seq.empty)
        } else {
          runTriggeredFlow(session, flowName, testCase, sparkConfigSet, flows)
        }
    }
  }

  test("test triggered flow parsing") {
    def querySpecs(input: String): Seq[(String, Boolean)] = {
      val (comments, code) = splitCommentsAndCodes(input)
      getQueriesWithDebugFlag(code, comments, Nil)
    }

    def parseTriggeredFlow(sql: String): Option[TriggeredFlowStatement] = {
      parseTriggeredFlowStatement(spark, sql)
    }

    def createFlowCommand(sql: String): CreateFlowCommand = {
      parseTriggeredFlow(sql) match {
        case Some(CreateTriggeredFlow(command)) => command
        case statement => fail(s"Expected CREATE FLOW, found $statement")
      }
    }

    def refreshFlowName(sql: String): Seq[String] = {
      parseTriggeredFlow(sql) match {
        case Some(RefreshTriggeredFlow(flowName)) => flowName
        case statement => fail(s"Expected REFRESH FLOW, found $statement")
      }
    }

    val specs = querySpecs(
      """-- ordinary comment
        |SELECT 1;
        |CREATE FLOW test_flow AS
        |INSERT INTO `spark_catalog`.`default`.`sink-table` BY NAME
        |SELECT * FROM STREAM(source);
        |REFRESH FLOW test_flow;
        |SELECT 2;
        |""".stripMargin)
    assert(specs.map(_._1) == Seq(
      "SELECT 1",
      """CREATE FLOW test_flow AS
        |INSERT INTO `spark_catalog`.`default`.`sink-table` BY NAME
        |SELECT * FROM STREAM(source)""".stripMargin,
      "REFRESH FLOW test_flow",
      "SELECT 2"))

    val flows = mutable.Map.empty[Seq[String], TriggeredFlow]
    registerTriggeredFlow(spark, createFlowCommand(specs(1)._1), flows)
    val flowName = refreshFlowName(specs(2)._1)
    val flow = resolveTriggeredFlow(spark, flowName, flows)
    assert(flow.targetTable == "`spark_catalog`.`default`.`sink-table`")
    assert(resolveTriggeredFlow(spark, flowName, flows) eq flow)
    val separatelyRegisteredFlow = registerTriggeredFlow(
      spark,
      createFlowCommand(specs(1)._1),
      mutable.Map.empty)
    assert(separatelyRegisteredFlow.checkpointLocation != flow.checkpointLocation)
    assert(parseTriggeredFlow(specs.head._1).isEmpty)
    assert(parseTriggeredFlow("select interval 10 nanoseconds").isEmpty)
    assert(parseTriggeredFlow(specs(1)._1)
      .exists(_.isInstanceOf[CreateTriggeredFlow]))
    assert(parseTriggeredFlow(specs(2)._1)
      .exists(_.isInstanceOf[RefreshTriggeredFlow]))
    val refreshWithoutParsedPlan = parseTriggeredFlowStatement(
      spark,
      "REFRESH FLOW test_flow",
      fail("REFRESH FLOW should not evaluate the Spark SQL parsed plan"))
    assert(refreshWithoutParsedPlan.contains(RefreshTriggeredFlow(Seq("test_flow"))))
    assert(refreshFlowName("refresh flow `catalog`.`flow-name`") == Seq("catalog", "flow-name"))
    val missingRefreshNameError = intercept[IllegalArgumentException] {
      parseTriggeredFlow("REFRESH FLOW")
    }
    assert(missingRefreshNameError.getMessage.contains("requires a flow name"))
    Seq("my-flow", "a b").foreach { invalidName =>
      val invalidRefreshNameError = intercept[IllegalArgumentException] {
        parseTriggeredFlow(s"REFRESH FLOW $invalidName")
      }
      assert(invalidRefreshNameError.getMessage.contains(s"'$invalidName' is invalid"))
    }

    val debugSpec = querySpecs(
      """--DEBUG
        |CREATE FLOW debug_flow AS
        |INSERT INTO sink BY NAME SELECT * FROM STREAM(source);
        |""".stripMargin).head
    assert(debugSpec._2)

    val nonByNameError = intercept[IllegalArgumentException] {
      registerTriggeredFlow(
        spark,
        createFlowCommand("CREATE FLOW f AS INSERT INTO sink SELECT * FROM STREAM(source)"),
        mutable.Map.empty)
    }
    assert(nonByNameError.getMessage.contains("INSERT INTO ... BY NAME SELECT"))

    val undefinedFlowError = intercept[IllegalArgumentException] {
      resolveTriggeredFlow(spark, Seq("missing_flow"), mutable.Map.empty)
    }
    assert(undefinedFlowError.getMessage.contains("has not been defined in this test run"))

    val batchFlows = mutable.Map.empty[Seq[String], TriggeredFlow]
    registerTriggeredFlow(
      spark,
      createFlowCommand(
        "CREATE FLOW batch_flow AS INSERT INTO sink BY NAME SELECT 1 AS id"),
      batchFlows)
    val batchFlowError = intercept[IllegalArgumentException] {
      runTriggeredFlow(
        spark,
        Seq("batch_flow"),
        RegularTestCase("batch-flow-error", "", ""),
        Nil,
        batchFlows)
    }
    assert(batchFlowError.getMessage.contains("triggered flow `batch_flow`"))
    assert(!batchFlowError.getMessage.contains("REFRESH FLOW"))

    val sharedTargetFlows = mutable.Map.empty[Seq[String], TriggeredFlow]
    registerTriggeredFlow(
      spark,
      createFlowCommand(
        "CREATE FLOW first_flow AS INSERT INTO shared_sink BY NAME " +
          "SELECT * FROM STREAM(first_source)"),
      sharedTargetFlows)
    Seq("shared_sink", "default.shared_sink", "spark_catalog.default.shared_sink")
      .zipWithIndex.foreach { case (targetTable, index) =>
        val sharedTargetError = intercept[IllegalArgumentException] {
          registerTriggeredFlow(
            spark,
            createFlowCommand(
              s"CREATE FLOW second_flow_$index AS INSERT INTO $targetTable BY NAME " +
                "SELECT * FROM STREAM(second_source)"),
            sharedTargetFlows)
        }
        assert(sharedTargetError.getMessage.contains("already written by flow `first_flow`"))
      }

    withTable("by_name_source", "by_name_sink") {
      spark.sql("CREATE TABLE by_name_source (second INT, first INT) USING parquet")
      spark.sql("INSERT INTO by_name_source VALUES (2, 1)")
      spark.sql("CREATE TABLE by_name_sink (first INT, second INT) USING parquet")
      val byNameFlows = mutable.Map.empty[Seq[String], TriggeredFlow]
      registerTriggeredFlow(
        spark,
        createFlowCommand(
          """CREATE FLOW by_name_flow AS
            |INSERT INTO by_name_sink BY NAME
            |SELECT second, first FROM STREAM(by_name_source)
            |""".stripMargin),
        byNameFlows)
      runTriggeredFlow(
        spark,
        Seq("by_name_flow"),
        RegularTestCase("by-name", "", ""),
        Nil,
        byNameFlows)
      checkAnswer(spark.table("by_name_sink"), Row(1, 2))
    }

    withTable("incompatible_source", "incompatible_sink") {
      spark.sql("CREATE TABLE incompatible_source (id INT) USING parquet")
      spark.sql("CREATE TABLE incompatible_sink (other INT) USING parquet")
      val incompatibleFlows = mutable.Map.empty[Seq[String], TriggeredFlow]
      registerTriggeredFlow(
        spark,
        createFlowCommand(
          """CREATE FLOW incompatible_flow AS
            |INSERT INTO incompatible_sink BY NAME
            |SELECT id FROM STREAM(incompatible_source)
            |""".stripMargin),
        incompatibleFlows)
      checkError(
        exception = intercept[AnalysisException] {
          runTriggeredFlow(
            spark,
            Seq("incompatible_flow"),
            RegularTestCase("incompatible", "", ""),
            Nil,
            incompatibleFlows)
        },
        condition = "INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS",
        parameters = Map(
          "tableName" -> "`spark_catalog`.`default`.`incompatible_sink`",
          "extraColumns" -> "`id`"))
    }

    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      withTable("runtime_error_source", "runtime_error_sink") {
        spark.sql("CREATE TABLE runtime_error_source (value STRING) USING parquet")
        spark.sql("INSERT INTO runtime_error_source VALUES ('not_an_int')")
        spark.sql("CREATE TABLE runtime_error_sink (value INT) USING parquet")
        val runtimeErrorFlows = mutable.Map.empty[Seq[String], TriggeredFlow]
        registerTriggeredFlow(
          spark,
          createFlowCommand(
            """CREATE FLOW runtime_error_flow AS
              |INSERT INTO runtime_error_sink BY NAME
              |SELECT CAST(value AS INT) AS value FROM STREAM(runtime_error_source)
              |""".stripMargin),
          runtimeErrorFlows)
        val (_, runtimeErrorOutput) = handleExceptions {
          runTriggeredFlow(
            spark,
            Seq("runtime_error_flow"),
            RegularTestCase("runtime-error", "", ""),
            Nil,
            runtimeErrorFlows)
        }
        assert(runtimeErrorOutput.exists(_.contains("CAST_INVALID_INPUT")))
        assert(!runtimeErrorOutput.exists(_.contains("STREAM_FAILED")))
        assert(!runtimeErrorOutput.exists(_.contains("runId")))
      }
    }
  }

  test("test triggered flow execution semantics") {
    def createFlowCommand(session: SparkSession, sql: String): CreateFlowCommand = {
      parseTriggeredFlowStatement(session, sql) match {
        case Some(CreateTriggeredFlow(command)) => command
        case statement => fail(s"Expected CREATE FLOW, found $statement")
      }
    }

    withTable("provider_source", "provider_sink") {
      spark.sql("CREATE TABLE provider_source (id INT, part STRING) USING parquet")
      spark.sql("INSERT INTO provider_source VALUES (1, 'a'), (2, 'b')")
      spark.sql(
        "CREATE TABLE provider_sink (id INT, part STRING) USING JSON PARTITIONED BY (part)")
      val flows = mutable.Map.empty[Seq[String], TriggeredFlow]
      registerTriggeredFlow(
        spark,
        createFlowCommand(
          spark,
          "CREATE FLOW provider_flow AS INSERT INTO provider_sink BY NAME " +
            "SELECT id, part FROM STREAM(provider_source)"),
        flows)
      runTriggeredFlow(
        spark,
        Seq("provider_flow"),
        RegularTestCase("provider", "", ""),
        Nil,
        flows)
      checkAnswer(spark.table("provider_sink"), Seq(Row(1, "a"), Row(2, "b")))
      spark.table("provider_sink")
        .selectExpr("part", "input_file_name() AS file")
        .collect()
        .foreach { row =>
          val part = row.getString(0)
          val file = row.getString(1)
          assert(file.contains(s"/part=$part/"),
            s"Expected row for partition '$part' to be stored in its partition directory: $file")
        }
    }

    withTable("options_source", "options_sink") {
      spark.sql("CREATE TABLE options_source (id STRING, value STRING) USING parquet")
      spark.sql("INSERT INTO options_source VALUES ('1', 'a'), ('2', 'b')")
      spark.sql(
        "CREATE TABLE options_sink (id STRING, value STRING) USING csv OPTIONS(delimiter = '|')")
      val flows = mutable.Map.empty[Seq[String], TriggeredFlow]
      registerTriggeredFlow(
        spark,
        createFlowCommand(
          spark,
          "CREATE FLOW options_flow AS INSERT INTO options_sink BY NAME " +
            "SELECT id, value FROM STREAM(options_source)"),
        flows)
      runTriggeredFlow(
        spark,
        Seq("options_flow"),
        RegularTestCase("options", "", ""),
        Nil,
        flows)
      checkAnswer(spark.table("options_sink"), Seq(Row("1", "a"), Row("2", "b")))
    }

    withTable("qualified_source") {
      spark.sql("CREATE TABLE qualified_source (id INT) USING parquet")
      spark.sql("INSERT INTO qualified_source VALUES (1)")
      val (firstDb, secondDb) = ("triggered_flow_db1", "triggered_flow_db2")
      withDatabase(firstDb, secondDb) {
        spark.sql(s"CREATE DATABASE $firstDb")
        spark.sql(s"CREATE DATABASE $secondDb")
        spark.sql(s"CREATE TABLE $firstDb.sink (id INT) USING parquet")
        spark.sql(s"CREATE TABLE $secondDb.sink (id INT) USING parquet")
        val flows = mutable.Map.empty[Seq[String], TriggeredFlow]

        spark.sql(s"USE $firstDb")
        registerTriggeredFlow(
          spark,
          createFlowCommand(
            spark,
            "CREATE FLOW first_qualified_flow AS INSERT INTO sink BY NAME " +
              "SELECT id FROM STREAM(spark_catalog.default.qualified_source)"),
          flows)

        spark.sql(s"USE $secondDb")
        registerTriggeredFlow(
          spark,
          createFlowCommand(
            spark,
            "CREATE FLOW second_qualified_flow AS INSERT INTO sink BY NAME " +
              "SELECT id FROM STREAM(spark_catalog.default.qualified_source)"),
          flows)
        runTriggeredFlow(
          spark,
          Seq("first_qualified_flow"),
          RegularTestCase("qualified-target", "", ""),
          Nil,
          flows)
        runTriggeredFlow(
          spark,
          Seq("second_qualified_flow"),
          RegularTestCase("qualified-target", "", ""),
          Nil,
          flows)
        checkAnswer(spark.table(s"$firstDb.sink"), Row(1))
        checkAnswer(spark.table(s"$secondDb.sink"), Row(1))
      }
    }

    withTable("retry_source", "retry_sink") {
      spark.sql("CREATE TABLE retry_sink (id INT) USING parquet")
      val flows = mutable.Map.empty[Seq[String], TriggeredFlow]
      registerTriggeredFlow(
        spark,
        createFlowCommand(
          spark,
          "CREATE FLOW retry_flow AS INSERT INTO retry_sink BY NAME " +
            "SELECT id FROM STREAM(retry_source)"),
        flows)
      intercept[AnalysisException] {
        runTriggeredFlow(
          spark,
          Seq("retry_flow"),
          RegularTestCase("retry", "", ""),
          Nil,
          flows)
      }
      spark.sql("CREATE TABLE retry_source (id INT) USING parquet")
      spark.sql("INSERT INTO retry_source VALUES (1)")
      runTriggeredFlow(
        spark,
        Seq("retry_flow"),
        RegularTestCase("retry", "", ""),
        Nil,
        flows)
      checkAnswer(spark.table("retry_sink"), Row(1))
    }

    val nonAnsiSession = spark.newSession()
    nonAnsiSession.conf.set(SQLConf.ANSI_ENABLED.key, false)
    withTable("non_ansi_source", "non_ansi_sink") {
      nonAnsiSession.sql("CREATE TABLE non_ansi_source (value STRING) USING parquet")
      nonAnsiSession.sql("INSERT INTO non_ansi_source VALUES ('not_an_int')")
      nonAnsiSession.sql("CREATE TABLE non_ansi_sink (value INT) USING parquet")
      val flows = mutable.Map.empty[Seq[String], TriggeredFlow]
      registerTriggeredFlow(
        nonAnsiSession,
        createFlowCommand(
          nonAnsiSession,
          "CREATE FLOW non_ansi_flow AS INSERT INTO non_ansi_sink BY NAME " +
            "SELECT CAST(value AS INT) AS value FROM STREAM(non_ansi_source)"),
        flows)
      runTriggeredFlow(
        nonAnsiSession,
        Seq("non_ansi_flow"),
        RegularTestCase("non-ansi", "", ""),
        Seq(SQLConf.ANSI_ENABLED.key -> "false"),
        flows)
      checkAnswer(nonAnsiSession.table("non_ansi_sink"), Row(null))
    }
  }

}
