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

package org.apache.spark.sql.connector

import java.util
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.QueryTest.withQueryExecutionsCaptured
import org.apache.spark.sql.catalyst.analysis.{
  AnalysisContext,
  RelationCache,
  RelationResolution,
  UnresolvedRelation,
  V2TableReference}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.catalog.{
  Identifier,
  InMemoryBaseTable,
  InMemoryCatalog,
  InMemoryRowLevelOperationTableCatalog,
  Table,
  TableChange,
  TableWritePrivilege,
  TimeTravel}
import org.apache.spark.sql.execution.CommandResultExec
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class LoadCountingInMemoryCatalog extends InMemoryCatalog {
  val singleArgLoads = new AtomicInteger(0)

  override def loadTable(ident: Identifier): Table = {
    singleArgLoads.incrementAndGet()
    super.loadTable(ident)
  }
}

class StateAwareInMemoryCatalog extends LoadCountingInMemoryCatalog {
  // Include Spark's internal marker so the write-context test detects if it leaks before state
  // option projection. Production catalogs should declare only raw user option keys.
  override def tableStateOptionKeys(): util.Set[String] =
    util.Set.of("snapshot", UnresolvedRelation.REQUIRED_WRITE_PRIVILEGES)
}

class DataSourceV2OptionSuite extends DatasourceV2SQLBase {
  import testImplicits._

  private val catalogAndNamespace = "testcat.ns1.ns2."

  private def inMemoryCatalog: InMemoryCatalog =
    catalog("testcat").asInstanceOf[InMemoryCatalog]

  private def withStateAwareTable(
      f: (StateAwareInMemoryCatalog, String) => Unit): Unit = {
    withSQLConf(
      "spark.sql.catalog.statecat" -> classOf[StateAwareInMemoryCatalog].getName,
      "spark.sql.catalog.statecat.copyOnLoad" -> "true") {
      val tableName = "statecat.ns.table"
      withTable(tableName) {
        sql(s"CREATE TABLE $tableName (id bigint, data string)")
        sql(s"INSERT INTO $tableName VALUES (1, 'a'), (2, 'b')")
        f(catalog("statecat").asInstanceOf[StateAwareInMemoryCatalog], tableName)
      }
    }
  }

  private def assertOnlySnapshotOptions(
      catalog: StateAwareInMemoryCatalog,
      expectedSnapshot: String): Unit = {
    val loadOptions = catalog.loadTableCalls.map(_._2)
    assert(loadOptions.nonEmpty, "expected at least one options-aware table load")
    assert(loadOptions.forall { options =>
      options.size() == 1 && options.get("snapshot") == expectedSnapshot
    }, s"expected only snapshot=$expectedSnapshot to be forwarded, got: $loadOptions")
  }

  test("SPARK-36680: Supports Dynamic Table Options for SQL Select") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")

      var df = sql(s"SELECT * FROM $t1")
      var collected = df.queryExecution.optimizedPlan.collect {
        case scan: DataSourceV2ScanRelation =>
          assert(scan.relation.options.isEmpty)
      }
      assert (collected.size == 1)
      checkAnswer(df, Seq(Row(1, "a"), Row(2, "b")))

      df = sql(s"SELECT * FROM $t1 WITH (`split-size` = 5)")
      collected = df.queryExecution.optimizedPlan.collect {
        case scan: DataSourceV2ScanRelation =>
          assert(scan.relation.options.get("split-size") == "5")
      }
      assert (collected.size == 1)
      checkAnswer(df, Seq(Row(1, "a"), Row(2, "b")))

      collected = df.queryExecution.executedPlan.collect {
        case BatchScanExec(_, scan: InMemoryBaseTable#InMemoryBatchScan, _, _, _, _) =>
          assert(scan.options.get("split-size") === "5")
      }
      assert (collected.size == 1)

      val noValues = intercept[AnalysisException](
        sql(s"SELECT * FROM $t1 WITH (`split-size`)"))
      assert(noValues.message.contains(
        "Operation not allowed: Values must be specified for key(s): [split-size]"))
    }
  }

  test("SPARK-50286: Propagate options for DataFrameReader") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")

      var df = spark.table(t1)
      var collected = df.queryExecution.optimizedPlan.collect {
        case scan: DataSourceV2ScanRelation =>
          assert(scan.relation.options.isEmpty)
      }
      assert (collected.size == 1)
      checkAnswer(df, Seq(Row(1, "a"), Row(2, "b")))

      df = spark.read.option("split-size", "5").table(t1)
      collected = df.queryExecution.optimizedPlan.collect {
        case scan: DataSourceV2ScanRelation =>
          assert(scan.relation.options.get("split-size") == "5")
      }
      assert (collected.size == 1)
      checkAnswer(df, Seq(Row(1, "a"), Row(2, "b")))

      collected = df.queryExecution.executedPlan.collect {
        case BatchScanExec(_, scan: InMemoryBaseTable#InMemoryBatchScan, _, _, _, _) =>
          assert(scan.options.get("split-size") === "5")
      }
      assert (collected.size == 1)
    }
  }

  test("SPARK-49098, SPARK-50286: Supports Dynamic Table Options for SQL Insert") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      val df = sql(s"INSERT INTO $t1 WITH (`write.split-size` = 10) VALUES (1, 'a'), (2, 'b')")

      var collected = df.queryExecution.optimizedPlan.collect {
        case CommandResult(_, AppendData(relation: DataSourceV2Relation, _, _, _, _, _, _), _, _) =>
          assert(relation.options.get("write.split-size") == "10")
      }
      assert (collected.size == 1)

      collected = df.queryExecution.executedPlan.collect {
        case CommandResultExec(
          _, AppendDataExec(_, _, write, _, _),
          _) =>
          val append = write.toBatch.asInstanceOf[InMemoryBaseTable#Append]
          assert(append.info.options.get("write.split-size") === "10")
      }
      assert (collected.size == 1)

      val insertResult = sql(s"SELECT * FROM $t1")
      checkAnswer(insertResult, Seq(Row(1, "a"), Row(2, "b")))
    }
  }

  test("SPARK-58330: dynamic options are not lost when INSERT selects from the same table") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")

      // The target and the source are the same table, so they share one per-query relation-cache
      // entry. Each reference must keep its own options: the target's write options must not be
      // dropped by the source reference, and the source's read options must not leak to the target.
      val df = sql(s"INSERT INTO $t1 WITH (`write.split-size` = 10) " +
        s"SELECT id, data FROM $t1 WITH (`split-size` = 5)")

      val appendData = df.queryExecution.optimizedPlan.collectFirst {
        case CommandResult(_, a: AppendData, _, _) => a
      }.getOrElse(fail("expected an AppendData in the optimized plan"))

      // target: the write relation keeps its own option and does not pick up the source's
      val targetOptions = appendData.table.asInstanceOf[DataSourceV2Relation].options
      assert(targetOptions.get("write.split-size") === "10", "target write option")
      assert(!targetOptions.containsKey("split-size"), "target must not see source option")

      // source: the read relation under the query keeps its own option and does not pick up
      // the target's
      val sourceOptions = appendData.query.collect {
        case r: DataSourceV2Relation => r.options
        case s: DataSourceV2ScanRelation => s.relation.options
      }.filter(_.containsKey("split-size"))
      assert(sourceOptions.nonEmpty, "source relation carrying the option was not found")
      sourceOptions.foreach { opts =>
        assert(opts.get("split-size") === "5", "source read option")
        assert(!opts.containsKey("write.split-size"), "source must not see target option")
      }

      checkAnswer(sql(s"SELECT * FROM $t1"),
        Seq(Row(1, "a"), Row(2, "b"), Row(1, "a"), Row(2, "b")))
    }
  }

  test("SPARK-58330: each reference in a self-join keeps its own dynamic options") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")

      // Both references are reads of the same table with different options, so they share one
      // per-query relation-cache entry. Neither scan should inherit the other's options.
      val df = sql(s"SELECT a.id FROM $t1 WITH (`split-size` = 5) a " +
        s"JOIN $t1 WITH (`split-size` = 9) b ON a.id = b.id")

      val splitSizes = df.queryExecution.optimizedPlan.collect {
        case s: DataSourceV2ScanRelation => s.relation.options.get("split-size")
      }
      assert(splitSizes.sorted === Seq("5", "9"))

      checkAnswer(df, Seq(Row(1), Row(2)))
    }
  }

  test("SPARK-58330: each reference in a streaming self-join keeps its own dynamic options") {
    withSQLConf(
      "spark.sql.catalog.streamcat" -> classOf[InMemoryRowLevelOperationTableCatalog].getName) {
      val t1 = "streamcat.ns.table"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string)")

        // Both references are streaming reads of the same table with different options, so they
        // share one per-query relation-cache entry. Neither should inherit the other's options.
        // The plan is only analyzed here, never executed (no writeStream.start()), so no actual
        // streaming query runs.
        val df = sql(s"SELECT a.id FROM STREAM $t1 WITH (`split-size` = 5) a " +
          s"JOIN STREAM $t1 WITH (`split-size` = 9) b ON a.id = b.id")

        val splitSizes = df.queryExecution.analyzed.collect {
          case r: StreamingRelationV2 => r.extraOptions.get("split-size")
        }
        assert(splitSizes.sorted === Seq("5", "9"))
      }
    }
  }

  test("SPARK-58330: a streaming CTE referencing the same table keeps its own dynamic options") {
    withSQLConf(
      "spark.sql.catalog.streamcat" -> classOf[InMemoryRowLevelOperationTableCatalog].getName) {
      val t1 = "streamcat.ns.table"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string)")

        // The CTE's inner scan of `STREAM t WITH (...)` and the outer `STREAM t WITH (...)`
        // reference resolve to the same per-query relation-cache entry; CTE substitution must
        // not let one leak into the other. Analysis-only, as above.
        val df = sql(s"WITH x AS (SELECT id FROM STREAM $t1 WITH (`split-size` = 5)) " +
          s"SELECT a.id FROM STREAM $t1 WITH (`split-size` = 9) a CROSS JOIN x")

        val splitSizes = df.queryExecution.analyzed.collect {
          case r: StreamingRelationV2 => r.extraOptions.get("split-size")
        }
        assert(splitSizes.sorted === Seq("5", "9"))
      }
    }
  }

  test("SPARK-50286: Propagate options for DataFrameWriter Append") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      val captured = withQueryExecutionsCaptured(spark) {
        Seq(1 -> "a", 2 -> "b").toDF("id", "data")
          .write
          .option("write.split-size", "10")
          .mode("append")
          .insertInto(t1)
      }
      assert(captured.size === 1)
      val qe = captured.head
      var collected = qe.optimizedPlan.collect {
        case AppendData(_: DataSourceV2Relation, _, writeOptions, _, _, _, _) =>
          assert(writeOptions("write.split-size") == "10")
      }
      assert (collected.size == 1)

      collected = qe.executedPlan.collect {
        case AppendDataExec(_, _, write, _, _) =>
          val append = write.toBatch.asInstanceOf[InMemoryBaseTable#Append]
          assert(append.info.options.get("write.split-size") === "10")
      }
      assert (collected.size == 1)
    }
  }

  test("SPARK-50286: Propagate options for DataFrameWriterV2 Append") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      val captured = withQueryExecutionsCaptured(spark) {
        Seq(1 -> "a", 2 -> "b").toDF("id", "data")
          .writeTo(t1)
          .option("write.split-size", "10")
          .append()
      }
      assert(captured.size === 1)
      val qe = captured.head
      var collected = qe.optimizedPlan.collect {
        case AppendData(_: DataSourceV2Relation, _, writeOptions, _, _, _, _) =>
          assert(writeOptions("write.split-size") == "10")
      }
      assert (collected.size == 1)

      collected = qe.executedPlan.collect {
        case AppendDataExec(_, _, write, _, _) =>
          val append = write.toBatch.asInstanceOf[InMemoryBaseTable#Append]
          assert(append.info.options.get("write.split-size") === "10")
      }
      assert (collected.size == 1)
    }
  }

  test("SPARK-49098, SPARK-50286: Supports Dynamic Table Options for SQL Insert Overwrite") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")

      val df = sql(s"INSERT OVERWRITE $t1 WITH (`write.split-size` = 10) " +
        s"VALUES (3, 'c'), (4, 'd')")
      var collected = df.queryExecution.optimizedPlan.collect {
        case CommandResult(_,
          OverwriteByExpression(relation: DataSourceV2Relation, _, _, _, _, _, _, _),
          _, _) =>
          assert(relation.options.get("write.split-size") === "10")
      }
      assert (collected.size == 1)

      collected = df.queryExecution.executedPlan.collect {
        case CommandResultExec(
          _, OverwriteByExpressionExec(_, _, write, _, _),
          _) =>
          val append = write.toBatch.asInstanceOf[InMemoryBaseTable#TruncateAndAppend]
          assert(append.info.options.get("write.split-size") === "10")
      }
      assert (collected.size == 1)

      val insertResult = sql(s"SELECT * FROM $t1")
      checkAnswer(insertResult, Seq(Row(3, "c"), Row(4, "d")))
    }
  }

  test("SPARK-50286: Propagate options for DataFrameWriterV2 OverwritePartitions") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")

      val captured = withQueryExecutionsCaptured(spark) {
        Seq(3 -> "c", 4 -> "d").toDF("id", "data")
          .writeTo(t1)
          .option("write.split-size", "10")
          .overwritePartitions()
      }
      assert(captured.size === 1)
      val qe = captured.head
      var collected = qe.optimizedPlan.collect {
        case OverwritePartitionsDynamic(_: DataSourceV2Relation, _, writeOptions, _, _, _) =>
          assert(writeOptions("write.split-size") === "10")
      }
      assert (collected.size == 1)

      collected = qe.executedPlan.collect {
        case OverwritePartitionsDynamicExec(_, _, write, _, _) =>
          val dynOverwrite = write.toBatch.asInstanceOf[InMemoryBaseTable#DynamicOverwrite]
          assert(dynOverwrite.info.options.get("write.split-size") === "10")
      }
      assert (collected.size == 1)
    }
  }

  test("SPARK-49098, SPARK-50286: Supports Dynamic Table Options for SQL Insert Replace") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")

      val df = sql(s"INSERT INTO $t1 WITH (`write.split-size` = 10) " +
        s"REPLACE WHERE TRUE " +
        s"VALUES (3, 'c'), (4, 'd')")
      var collected = df.queryExecution.optimizedPlan.collect {
        case CommandResult(_,
          OverwriteByExpression(relation: DataSourceV2Relation, _, _, _, _, _, _, _),
          _, _) =>
          assert(relation.options.get("write.split-size") == "10")
      }
      assert (collected.size == 1)

      collected = df.queryExecution.executedPlan.collect {
        case CommandResultExec(
          _, OverwriteByExpressionExec(_, _, write, _, _),
          _) =>
          val append = write.toBatch.asInstanceOf[InMemoryBaseTable#TruncateAndAppend]
          assert(append.info.options.get("write.split-size") === "10")
      }
      assert (collected.size == 1)

      val insertResult = sql(s"SELECT * FROM $t1")
      checkAnswer(insertResult, Seq(Row(3, "c"), Row(4, "d")))
    }
  }

  test("SPARK-50286: Propagate options for DataFrameWriter Overwrite") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      val captured = withQueryExecutionsCaptured(spark) {
        Seq(1 -> "a", 2 -> "b").toDF("id", "data")
          .write
          .option("write.split-size", "10")
          .mode("overwrite")
          .insertInto(t1)
      }
      assert(captured.size === 1)

      val qe = captured.head
      var collected = qe.optimizedPlan.collect {
        case OverwriteByExpression(_: DataSourceV2Relation, _, _, writeOptions, _, _, _, _) =>
          assert(writeOptions("write.split-size") === "10")
      }
      assert (collected.size == 1)

      collected = qe.executedPlan.collect {
        case OverwriteByExpressionExec(_, _, write, _, _) =>
          val append = write.toBatch.asInstanceOf[InMemoryBaseTable#TruncateAndAppend]
          assert(append.info.options.get("write.split-size") === "10")
      }
      assert (collected.size == 1)
    }
  }

  test("SPARK-50286: Propagate options for DataFrameWriterV2 Overwrite") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")

      val captured = withQueryExecutionsCaptured(spark) {
        Seq(3 -> "c", 4 -> "d").toDF("id", "data")
          .writeTo(t1)
          .option("write.split-size", "10")
          .overwrite(lit(true))
      }
      assert(captured.size === 1)
      val qe = captured.head

      var collected = qe.optimizedPlan.collect {
        case OverwriteByExpression(_: DataSourceV2Relation, _, _, writeOptions, _, _, _, _) =>
          assert(writeOptions("write.split-size") === "10")
      }
      assert (collected.size == 1)

      collected = qe.executedPlan.collect {
        case OverwriteByExpressionExec(_, _, write, _, _) =>
          val append = write.toBatch.asInstanceOf[InMemoryBaseTable#TruncateAndAppend]
          assert(append.info.options.get("write.split-size") === "10")
      }
      assert (collected.size == 1)
    }
  }

  test("only table-state options are forwarded to loadTable - DataFrame API") {
    withStateAwareTable { (stateCatalog, tableName) =>
      stateCatalog.resetLoadTableCalls()
      spark.read
        .option("snapshot", "s1")
        .option("customOption", "customValue")
        .table(tableName)
        .collect()

      assertOnlySnapshotOptions(stateCatalog, "s1")
    }
  }

  test("only table-state options are forwarded to loadTable - SQL") {
    withStateAwareTable { (stateCatalog, tableName) =>
      stateCatalog.resetLoadTableCalls()
      sql(s"SELECT * FROM $tableName " +
        "WITH ('snapshot' = 's1', 'customOption' = 'customValue')").collect()

      assertOnlySnapshotOptions(stateCatalog, "s1")
    }
  }

  test("only table-state options are forwarded to loadTable - DataStreamReader") {
    withStateAwareTable { (stateCatalog, tableName) =>
      stateCatalog.resetLoadTableCalls()
      // Trigger analysis of the streaming relation.
      spark.readStream
        .option("snapshot", "s1")
        .option("customOption", "customValue")
        .table(tableName)
        .queryExecution
        .analyzed

      assertOnlySnapshotOptions(stateCatalog, "s1")
    }
  }

  test("time travel is passed in TableContext and excluded from load options") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      // versionAsOf is the default time-travel version option key
      // (SQLConf.TIME_TRAVEL_VERSION_KEY). Pin a versioned copy so the versioned load succeeds.
      inMemoryCatalog.pinTable(Identifier.of(Array("ns1", "ns2"), "table"), "v1")

      spark.read
        .option("versionAsOf", "v1")
        .option("customOption", "customValue")
        .table(t1)
        .collect()

      val ctx = inMemoryCatalog.lastTableContext
      assert(ctx.isDefined)
      assert(ctx.get.timeTravel().isPresent)
      assert(ctx.get.timeTravel().get() === new TimeTravel.AsOfVersion("v1"))

      val opts = inMemoryCatalog.lastLoadTableOptions
      assert(opts.isDefined)
      assert(opts.get.isEmpty)
    }
  }

  test("write privileges are carried in TableContext, internal key stripped") {
    withStateAwareTable { (stateCatalog, tableName) =>
      stateCatalog.resetLoadTableCalls()
      sql(s"INSERT INTO $tableName WITH ('snapshot' = 's1') VALUES (3, 'c')")

      val ctx = stateCatalog.lastTableContext
      assert(ctx.isDefined)
      assert(!ctx.get.writePrivileges().isEmpty)

      val opts = stateCatalog.lastLoadTableOptions
      assert(opts.isDefined)
      assert(opts.get.get("snapshot") === "s1")
      // The internal write-privileges marker must not leak to the connector as a user option.
      assert(opts.get.get(UnresolvedRelation.REQUIRED_WRITE_PRIVILEGES) === null)
    }
  }

  test("execution refresh filters load options when a catalog declares no state options") {
    registerCatalog("loadcounting", classOf[LoadCountingInMemoryCatalog])
    val loadCountingCatalog =
      catalog("loadcounting").asInstanceOf[LoadCountingInMemoryCatalog]
    val t1 = "loadcounting.ns1.ns2.table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")
      loadCountingCatalog.resetLoadTableCalls()
      loadCountingCatalog.singleArgLoads.set(0)

      spark.read.option("split-size", "5").table(t1).collect()

      // Both analysis and the execution-time refresh enter through the options-aware overload,
      // but this catalog declares no table-state options, so each receives an empty option map.
      val optionAwareLoads = loadCountingCatalog.loadTableCalls
      assert(optionAwareLoads.size === 2,
        s"expected one analysis load and one refresh load, got: $optionAwareLoads")
      assert(optionAwareLoads.forall(_._2.isEmpty),
        s"expected analysis and refresh to filter split-size, got: $optionAwareLoads")
      assert(loadCountingCatalog.singleArgLoads.get() === 2,
        s"expected two delegated single-argument loads, got: " +
          loadCountingCatalog.singleArgLoads.get())
    }
  }

  test("catalogs with no declared state options share table state across option bags") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")
      inMemoryCatalog.resetLoadTableCalls()

      // The catalog declares no table-state option keys, so none of them affect table state.
      val df = sql(s"SELECT a.id FROM $t1 WITH (`split-size` = 5) a " +
        s"JOIN $t1 WITH (`split-size` = 9) b ON a.id = b.id")
      df.queryExecution.analyzed

      // The first reference loads the table and the second reuses that table state.
      val analysisLoads = inMemoryCatalog.loadTableCalls
      assert(analysisLoads.size === 1,
        s"expected the second option bag to reuse the loaded table state, got: $analysisLoads")
      assert(analysisLoads.head._2.isEmpty,
        s"expected no table-state options to be forwarded, got: $analysisLoads")

      // The execution-time refresh loads the first reference and reuses its table state for the
      // second reference.
      inMemoryCatalog.resetLoadTableCalls()
      df.collect()
      val refreshLoads = inMemoryCatalog.loadTableCalls
      assert(refreshLoads.size === 1,
        s"expected refresh to share table state across option bags, got: $refreshLoads")
      assert(refreshLoads.head._2.isEmpty,
        s"expected refresh to filter scan options, got: $refreshLoads")

      // Each scan also keeps its own option end-to-end (neither reference inherits the other's).
      val splitSizes = df.queryExecution.optimizedPlan.collect {
        case s: DataSourceV2ScanRelation => s.relation.options.get("split-size")
      }.sorted
      assert(splitSizes === Seq("5", "9"))
    }
  }

  test("same table state shares one Table while preserving each reference's options") {
    withStateAwareTable { (stateCatalog, tableName) =>
      stateCatalog.resetLoadTableCalls()
      stateCatalog.singleArgLoads.set(0)

      val df = sql(s"SELECT a.id FROM $tableName " +
        s"WITH (`SnApShOt` = 's1', `split-size` = 5) a JOIN $tableName " +
        s"WITH (`snapshot` = 's1', `split-size` = 9) b ON a.id = b.id")

      val analyzedRelations = df.queryExecution.analyzed.collect {
        case r: DataSourceV2Relation if r.options.containsKey("split-size") => r
      }
      assert(analyzedRelations.size == 2)
      assert(analyzedRelations.map(_.options.get("split-size")).sorted == Seq("5", "9"))
      assert(analyzedRelations.map(_.options.get("snapshot")).distinct == Seq("s1"))
      assert(analyzedRelations.head.table eq analyzedRelations.last.table)

      val analysisLoads = stateCatalog.loadTableCalls.filter(_._2.get("snapshot") == "s1")
      assert(analysisLoads.size == 1,
        s"expected one catalog load for state s1 during analysis, got: $analysisLoads")
      assert(analysisLoads.head._2.size() == 1,
        s"expected scan options to be filtered during analysis, got: $analysisLoads")

      stateCatalog.resetLoadTableCalls()
      stateCatalog.singleArgLoads.set(0)
      assert(df.collect().toSeq == Seq(Row(1), Row(2)))

      val refreshLoads = stateCatalog.loadTableCalls.filter(_._2.get("snapshot") == "s1")
      assert(refreshLoads.size == 1,
        s"expected one catalog load for state s1 during refresh, got: $refreshLoads")
      assert(refreshLoads.head._2.size() == 1,
        s"expected scan options to be filtered during refresh, got: $refreshLoads")
      val refreshedRelations = df.queryExecution.optimizedPlan.collect {
        case s: DataSourceV2ScanRelation if s.relation.options.containsKey("split-size") =>
          s.relation
      }
      assert(refreshedRelations.size == 2)
      assert(refreshedRelations.head.table eq refreshedRelations.last.table)
    }
  }

  test("shared relation cache selects by state and preserves each reference's full options") {
    withStateAwareTable { (stateCatalog, tableName) =>
      val cached = spark.read
        .option("snapshot", "s1")
        .option("split-size", "5")
        .table(tableName)
      val otherStateCached = spark.read
        .option("snapshot", "s2")
        .option("split-size", "7")
        .table(tableName)
      cached.cache()
      otherStateCached.cache()
      try {
        cached.collect()
        // Cache this relation last so it is searched first. An s1 lookup must scan past it.
        otherStateCached.collect()
        val cachedTable = cached.queryExecution.analyzed.collectFirst {
          case r: DataSourceV2Relation => r.table
        }.getOrElse(fail("expected a cached v2 relation"))

        def relations(df: DataFrame): Seq[DataSourceV2Relation] = {
          df.queryExecution.analyzed.collect {
            case r: DataSourceV2Relation if r.options.containsKey("snapshot") => r
          }
        }

        stateCatalog.resetLoadTableCalls()
        val cachedFirst = sql(s"SELECT a.id FROM $tableName " +
          s"WITH (`snapshot` = 's1', `split-size` = 5) a JOIN $tableName " +
          s"WITH (`snapshot` = 's1', `split-size` = 9) b ON a.id = b.id")
        val cachedFirstRelations = relations(cachedFirst)
        assert(cachedFirstRelations.size == 2)
        assert(cachedFirstRelations.forall(_.table eq cachedTable))
        assert(cachedFirstRelations.map(_.options.get("split-size")).sorted == Seq("5", "9"))
        assert(stateCatalog.loadTableCalls.count(_._2.get("snapshot") == "s1") == 1)

        stateCatalog.resetLoadTableCalls()
        assert(cachedFirst.collect().map(_.getLong(0)).sorted.toSeq == Seq(1L, 2L))
        assert(stateCatalog.loadTableCalls.isEmpty,
          s"shared relation cache pin should avoid refresh reloads, got: " +
            stateCatalog.loadTableCalls)

        stateCatalog.resetLoadTableCalls()
        val uncachedFirst = sql(s"SELECT a.id FROM $tableName " +
          s"WITH (`snapshot` = 's1', `split-size` = 9) a JOIN $tableName " +
          s"WITH (`snapshot` = 's1', `split-size` = 5) b ON a.id = b.id")
        val uncachedFirstRelations = relations(uncachedFirst)
        assert(uncachedFirstRelations.size == 2)
        assert(uncachedFirstRelations.forall(_.table eq cachedTable))
        assert(uncachedFirstRelations.map(_.options.get("split-size")).sorted == Seq("5", "9"))
        assert(stateCatalog.loadTableCalls.count(_._2.get("snapshot") == "s1") == 1)

        stateCatalog.resetLoadTableCalls()
        assert(uncachedFirst.collect().map(_.getLong(0)).sorted.toSeq == Seq(1L, 2L))
        assert(stateCatalog.loadTableCalls.isEmpty,
          s"shared relation cache pin should avoid refresh reloads, got: " +
            stateCatalog.loadTableCalls)
      } finally {
        otherStateCached.unpersist()
        cached.unpersist()
      }
    }
  }

  test("shared relation cache makes table selection independent of reference order") {
    withStateAwareTable { (stateCatalog, tableName) =>
      val ident = Identifier.of(Array("ns"), "table")
      stateCatalog.alterTable(ident, TableChange.setProperty("version", "X"))

      val cached = spark.read.option("split-size", "5").table(tableName)
      cached.cache()
      try {
        cached.collect()
        val versionX = cached.queryExecution.analyzed.collectFirst {
          case r: DataSourceV2Relation => r.table
        }.getOrElse(fail("expected a cached v2 relation"))
        assert(versionX.properties().get("version") == "X")

        // Simulate an external catalog update that does not invalidate Spark's relation cache.
        val versionY = stateCatalog.alterTable(
          ident,
          TableChange.setProperty("version", "Y"))
        assert(versionY.properties().get("version") == "Y")
        assert(versionY.id == versionX.id)
        assert(versionY ne versionX)

        def relations(df: DataFrame): Seq[DataSourceV2Relation] = {
          df.queryExecution.analyzed.collect {
            case r: DataSourceV2Relation if r.options.containsKey("split-size") => r
          }
        }

        val readA = spark.read.option("split-size", "5").table(tableName)
        val readB = spark.read.option("split-size", "9").table(tableName)
        assert(relations(readA).map(_.table) == Seq(versionX))
        assert(relations(readB).map(_.table) == Seq(versionX))

        val aThenB = sql(s"SELECT a.id FROM $tableName WITH (`split-size` = 5) a " +
          s"JOIN $tableName WITH (`split-size` = 9) b ON a.id = b.id")
        val bThenA = sql(s"SELECT a.id FROM $tableName WITH (`split-size` = 9) a " +
          s"JOIN $tableName WITH (`split-size` = 5) b ON a.id = b.id")
        assert(relations(aThenB).map(_.table) == Seq(versionX, versionX))
        assert(relations(bThenA).map(_.table) == Seq(versionX, versionX))
      } finally {
        cached.unpersist()
      }
    }
  }

  test("shared relation cache rejects a different declared table state") {
    withStateAwareTable { (stateCatalog, tableName) =>
      val cached = spark.read
        .option("snapshot", "s1")
        .option("split-size", "5")
        .table(tableName)
      cached.cache()
      try {
        cached.collect()
        val cachedTable = cached.queryExecution.analyzed.collectFirst {
          case r: DataSourceV2Relation => r.table
        }.getOrElse(fail("expected a cached v2 relation"))

        stateCatalog.resetLoadTableCalls()
        val differentState = spark.read
          .option("snapshot", "s2")
          .option("split-size", "5")
          .table(tableName)
        val relation = differentState.queryExecution.analyzed.collectFirst {
          case r: DataSourceV2Relation => r
        }.getOrElse(fail("expected a v2 relation"))

        assert(relation.table ne cachedTable)
        assert(relation.options.get("snapshot") == "s2")
        assert(relation.options.get("split-size") == "5")
        assert(stateCatalog.loadTableCalls.count(_._2.get("snapshot") == "s2") == 1)
      } finally {
        cached.unpersist()
      }
    }
  }

  test("streaming references participate in the query table-state cache") {
    withStateAwareTable { (stateCatalog, tableName) =>
      stateCatalog.resetLoadTableCalls()
      val df = sql(s"SELECT a.id FROM STREAM $tableName " +
        s"WITH (`snapshot` = 's1', `split-size` = 5) a JOIN STREAM $tableName " +
        s"WITH (`snapshot` = 's1', `split-size` = 9) b ON a.id = b.id")
      val relations = df.queryExecution.analyzed.collect {
        case r: StreamingRelationV2 if r.extraOptions.containsKey("snapshot") => r
      }

      assert(relations.size == 2)
      assert(relations.map(_.extraOptions.get("split-size")).sorted == Seq("5", "9"))
      assert(relations.head.table eq relations.last.table)
      val stateLoads = stateCatalog.loadTableCalls.count(_._2.get("snapshot") == "s1")
      assert(stateLoads == 1, s"expected one streaming table load, got: $stateLoads")
    }
  }

  test("different table-state option values establish separate table pins") {
    withStateAwareTable { (stateCatalog, tableName) =>
      stateCatalog.resetLoadTableCalls()
      stateCatalog.singleArgLoads.set(0)

      val df = sql(s"SELECT a.id FROM $tableName " +
        s"WITH (`snapshot` = 's1', `split-size` = 5) a JOIN $tableName " +
        s"WITH (`snapshot` = 's2', `split-size` = 9) b ON a.id = b.id")
      val relations = df.queryExecution.analyzed.collect {
        case r: DataSourceV2Relation if r.options.containsKey("snapshot") => r
      }

      assert(relations.size == 2)
      assert(relations.map(_.options.get("snapshot")).sorted == Seq("s1", "s2"))
      assert(relations.head.table ne relations.last.table)
      val loadedStates = stateCatalog.loadTableCalls
        .map(_._2.get("snapshot"))
        .filter(_ != null)
        .sorted
      assert(loadedStates == Seq("s1", "s2"))
    }
  }

  test("persistent write targets bypass query-scoped cache lookups") {
    withStateAwareTable { (stateCatalog, tableName) =>
      stateCatalog.resetLoadTableCalls()
      val resolver = new RelationResolution(
        spark.sessionState.catalogManager,
        RelationCache.empty)
      val options = new CaseInsensitiveStringMap(
        java.util.Map.of("snapshot", "s1", "split-size", "5"))
      val read = UnresolvedRelation(tableName.split("\\.").toSeq, options)
      val write = read.requireWritePrivileges(Set(TableWritePrivilege.INSERT))

      def resolve(relation: UnresolvedRelation): DataSourceV2Relation = {
        resolver.resolveRelation(relation).flatMap(_.collectFirst {
          case r: DataSourceV2Relation => r
        }).getOrElse(fail(s"failed to resolve ${relation.name} as a v2 relation"))
      }

      AnalysisContext.withNewAnalysisContext {
        val readRelation = resolve(read)
        val writeRelation = resolve(write)

        assert(readRelation.table ne writeRelation.table)
        assert(AnalysisContext.get.tableCache.size == 1)
        assert(AnalysisContext.get.relationCache.size == 1)
      }

      assert(stateCatalog.loadTableCalls.size == 2)
      assert(stateCatalog.loadTableCalls.count(_._1.writePrivileges().isEmpty) == 1)
      assert(stateCatalog.loadTableCalls.count(
        _._1.writePrivileges().contains(TableWritePrivilege.INSERT)) == 1)
      assert(stateCatalog.loadTableCalls.forall(_._2.get("snapshot") == "s1"))
      assert(stateCatalog.loadTableCalls.forall(_._2.size() == 1))
    }
  }

  test("persistent write targets establish table pins for subsequent reads") {
    withStateAwareTable { (stateCatalog, tableName) =>
      stateCatalog.resetLoadTableCalls()
      val resolver = new RelationResolution(
        spark.sessionState.catalogManager,
        RelationCache.empty)
      val writeOptions = new CaseInsensitiveStringMap(
        java.util.Map.of("snapshot", "s1", "split-size", "5"))
      val readOptions = new CaseInsensitiveStringMap(
        java.util.Map.of("snapshot", "s1", "split-size", "9"))
      val write = UnresolvedRelation(tableName.split("\\.").toSeq, writeOptions)
        .requireWritePrivileges(Set(TableWritePrivilege.INSERT))
      val read = UnresolvedRelation(tableName.split("\\.").toSeq, readOptions)

      def resolve(relation: UnresolvedRelation): DataSourceV2Relation = {
        resolver.resolveRelation(relation).flatMap(_.collectFirst {
          case r: DataSourceV2Relation => r
        }).getOrElse(fail(s"failed to resolve ${relation.name} as a v2 relation"))
      }

      AnalysisContext.withNewAnalysisContext {
        val writeRelation = resolve(write)
        val readRelation = resolve(read)

        assert(writeRelation.table eq readRelation.table)
        assert(writeRelation.options.get("split-size") == "5")
        assert(readRelation.options.get("split-size") == "9")
        assert(AnalysisContext.get.tableCache.size == 1)
        assert(AnalysisContext.get.relationCache.size == 2)
      }

      assert(stateCatalog.loadTableCalls.size == 1)
      assert(stateCatalog.loadTableCalls.head._1.writePrivileges().contains(
        TableWritePrivilege.INSERT))
      assertOnlySnapshotOptions(stateCatalog, "s1")
    }
  }

  test("transaction V2TableReference skips shared lookup and writes bypass query caches") {
    withStateAwareTable { (stateCatalog, tableName) =>
      val original = spark.read
        .option("snapshot", "s1")
        .option("split-size", "5")
        .table(tableName)
        .queryExecution
        .analyzed
        .collectFirst { case r: DataSourceV2Relation => r }
        .getOrElse(fail("expected a v2 relation"))
      val readRef = V2TableReference.createForTransaction(original)
      val otherReadRef = V2TableReference.createForTransaction(original.copy(
        options = new CaseInsensitiveStringMap(
          java.util.Map.of("snapshot", "s1", "split-size", "9"))))
      val writeRef = V2TableReference.createForWriteTarget(original)
      var sharedRelationCacheLookups = 0
      val sharedRelationCache: RelationCache = (_, _, _, _, _) => {
        sharedRelationCacheLookups += 1
        None
      }
      val resolver = new RelationResolution(
        spark.sessionState.catalogManager,
        sharedRelationCache)

      stateCatalog.resetLoadTableCalls()
      stateCatalog.singleArgLoads.set(0)
      AnalysisContext.withNewAnalysisContext {
        val readRelation = resolver.resolveReference(readRef).asInstanceOf[DataSourceV2Relation]
        val cachedReadRelation =
          resolver.resolveReference(readRef).asInstanceOf[DataSourceV2Relation]
        val otherReadRelation =
          resolver.resolveReference(otherReadRef).asInstanceOf[DataSourceV2Relation]
        val writeRelation = resolver.resolveReference(writeRef).asInstanceOf[DataSourceV2Relation]
        val readAfterWriteRelation =
          resolver.resolveReference(readRef).asInstanceOf[DataSourceV2Relation]

        assert(readRelation.table eq cachedReadRelation.table)
        assert(readRelation.table eq otherReadRelation.table)
        assert(readRelation.table eq readAfterWriteRelation.table)
        assert(readRelation.table ne writeRelation.table)
        assert(readRelation.options.get("split-size") == "5")
        assert(otherReadRelation.options.get("split-size") == "9")
        assert(AnalysisContext.get.tableCache.size == 1)
        assert(AnalysisContext.get.relationCache.size == 2)
      }

      assert(sharedRelationCacheLookups == 0)
      assert(stateCatalog.singleArgLoads.get() == 2)
      assert(stateCatalog.loadTableCalls.size == 1)
      assert(stateCatalog.loadTableCalls.head._2.get("snapshot") == "s1")
      assert(stateCatalog.loadTableCalls.head._2.size() == 1)
    }
  }

  test("nested view resolution shares the query table-state cache") {
    withStateAwareTable { (stateCatalog, tableName) =>
      withView("state_nested_view") {
        sql(s"CREATE VIEW state_nested_view AS SELECT * FROM $tableName " +
          s"WITH (`snapshot` = 's1', `split-size` = 5)")
        stateCatalog.resetLoadTableCalls()

        val df = sql(s"SELECT v.id FROM state_nested_view v JOIN $tableName " +
          s"WITH (`snapshot` = 's1', `split-size` = 9) b ON v.id = b.id")
        val relations = df.queryExecution.analyzed.collect {
          case r: DataSourceV2Relation if r.options.containsKey("snapshot") => r
        }

        assert(relations.size == 2)
        assert(relations.map(_.options.get("split-size")).sorted == Seq("5", "9"))
        assert(relations.head.table eq relations.last.table)
        val stateLoads = stateCatalog.loadTableCalls.count(_._2.get("snapshot") == "s1")
        assert(stateLoads == 1, s"expected one nested-view table load, got: $stateLoads")
      }
    }
  }

  test("temporary-view V2TableReference consults shared cache only for the initial pin") {
    withStateAwareTable { (stateCatalog, tableName) =>
      val cached = spark.read
        .option("snapshot", "s1")
        .option("split-size", "5")
        .table(tableName)
        .queryExecution
        .analyzed
        .collectFirst { case r: DataSourceV2Relation => r }
        .getOrElse(fail("expected a v2 relation"))
      val initialRef = V2TableReference.createForTempView(cached, Seq("state_view"))
      val otherOptionsRef = V2TableReference.createForTempView(
        cached.copy(options = new CaseInsensitiveStringMap(
          java.util.Map.of("snapshot", "s1", "split-size", "9"))),
        Seq("state_view"))
      var sharedRelationCacheLookups = 0
      val sharedRelationCache: RelationCache = (_, _, _, _, _) => {
        sharedRelationCacheLookups += 1
        Some(cached)
      }
      val resolver = new RelationResolution(
        spark.sessionState.catalogManager,
        sharedRelationCache)

      stateCatalog.resetLoadTableCalls()
      stateCatalog.singleArgLoads.set(0)
      AnalysisContext.withNewAnalysisContext {
        val initialRelation =
          resolver.resolveReference(initialRef).asInstanceOf[DataSourceV2Relation]
        val cachedRelation =
          resolver.resolveReference(initialRef).asInstanceOf[DataSourceV2Relation]
        val otherOptionsRelation =
          resolver.resolveReference(otherOptionsRef).asInstanceOf[DataSourceV2Relation]

        assert(initialRelation.table eq cached.table)
        assert(cachedRelation.table eq cached.table)
        assert(otherOptionsRelation.table eq cached.table)
        assert(initialRelation.options.get("split-size") == "5")
        assert(otherOptionsRelation.options.get("split-size") == "9")
        assert(AnalysisContext.get.tableCache.size == 1)
        assert(AnalysisContext.get.relationCache.size == 2)
      }

      assert(sharedRelationCacheLookups == 1)
      assert(stateCatalog.singleArgLoads.get() == 1)
      assert(stateCatalog.loadTableCalls.size == 1)
      assert(stateCatalog.loadTableCalls.head._2.get("snapshot") == "s1")
      assert(stateCatalog.loadTableCalls.head._2.size() == 1)
    }
  }

  test("temporary-view re-resolution preserves the CacheManager table pin") {
    withStateAwareTable { (stateCatalog, tableName) =>
      withTempView("state_view") {
        val cached = spark.read
          .option("snapshot", "s1")
          .option("split-size", "5")
          .table(tableName)
        cached.cache()
        try {
          cached.collect()
          val cachedTable = cached.queryExecution.analyzed.collectFirst {
            case r: DataSourceV2Relation => r.table
          }.getOrElse(fail("expected a cached v2 relation"))
          cached.createOrReplaceTempView("state_view")
          stateCatalog.resetLoadTableCalls()
          stateCatalog.singleArgLoads.set(0)

          val df = sql(s"SELECT v.id FROM state_view v JOIN $tableName " +
            s"WITH (`snapshot` = 's1', `split-size` = 9) b ON v.id = b.id")
          val relations = df.queryExecution.analyzed.collect {
            case r: DataSourceV2Relation if r.options.containsKey("snapshot") => r
          }

          assert(relations.size == 2)
          assert(relations.map(_.options.get("split-size")).sorted == Seq("5", "9"))
          assert(relations.forall(_.table eq cachedTable))
          assert(stateCatalog.singleArgLoads.get() == 1,
            s"expected one table load while re-resolving the query, got: " +
              stateCatalog.singleArgLoads.get())
          assert(stateCatalog.loadTableCalls.size == 1)
          assert(stateCatalog.loadTableCalls.head._2.get("snapshot") == "s1")
          assert(stateCatalog.loadTableCalls.head._2.size() == 1)
        } finally {
          cached.unpersist()
        }
      }
    }
  }

  test("SPARK-58389: repeated references with the same options load the table once") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")
      inMemoryCatalog.resetLoadTableCalls()

      // Both references carry the same options, so they share one relation-cache entry: the table
      // is loaded once (resolve-once-per-query is preserved for identical option bags).
      val df = sql(s"SELECT a.id FROM $t1 WITH (`split-size` = 5) a " +
        s"JOIN $t1 WITH (`split-size` = 5) b ON a.id = b.id")
      df.queryExecution.analyzed

      val analysisLoads = inMemoryCatalog.loadTableCalls
      assert(analysisLoads.size === 1,
        s"expected a single loadTable for identical option bags, got: $analysisLoads")
      assert(analysisLoads.head._2.isEmpty,
        s"expected scan options to be filtered during analysis, got: $analysisLoads")

      // The refresh phase also reuses one load for repeated references with identical options.
      inMemoryCatalog.resetLoadTableCalls()
      df.collect()
      val refreshLoads = inMemoryCatalog.loadTableCalls
      assert(refreshLoads.size === 1,
        s"expected refresh to load identical option bags once, got: $refreshLoads")
      assert(refreshLoads.head._2.isEmpty,
        s"expected scan options to be filtered during refresh, got: $refreshLoads")
    }
  }

  test("SPARK-58389: time travel option on a write target is rejected with a user-facing error") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")

      // A time-travel option on a write target is reachable via the option form (the `AS OF`
      // syntax is blocked earlier by the parser). It must surface as a user-facing analysis error,
      // not the internal TableContext mutual-exclusion guard (which would report INTERNAL_ERROR).
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"INSERT INTO $t1 WITH ('versionAsOf' = 'v1') VALUES (1, 'a')")
        },
        condition = "UNSUPPORTED_FEATURE.TIME_TRAVEL",
        parameters = Map("relationId" -> "`testcat`.`ns1`.`ns2`.`table`"))
    }
  }

  test("SPARK-58389: CACHE TABLE result is not reused for a read carrying different options") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")

      // Cache the option-free read. The CacheManager keys entries on the query plan, and a DSv2
      // relation's plan carries its `options`, so the cached entry's fingerprint is "no options".
      val cached = spark.table(t1)
      cached.cache()
      try {
        val cacheManager = spark.sharedState.cacheManager

        // An option-free read has the same plan fingerprint, so it reuses the cached result.
        assert(cacheManager.lookupCachedData(spark.table(t1)).isDefined,
          "an option-free read should hit the cached result")

        // A read carrying options has a different fingerprint, so it must NOT reuse the cached
        // result -- otherwise the connector's options would be silently ignored on a cache hit.
        assert(
          cacheManager.lookupCachedData(spark.read.option("split-size", "5").table(t1)).isEmpty,
          "a read carrying options must not reuse the option-free cached result")
      } finally {
        cached.unpersist()
      }
    }
  }

  test("execution refresh reuses cached table state with different scan options") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")

      val cached = spark.table(t1)
      cached.cache()
      try {
        assert(cached.count() === 2)

        // This catalog declares no state options, so analysis and refresh may reuse the cached
        // table while the relation keeps split-size=5 as its complete option bag.
        val df = spark.read.option("split-size", "5").table(t1).filter("id > 0")
        val analyzedRelation = df.queryExecution.analyzed.collectFirst {
          case r: DataSourceV2Relation => r
        }.getOrElse(fail("expected a v2 relation"))
        assert(analyzedRelation.options.get("split-size") == "5")
        inMemoryCatalog.resetLoadTableCalls()
        df.collect()

        assert(inMemoryCatalog.loadTableCalls.isEmpty,
          s"expected refresh to reuse the matching table state, got: " +
            inMemoryCatalog.loadTableCalls)
      } finally {
        cached.unpersist()
      }
    }
  }

  test("recaching preserves relation options and filters table load options") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")

      val cached = spark.read.option("split-size", "5").table(t1)
      cached.cache()
      try {
        assert(cached.count() === 2)

        // Refreshing a cached table rebuilds its CacheManager entry. The rebuilt relation retains
        // the original options, but this catalog declares no state options for the table reload.
        inMemoryCatalog.resetLoadTableCalls()
        spark.catalog.refreshTable(t1)

        val recacheLoads = inMemoryCatalog.loadTableCalls
        assert(recacheLoads.nonEmpty, "expected recache to reload the table")
        assert(recacheLoads.forall(_._2.isEmpty),
          s"expected recache to filter split-size, got: $recacheLoads")

        val cacheManager = spark.sharedState.cacheManager
        val sameOptions = spark.read.option("split-size", "5").table(t1)
        val recached = cacheManager.lookupCachedData(sameOptions)
        assert(recached.isDefined, "a read with the original options should hit after recache")

        val recachedOptions = recached.get.plan.collect {
          case r: DataSourceV2Relation => r.options.get("split-size")
        }
        assert(recachedOptions === Seq("5"),
          s"expected the recached relation to retain split-size=5, got: $recachedOptions")

        assert(cacheManager.lookupCachedData(spark.table(t1)).isEmpty,
          "an option-free read must not reuse the recached option-carrying result")
      } finally {
        spark.catalog.clearCache()
      }
    }
  }

  test("recaching a filtered plan filters table load options") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")

      // The filter forces CacheManager.tryRefreshPlan through V2TableRefreshUtil instead of the
      // bare-relation fast path covered by the preceding test.
      val cached = spark.read.option("split-size", "5").table(t1).filter("id > 0")
      cached.cache()
      try {
        assert(cached.count() === 2)
        inMemoryCatalog.resetLoadTableCalls()

        spark.catalog.refreshTable(t1)

        val recacheLoads = inMemoryCatalog.loadTableCalls
        assert(recacheLoads.nonEmpty, "expected filtered recache to reload the table")
        assert(recacheLoads.forall(_._2.isEmpty),
          s"expected filtered recache to filter split-size, got: $recacheLoads")

        val samePlan = spark.read.option("split-size", "5").table(t1).filter("id > 0")
        assert(spark.sharedState.cacheManager.lookupCachedData(samePlan).isDefined,
          "the filtered plan should remain cached after refresh")
      } finally {
        spark.catalog.clearCache()
      }
    }
  }

  test("SPARK-58389: a DataFrame temp view's options do not leak to a later reference") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")

      // The temp view resolves via the V2TableReference path, whose relation carries the view's
      // options. A later option-free reference to the same table must not inherit them.
      withTempView("v") {
        spark.read.option("split-size", "5").table(t1).createOrReplaceTempView("v")
        val df = sql(s"SELECT v.id FROM v JOIN $t1 b ON v.id = b.id")

        val splitSizes = df.queryExecution.analyzed.collect {
          case r: DataSourceV2Relation => Option(r.options.get("split-size"))
        }
        // Exactly one reference (`v`) keeps its option; `b` (option-free) must not inherit it.
        assert(splitSizes.flatten === Seq("5"),
          s"option leaked to the option-free reference, got: $splitSizes")
        assert(splitSizes.contains(None),
          s"expected an option-free reference, got: $splitSizes")
      }
    }
  }
}
