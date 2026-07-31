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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.QueryTest.withQueryExecutionsCaptured
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryBaseTable, InMemoryCatalog, InMemoryRowLevelOperationTableCatalog, Table, TimeTravel}
import org.apache.spark.sql.execution.CommandResultExec
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.functions.lit

class LoadCountingInMemoryCatalog extends InMemoryCatalog {
  val singleArgLoads = new AtomicInteger(0)

  override def loadTable(ident: Identifier): Table = {
    singleArgLoads.incrementAndGet()
    super.loadTable(ident)
  }
}

class DataSourceV2OptionSuite extends DatasourceV2SQLBase {
  import testImplicits._

  private val catalogAndNamespace = "testcat.ns1.ns2."

  private def inMemoryCatalog: InMemoryCatalog =
    catalog("testcat").asInstanceOf[InMemoryCatalog]

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

  test("options are forwarded to loadTable - DataFrame API") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      spark.read.option("customOption", "customValue").table(t1).collect()

      val opts = inMemoryCatalog.lastLoadTableOptions
      assert(opts.isDefined)
      assert(opts.get.get("customOption") === "customValue")
    }
  }

  test("options are forwarded to loadTable - SQL") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"SELECT * FROM $t1 WITH ('customOption' = 'customValue')").collect()

      val opts = inMemoryCatalog.lastLoadTableOptions
      assert(opts.isDefined)
      assert(opts.get.get("customOption") === "customValue")
    }
  }

  test("options are forwarded to loadTable - DataStreamReader") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      // Trigger analysis of the streaming relation.
      spark.readStream.option("customOption", "customValue").table(t1).queryExecution.analyzed

      val opts = inMemoryCatalog.lastLoadTableOptions
      assert(opts.isDefined)
      assert(opts.get.get("customOption") === "customValue")
    }
  }

  test("options are forwarded to loadTable alongside time travel") {
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
      assert(opts.get.get("customOption") === "customValue")
      assert(opts.get.get("versionAsOf") === "v1")
    }
  }

  test("write privileges are carried in TableContext, internal key stripped") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a')")

      val ctx = inMemoryCatalog.lastTableContext
      assert(ctx.isDefined)
      assert(!ctx.get.writePrivileges().isEmpty)

      val opts = inMemoryCatalog.lastLoadTableOptions
      assert(opts.isDefined)
      // The internal write-privileges marker must not leak to the connector as a user option.
      assert(opts.get.get(UnresolvedRelation.REQUIRED_WRITE_PRIVILEGES) === null)
    }
  }

  test("SPARK-58389: execution refresh forwards options on a plain table read") {
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

      // Both analysis and the execution-time refresh must enter through the options-aware
      // overload. Each then delegates to the single-argument overload in this test catalog.
      val optionAwareLoads = loadCountingCatalog.loadTableCalls
        .map(_._2.get("split-size"))
        .filter(_ != null)
      assert(optionAwareLoads === Seq("5", "5"),
        s"expected analysis and refresh to forward split-size=5, got: $optionAwareLoads")
      assert(loadCountingCatalog.singleArgLoads.get() === 2,
        s"expected two delegated single-argument loads, got: " +
          loadCountingCatalog.singleArgLoads.get())
    }
  }

  test("SPARK-58389: a self-join with different options loads the table once per option bag") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")
      inMemoryCatalog.resetLoadTableCalls()

      // The two references share a name but carry different options. Because a catalog's
      // options-aware loadTable can return a different Table depending on the options, the analyzer
      // relation cache is keyed on the options, so each reference triggers its own loadTable rather
      // than reusing the first reference's Table.
      val df = sql(s"SELECT a.id FROM $t1 WITH (`split-size` = 5) a " +
        s"JOIN $t1 WITH (`split-size` = 9) b ON a.id = b.id")
      df.queryExecution.analyzed

      // Each distinct option bag reaches the catalog as its own loadTable call during analysis.
      val loadedOptions = inMemoryCatalog.loadTableCalls
        .map(_._2.get("split-size"))
        .filter(_ != null)
        .sorted
      assert(loadedOptions === Seq("5", "9"),
        s"expected one loadTable per distinct option bag, got: $loadedOptions")

      // The execution-time refresh also reloads once per option bag instead of sharing one Table
      // across the two references.
      inMemoryCatalog.resetLoadTableCalls()
      df.collect()
      val refreshedOptions = inMemoryCatalog.loadTableCalls
        .map(_._2.get("split-size"))
        .filter(_ != null)
        .sorted
      assert(refreshedOptions === Seq("5", "9"),
        s"expected refresh to load each distinct option bag, got: $refreshedOptions")

      // Each scan also keeps its own option end-to-end (neither reference inherits the other's).
      val splitSizes = df.queryExecution.optimizedPlan.collect {
        case s: DataSourceV2ScanRelation => s.relation.options.get("split-size")
      }.sorted
      assert(splitSizes === Seq("5", "9"))
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

      val splitSizeLoads = inMemoryCatalog.loadTableCalls
        .count(_._2.get("split-size") === "5")
      assert(splitSizeLoads === 1,
        s"expected a single loadTable for identical option bags, got: $splitSizeLoads")

      // The refresh phase also reuses one load for repeated references with identical options.
      inMemoryCatalog.resetLoadTableCalls()
      df.collect()
      val refreshedSplitSizeLoads = inMemoryCatalog.loadTableCalls
        .count(_._2.get("split-size") === "5")
      assert(refreshedSplitSizeLoads === 1,
        s"expected refresh to load identical option bags once, got: $refreshedSplitSizeLoads")
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

  test("SPARK-58389: execution refresh does not reuse cached relation with different options") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")

      val cached = spark.table(t1)
      cached.cache()
      try {
        assert(cached.count() === 2)

        // Analysis correctly rejects the option-free shared relation cache entry. Reset after
        // analysis to isolate the execution-time refresh, which must apply the same options check.
        val df = spark.read.option("split-size", "5").table(t1).filter("id > 0")
        df.queryExecution.analyzed
        inMemoryCatalog.resetLoadTableCalls()
        df.collect()

        val refreshLoads = inMemoryCatalog.loadTableCalls
          .map(_._2.get("split-size"))
          .filter(_ != null)
        assert(refreshLoads === Seq("5"),
          s"expected refresh to reject the option-free cache entry, got: $refreshLoads")
      } finally {
        cached.unpersist()
      }
    }
  }

  test("SPARK-58389: recaching preserves and forwards table options") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")

      val cached = spark.read.option("split-size", "5").table(t1)
      cached.cache()
      try {
        assert(cached.count() === 2)

        // Refreshing a cached table rebuilds its CacheManager entry. The rebuilt relation must
        // retain the original options, and the catalog must use them when it reloads the Table.
        inMemoryCatalog.resetLoadTableCalls()
        spark.catalog.refreshTable(t1)

        val recacheLoads = inMemoryCatalog.loadTableCalls.map(_._2.get("split-size"))
        assert(recacheLoads.contains("5"),
          s"expected recache to forward split-size=5, got: $recacheLoads")

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

  test("SPARK-58389: recaching a non-relation plan forwards table options") {
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
          .map(_._2.get("split-size"))
          .filter(_ != null)
        assert(recacheLoads.contains("5"),
          s"expected non-relation recache to forward split-size=5, got: $recacheLoads")

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
