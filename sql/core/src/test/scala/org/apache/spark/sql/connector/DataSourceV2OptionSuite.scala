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

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.QueryTest.withQueryExecutionsCaptured
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.InMemoryBaseTable
import org.apache.spark.sql.execution.CommandResultExec
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.functions.lit

class DataSourceV2OptionSuite extends DatasourceV2SQLBase {
  import testImplicits._

  private val catalogAndNamespace = "testcat.ns1.ns2."

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
}
