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
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, CommandResult, OverwriteByExpression, OverwritePartitionsDynamic}
import org.apache.spark.sql.connector.catalog.InMemoryBaseTable
import org.apache.spark.sql.execution.{CommandResultExec, QueryExecution}
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.util.QueryExecutionListener

class DataSourceV2OptionSuite extends DatasourceV2SQLBase {

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

  test("SPARK-50286: Propagate options for DataFrame Read") {
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

  test("SPARK-36680, SPARK-50286: Supports Dynamic Table Options for SQL Insert") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      val df = sql(s"INSERT INTO $t1 WITH (`write.split-size` = 10) VALUES (1, 'a'), (2, 'b')")

      var collected = df.queryExecution.optimizedPlan.collect {
        case CommandResult(_, AppendData(relation: DataSourceV2Relation, _, _, _, _, _), _, _) =>
          assert(relation.options.get("write.split-size") == "10")
      }
      assert (collected.size == 1)

      collected = df.queryExecution.executedPlan.collect {
        case CommandResultExec(
          _, AppendDataExec(_, _, write),
          _) =>
          val append = write.toBatch.asInstanceOf[InMemoryBaseTable#Append]
          assert(append.info.options.get("write.split-size") === "10")
      }
      assert (collected.size == 1)

      val insertResult = sql(s"SELECT * FROM $t1")
      checkAnswer(insertResult, Seq(Row(1, "a"), Row(2, "b")))
    }
  }

  test("SPARK-50286: Propagate options for DataFrame Append") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      withListener {
        sql("VALUES (1, 'a'), (2, 'b')")
          .withColumnRenamed("col1", "id")
          .withColumnRenamed("col2", "data")
          .writeTo(t1)
          .option("write.split-size", "10")
          .append()
      } { qe =>
        var collected = qe.optimizedPlan.collect {
          case AppendData(_: DataSourceV2Relation, _, writeOptions, _, _, _) =>
            assert(writeOptions("write.split-size") == "10")
        }
        assert (collected.size == 1)

        collected = qe.executedPlan.collect {
          case AppendDataExec(_, _, write) =>
            val append = write.toBatch.asInstanceOf[InMemoryBaseTable#Append]
            assert(append.info.options.get("write.split-size") === "10")
        }
        assert (collected.size == 1)
      }
    }
  }

  test("SPARK-36680, SPARK-50286: Supports Dynamic Table Options for SQL Insert Overwrite") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")

      val df = sql(s"INSERT OVERWRITE $t1 WITH (`write.split-size` = 10) " +
        s"VALUES (3, 'c'), (4, 'd')")
      var collected = df.queryExecution.optimizedPlan.collect {
        case CommandResult(_,
          OverwriteByExpression(relation: DataSourceV2Relation, _, _, _, _, _, _),
          _, _) =>
          assert(relation.options.get("write.split-size") === "10")
      }
      assert (collected.size == 1)

      collected = df.queryExecution.executedPlan.collect {
        case CommandResultExec(
          _, OverwriteByExpressionExec(_, _, write),
          _) =>
          val append = write.toBatch.asInstanceOf[InMemoryBaseTable#TruncateAndAppend]
          assert(append.info.options.get("write.split-size") === "10")
      }
      assert (collected.size == 1)

      val insertResult = sql(s"SELECT * FROM $t1")
      checkAnswer(insertResult, Seq(Row(3, "c"), Row(4, "d")))
    }
  }

  test("SPARK-50286: Propagate options for DataFrame OverwritePartitions") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")

      withListener {
        sql("VALUES (3, 'c'), (4, 'd')")
          .withColumnRenamed("col1", "id")
          .withColumnRenamed("col2", "data")
          .writeTo(t1)
          .option("write.split-size", "10")
          .overwritePartitions()
      } { qe =>
        var collected = qe.optimizedPlan.collect {
          case OverwritePartitionsDynamic(_: DataSourceV2Relation, _, writeOptions, _, _) =>
            assert(writeOptions("write.split-size") === "10")
        }
        assert (collected.size == 1)

        collected = qe.executedPlan.collect {
          case OverwritePartitionsDynamicExec(_, _, write) =>
            val dynOverwrite = write.toBatch.asInstanceOf[InMemoryBaseTable#DynamicOverwrite]
            assert(dynOverwrite.info.options.get("write.split-size") === "10")
        }
        assert (collected.size == 1)
      }
    }
  }

  test("SPARK-36680, SPARK-50286: Supports Dynamic Table Options for SQL Insert Replace") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")

      val df = sql(s"INSERT INTO $t1 WITH (`write.split-size` = 10) " +
        s"REPLACE WHERE TRUE " +
        s"VALUES (3, 'c'), (4, 'd')")
      var collected = df.queryExecution.optimizedPlan.collect {
        case CommandResult(_,
          OverwriteByExpression(relation: DataSourceV2Relation, _, _, _, _, _, _),
          _, _) =>
          assert(relation.options.get("write.split-size") == "10")
      }
      assert (collected.size == 1)

      collected = df.queryExecution.executedPlan.collect {
        case CommandResultExec(
          _, OverwriteByExpressionExec(_, _, write),
          _) =>
          val append = write.toBatch.asInstanceOf[InMemoryBaseTable#TruncateAndAppend]
          assert(append.info.options.get("write.split-size") === "10")
      }
      assert (collected.size == 1)

      val insertResult = sql(s"SELECT * FROM $t1")
      checkAnswer(insertResult, Seq(Row(3, "c"), Row(4, "d")))
    }
  }

  test("SPARK-50286: Propagate options for DataFrame Overwrite") {
    val t1 = s"${catalogAndNamespace}table"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string)")
      sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")

      withListener {
        sql("VALUES (3, 'c'), (4, 'd')")
          .withColumnRenamed("col1", "id")
          .withColumnRenamed("col2", "data")
          .writeTo(t1)
          .option("write.split-size", "10")
          .overwrite(lit(true))
      } { qe =>
        var collected = qe.optimizedPlan.collect {
          case OverwriteByExpression(_: DataSourceV2Relation, _, _, writeOptions, _, _, _) =>
            assert(writeOptions("write.split-size") === "10")
        }
        assert (collected.size == 1)

        collected = qe.executedPlan.collect {
          case OverwriteByExpressionExec(_, _, write) =>
            val append = write.toBatch.asInstanceOf[InMemoryBaseTable#TruncateAndAppend]
            assert(append.info.options.get("write.split-size") === "10")
        }
        assert (collected.size == 1)
      }
    }
  }

  def withListener(action: => Unit)(callback: QueryExecution => Unit): Unit = {
    var executedQe: Option[QueryExecution] = None
    val listener = new QueryExecutionListener {
      override def onFailure(f: String, qe: QueryExecution, e: Exception): Unit = throw e

      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit =
        executedQe = Some(qe)
    }
    spark.listenerManager.register(listener)
    try {
      action
      sparkContext.listenerBus.waitUntilEmpty()
      // forward the qe to the scalatest thread otherwise assertion failure won't fail the test
      callback(executedQe.getOrElse(fail("query may be failed")))
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }
}
