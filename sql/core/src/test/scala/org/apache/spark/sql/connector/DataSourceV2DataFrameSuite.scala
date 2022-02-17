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

import java.util.Collections

import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SaveMode}
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, CreateTableAsSelect, LogicalPlan, ReplaceTableAsSelect}
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryTableCatalog}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.QueryExecutionListener

class DataSourceV2DataFrameSuite
  extends InsertIntoTests(supportsDynamicOverwrite = true, includeSQLOnlyTests = false) {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  import testImplicits._

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    spark.conf.set("spark.sql.catalog.testcat2", classOf[InMemoryTableCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.clear()
  }

  override protected val catalogAndNamespace: String = "testcat.ns1.ns2.tbls"
  override protected val v2Format: String = classOf[FakeV2Provider].getName

  override def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
  }

  override protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val dfw = insert.write.format(v2Format)
    if (mode != null) {
      dfw.mode(mode)
    }
    dfw.insertInto(tableName)
  }

  test("insertInto: append across catalog") {
    val t1 = "testcat.ns1.ns2.tbl"
    val t2 = "testcat2.db.tbl"
    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo")
      sql(s"CREATE TABLE $t2 (id bigint, data string) USING foo")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.insertInto(t1)
      spark.table(t1).write.insertInto(t2)
      checkAnswer(spark.table(t2), df)
    }
  }

  testQuietly("saveAsTable: table doesn't exist => create table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.saveAsTable(t1)
      checkAnswer(spark.table(t1), df)
    }
  }

  testQuietly("saveAsTable: table exists => append by name") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      // Default saveMode is ErrorIfExists
      intercept[TableAlreadyExistsException] {
        df.write.saveAsTable(t1)
      }
      assert(spark.table(t1).count() === 0)

      // appends are by name not by position
      df.select('data, 'id).write.mode("append").saveAsTable(t1)
      checkAnswer(spark.table(t1), df)
    }
  }

  testQuietly("saveAsTable: table overwrite and table doesn't exist => create table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.mode("overwrite").saveAsTable(t1)
      checkAnswer(spark.table(t1), df)
    }
  }

  testQuietly("saveAsTable: table overwrite and table exists => replace table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 USING foo AS SELECT 'c', 'd'")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.mode("overwrite").saveAsTable(t1)
      checkAnswer(spark.table(t1), df)
    }
  }

  testQuietly("saveAsTable: ignore mode and table doesn't exist => create table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.mode("ignore").saveAsTable(t1)
      checkAnswer(spark.table(t1), df)
    }
  }

  testQuietly("saveAsTable: ignore mode and table exists => do nothing") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      sql(s"CREATE TABLE $t1 USING foo AS SELECT 'c', 'd'")
      df.write.mode("ignore").saveAsTable(t1)
      checkAnswer(spark.table(t1), Seq(Row("c", "d")))
    }
  }

  testQuietly("SPARK-29778: saveAsTable: append mode takes write options") {

    var plan: LogicalPlan = null
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        plan = qe.analyzed
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }

    try {
      spark.listenerManager.register(listener)

      val t1 = "testcat.ns1.ns2.tbl"

      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo")

      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.option("other", "20").mode("append").saveAsTable(t1)

      sparkContext.listenerBus.waitUntilEmpty()
      plan match {
        case p: AppendData =>
          assert(p.writeOptions == Map("other" -> "20"))
        case other =>
          fail(s"Expected to parse ${classOf[AppendData].getName} from query," +
            s"got ${other.getClass.getName}: $plan")
      }

      checkAnswer(spark.table(t1), df)
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }

  test("Cannot write data with intervals to v2") {
    withSQLConf(SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true") {
      withTable("testcat.table_name") {
        val testCatalog = spark.sessionState.catalogManager.catalog("testcat").asTableCatalog
        testCatalog.createTable(
          Identifier.of(Array(), "table_name"),
          new StructType().add("i", "interval"),
          Array.empty, Collections.emptyMap[String, String])
        val df = sql(s"select interval 1 millisecond as i")
        val v2Writer = df.writeTo("testcat.table_name")
        val e1 = intercept[AnalysisException](v2Writer.append())
        assert(e1.getMessage.contains(s"Cannot use interval type in the table schema."))
        val e2 = intercept[AnalysisException](v2Writer.overwrite(df("i")))
        assert(e2.getMessage.contains(s"Cannot use interval type in the table schema."))
        val e3 = intercept[AnalysisException](v2Writer.overwritePartitions())
        assert(e3.getMessage.contains(s"Cannot use interval type in the table schema."))
      }
    }
  }

  test("options to scan v2 table should be passed to DataSourceV2Relation") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      val df1 = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df1.write.saveAsTable(t1)

      val optionName = "fakeOption"
      val df2 = spark.read
        .option(optionName, false)
        .table(t1)
      val options = df2.queryExecution.analyzed.collectFirst {
        case d: DataSourceV2Relation => d.options
      }.get
      assert(options.get(optionName) === "false")
    }
  }

  test("CTAS and RTAS should take write options") {

    var plan: LogicalPlan = null
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        plan = qe.analyzed
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }

    try {
      spark.listenerManager.register(listener)

      val t1 = "testcat.ns1.ns2.tbl"

      val df1 = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df1.write.option("option1", "20").saveAsTable(t1)

      sparkContext.listenerBus.waitUntilEmpty()
      plan match {
        case o: CreateTableAsSelect =>
          assert(o.writeOptions == Map("option1" -> "20"))
        case other =>
          fail(s"Expected to parse ${classOf[CreateTableAsSelect].getName} from query," +
            s"got ${other.getClass.getName}: $plan")
      }
      checkAnswer(spark.table(t1), df1)

      val df2 = Seq((1L, "d"), (2L, "e"), (3L, "f")).toDF("id", "data")
      df2.write.option("option2", "30").mode("overwrite").saveAsTable(t1)

      sparkContext.listenerBus.waitUntilEmpty()
      plan match {
        case o: ReplaceTableAsSelect =>
          assert(o.writeOptions == Map("option2" -> "30"))
        case other =>
          fail(s"Expected to parse ${classOf[ReplaceTableAsSelect].getName} from query," +
            s"got ${other.getClass.getName}: $plan")
      }

      checkAnswer(spark.table(t1), df2)
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }
}
