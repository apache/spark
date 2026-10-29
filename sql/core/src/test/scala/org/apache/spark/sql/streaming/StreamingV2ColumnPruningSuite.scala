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

package org.apache.spark.sql.streaming

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{InMemoryBaseTable, InMemoryTableCatalog}
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2ScanRelation
import org.apache.spark.sql.internal.SQLConf

class StreamingV2ColumnPruningSuite extends StreamTest with BeforeAndAfter {

  private val tbl = "testcat.t"

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.conf.unset("spark.sql.catalog.testcat")
  }

  private def withSourceTable(properties: String = "")(f: => Unit): Unit = {
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (id bigint, data string, extra int) $properties")
      sql(s"INSERT INTO $tbl VALUES (1, 'a', 10), (2, 'b', 20)")
      f
    }
  }

  private def insert(values: String): StreamAction = Execute { _ =>
    sql(s"INSERT INTO $tbl VALUES $values")
  }

  private def assertReadSchema(columns: String*): StreamAction = AssertOnQuery { query =>
    val readSchemas = query.logicalPlan.collect {
      case relation: StreamingDataSourceV2ScanRelation =>
        relation.scan.readSchema().fieldNames.toSeq
    }
    assert(readSchemas.nonEmpty)
    readSchemas.foreach(readSchema => assert(readSchema === columns))
    true
  }

  test("read only the columns that a streaming query needs, also after a restart") {
    withSourceTable() {
      val df = spark.readStream.table(tbl).where("extra > 10").select("data")
      testStream(df)(
        ProcessAllAvailable(),
        assertReadSchema("data", "extra"),
        CheckAnswer(Row("b")),
        StopStream,
        insert("(3, 'c', 30), (4, 'd', 5)"),
        StartStream(),
        ProcessAllAvailable(),
        assertReadSchema("data", "extra"),
        CheckAnswer(Row("b"), Row("c")))
    }
  }

  test("read only the columns of a streaming aggregation, also after a restart") {
    withSourceTable() {
      val df = spark.readStream.table(tbl).groupBy("data").count()
      testStream(df, OutputMode.Complete())(
        ProcessAllAvailable(),
        assertReadSchema("data"),
        CheckAnswer(Row("a", 1L), Row("b", 1L)),
        StopStream,
        insert("(3, 'a', 30)"),
        StartStream(),
        ProcessAllAvailable(),
        assertReadSchema("data"),
        CheckAnswer(Row("a", 2L), Row("b", 1L)))
    }
  }

  test("read only the columns of a streaming deduplication, also after a restart") {
    withSourceTable() {
      val df = spark.readStream.table(tbl).dropDuplicates("data").select("id")
      testStream(df)(
        ProcessAllAvailable(),
        assertReadSchema("id", "data"),
        CheckAnswer(Row(1L), Row(2L)),
        StopStream,
        insert("(3, 'a', 30), (4, 'c', 40)"),
        StartStream(),
        ProcessAllAvailable(),
        assertReadSchema("id", "data"),
        CheckAnswer(Row(1L), Row(2L), Row(4L)))
    }
  }

  test("read the metadata columns that a streaming query needs") {
    withSourceTable() {
      val df = spark.readStream.table(tbl).select("data", "index")
      testStream(df)(
        ProcessAllAvailable(),
        assertReadSchema("data", "index"),
        CheckAnswerRowsByFunc(rows => {
          assert(rows.map(_.getString(0)).sorted === Seq("a", "b"))
          assert(rows.forall(!_.isNullAt(1)))
        }, lastOnly = false))
    }
  }

  test("read all the columns of the children of a union") {
    withSourceTable() {
      val source = spark.readStream.table(tbl)
      // A union maps the columns of its children by position, and its output is the one of its
      // first child, so the query does not reference the columns of its other children.
      val df = source.union(source).select("data")
      testStream(df)(
        ProcessAllAvailable(),
        assertReadSchema("id", "data", "extra"),
        CheckAnswer(Row("a"), Row("b"), Row("a"), Row("b")))
    }
  }

  test("read the columns that a scan returns when it reads more than the required ones") {
    withSourceTable(
        s"TBLPROPERTIES ('${InMemoryBaseTable.SIMULATE_PARTIAL_COLUMN_PRUNING}' = 'true')") {
      val df = spark.readStream.table(tbl).select("data", "index")
      testStream(df)(
        ProcessAllAvailable(),
        assertReadSchema("id", "data", "extra", "index"),
        CheckAnswerRowsByFunc(rows => {
          assert(rows.map(_.getString(0)).sorted === Seq("a", "b"))
          assert(rows.forall(!_.isNullAt(1)))
        }, lastOnly = false))
    }
  }

  test("read all the columns when the pruning of streaming scans is disabled") {
    withSQLConf(SQLConf.STREAMING_V2_COLUMN_PRUNING_ENABLED.key -> "false") {
      withSourceTable() {
        val df = spark.readStream.table(tbl).select("data")
        testStream(df)(
          ProcessAllAvailable(),
          assertReadSchema("id", "data", "extra"),
          CheckAnswer(Row("a"), Row("b")))
      }
    }
  }
}
