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
import org.apache.spark.sql.functions.struct

class MetadataColumnSuite extends DatasourceV2SQLBase {
  import testImplicits._

  private val tbl = "testcat.t"

  private def prepareTable(): Unit = {
    sql(s"CREATE TABLE $tbl (id bigint, data string) PARTITIONED BY (bucket(4, id), id)")
    sql(s"INSERT INTO $tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")
  }

  test("SPARK-31255: Project a metadata column") {
    withTable(tbl) {
      prepareTable()
      val sqlQuery = sql(s"SELECT id, data, index, _partition FROM $tbl")
      val dfQuery = spark.table(tbl).select("id", "data", "index", "_partition")

      Seq(sqlQuery, dfQuery).foreach { query =>
        checkAnswer(query, Seq(Row(1, "a", 0, "3/1"), Row(2, "b", 0, "0/2"), Row(3, "c", 0, "1/3")))
      }
    }
  }

  test("SPARK-31255: Projects data column when metadata column has the same name") {
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (index bigint, data string) PARTITIONED BY (bucket(4, index), index)")
      sql(s"INSERT INTO $tbl VALUES (3, 'c'), (2, 'b'), (1, 'a')")

      val sqlQuery = sql(s"SELECT index, data, _partition FROM $tbl")
      val dfQuery = spark.table(tbl).select("index", "data", "_partition")

      Seq(sqlQuery, dfQuery).foreach { query =>
        checkAnswer(query, Seq(Row(3, "c", "1/3"), Row(2, "b", "0/2"), Row(1, "a", "3/1")))
      }
    }
  }

  test("SPARK-31255: * expansion does not include metadata columns") {
    withTable(tbl) {
      prepareTable()
      val sqlQuery = sql(s"SELECT * FROM $tbl")
      val dfQuery = spark.table(tbl)

      Seq(sqlQuery, dfQuery).foreach { query =>
        checkAnswer(query, Seq(Row(1, "a"), Row(2, "b"), Row(3, "c")))
      }
    }
  }

  test("SPARK-31255: metadata column should only be produced when necessary") {
    withTable(tbl) {
      prepareTable()
      val sqlQuery = sql(s"SELECT * FROM $tbl WHERE index = 0")
      val dfQuery = spark.table(tbl).filter("index = 0")

      Seq(sqlQuery, dfQuery).foreach { query =>
        assert(query.schema.fieldNames.toSeq == Seq("id", "data"))
      }
    }
  }

  test("SPARK-34547: metadata columns are resolved last") {
    withTable(tbl) {
      prepareTable()
      withTempView("v") {
        sql(s"CREATE TEMPORARY VIEW v AS SELECT * FROM " +
          s"VALUES (1, -1), (2, -2), (3, -3) AS v(id, index)")

        val sqlQuery = sql(s"SELECT $tbl.id, v.id, data, index, $tbl.index, v.index " +
          s"FROM $tbl JOIN v WHERE $tbl.id = v.id")
        val tableDf = spark.table(tbl)
        val viewDf = spark.table("v")
        val dfQuery = tableDf.join(viewDf, tableDf.col("id") === viewDf.col("id"))
          .select(s"$tbl.id", "v.id", "data", "index", s"$tbl.index", "v.index")

        Seq(sqlQuery, dfQuery).foreach { query =>
          checkAnswer(query,
            Seq(
              Row(1, 1, "a", -1, 0, -1),
              Row(2, 2, "b", -2, 0, -2),
              Row(3, 3, "c", -3, 0, -3)
            )
          )
        }
      }
    }
  }

  test("SPARK-34555: Resolve DataFrame metadata column") {
    withTable(tbl) {
      prepareTable()
      val table = spark.table(tbl)
      val dfQuery = table.select(
        table.col("id"),
        table.col("data"),
        table.col("index"),
        table.col("_partition")
      )

      checkAnswer(
        dfQuery,
        Seq(Row(1, "a", 0, "3/1"), Row(2, "b", 0, "0/2"), Row(3, "c", 0, "1/3"))
      )
    }
  }

  test("SPARK-34923: propagate metadata columns through Project") {
    withTable(tbl) {
      prepareTable()
      checkAnswer(
        spark.table(tbl).select("id", "data").select("index", "_partition"),
        Seq(Row(0, "3/1"), Row(0, "0/2"), Row(0, "1/3"))
      )
    }
  }

  test("SPARK-34923: do not propagate metadata columns through View") {
    val view = "view"
    withTable(tbl) {
      withTempView(view) {
        prepareTable()
        sql(s"CACHE TABLE $view AS SELECT * FROM $tbl")
        assertThrows[AnalysisException] {
          sql(s"SELECT index, _partition FROM $view")
        }
      }
    }
  }

  test("SPARK-34923: propagate metadata columns through Filter") {
    withTable(tbl) {
      prepareTable()
      val sqlQuery = sql(s"SELECT id, data, index, _partition FROM $tbl WHERE id > 1")
      val dfQuery = spark.table(tbl).where("id > 1").select("id", "data", "index", "_partition")

      Seq(sqlQuery, dfQuery).foreach { query =>
        checkAnswer(query, Seq(Row(2, "b", 0, "0/2"), Row(3, "c", 0, "1/3")))
      }
    }
  }

  test("SPARK-34923: propagate metadata columns through Sort") {
    withTable(tbl) {
      prepareTable()
      val sqlQuery = sql(s"SELECT id, data, index, _partition FROM $tbl ORDER BY id")
      val dfQuery = spark.table(tbl).orderBy("id").select("id", "data", "index", "_partition")

      Seq(sqlQuery, dfQuery).foreach { query =>
        checkAnswer(query, Seq(Row(1, "a", 0, "3/1"), Row(2, "b", 0, "0/2"), Row(3, "c", 0, "1/3")))
      }
    }
  }

  test("SPARK-34923: propagate metadata columns through RepartitionBy") {
    withTable(tbl) {
      prepareTable()
      val sqlQuery = sql(
        s"SELECT /*+ REPARTITION_BY_RANGE(3, id) */ id, data, index, _partition FROM $tbl")
      val dfQuery = spark.table(tbl).repartitionByRange(3, $"id")
        .select("id", "data", "index", "_partition")

      Seq(sqlQuery, dfQuery).foreach { query =>
        checkAnswer(query, Seq(Row(1, "a", 0, "3/1"), Row(2, "b", 0, "0/2"), Row(3, "c", 0, "1/3")))
      }
    }
  }

  test("SPARK-34923: propagate metadata columns through SubqueryAlias if child is leaf node") {
    val sbq = "sbq"
    withTable(tbl) {
      prepareTable()
      val sqlQuery = sql(
        s"SELECT $sbq.id, $sbq.data, $sbq.index, $sbq._partition FROM $tbl $sbq")
      val dfQuery = spark.table(tbl).as(sbq).select(
        s"$sbq.id", s"$sbq.data", s"$sbq.index", s"$sbq._partition")

      Seq(sqlQuery, dfQuery).foreach { query =>
        checkAnswer(query, Seq(Row(1, "a", 0, "3/1"), Row(2, "b", 0, "0/2"), Row(3, "c", 0, "1/3")))
      }

      assertThrows[AnalysisException] {
        sql(s"SELECT $sbq.index FROM (SELECT id FROM $tbl) $sbq")
      }
      assertThrows[AnalysisException] {
        spark.table(tbl).select($"id").as(sbq).select(s"$sbq.index")
      }
    }
  }

  test("SPARK-40149: select outer join metadata columns with DataFrame API") {
    val df1 = Seq(1 -> "a").toDF("k", "v").as("left")
    val df2 = Seq(1 -> "b").toDF("k", "v").as("right")
    val dfQuery = df1.join(df2, "k", "outer")
      .withColumn("left_all", struct($"left.*"))
      .withColumn("right_all", struct($"right.*"))
    checkAnswer(dfQuery, Row(1, "a", "b", Row(1, "a"), Row(1, "b")))
  }
}
