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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, BooleanType, IntegerType, MapType, StringType, StructField, StructType}

class DeltaBasedMergeIntoTableSuite extends DeltaBasedMergeIntoTableSuiteBase {

  import testImplicits._

  override protected lazy val extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("supports-deltas", "true")
    props
  }

  test("merge handles metadata columns correctly") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |{ "pk": 4, "salary": 400, "dep": "hr" }
          |{ "pk": 5, "salary": 500, "dep": "hr" }
          |""".stripMargin)

      val sourceDF = Seq(3, 4, 5, 6).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET t.salary = t.salary + 1
           |WHEN NOT MATCHED THEN
           | INSERT (pk, salary, dep) VALUES (s.pk, 0, 'new')
           |WHEN NOT MATCHED BY SOURCE AND pk = 1 THEN
           | DELETE
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(2, 200, "software"), // unchanged
          Row(3, 301, "hr"), // update
          Row(4, 401, "hr"), // update
          Row(5, 501, "hr"), // update
          Row(6, 0, "new"))) // insert

      checkLastWriteInfo(
        expectedRowSchema = table.schema,
        expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
        expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))

      checkLastWriteLog(
        deleteWriteLogEntry(id = 1, metadata = Row("hr", null)),
        updateWriteLogEntry(id = 3, metadata = Row("hr", null), data = Row(3, 301, "hr")),
        updateWriteLogEntry(id = 4, metadata = Row("hr", null), data = Row(4, 401, "hr")),
        updateWriteLogEntry(id = 5, metadata = Row("hr", null), data = Row(5, 501, "hr")),
        insertWriteLogEntry(data = Row(6, 0, "new")))
    }
  }

  test("merge into schema evolution add column with nested field and set explicit columns" +
    "2") {
    Seq(true, false).foreach { withSchemaEvolution =>
      withTempView("source") {
        createAndInitTable(
          s"""pk INT NOT NULL,
             |s STRUCT<c1: INT, c2: STRUCT<a: ARRAY<INT>, m: MAP<STRING, STRING>>>,
             |dep STRING""".stripMargin,
          """{ "pk": 1, "s": { "c1": 2, "c2": { "a": [1,2], "m": { "a": "b" } } }, "dep": "hr" }""")

        val sourceTableSchema = StructType(Seq(
          StructField("pk", IntegerType, nullable = false),
          StructField("s", StructType(Seq(
            StructField("c1", IntegerType),
            StructField("c2", StructType(Seq(
              StructField("a", ArrayType(IntegerType)),
              StructField("m", MapType(StringType, StringType)),
              StructField("c3", BooleanType) // new column
            )))
          ))),
          StructField("dep", StringType)
        ))
        val data = Seq(
          Row(1, Row(10, Row(Array(3, 4), Map("c" -> "d"), false)), "sales"),
          Row(2, Row(20, Row(Array(4, 5), Map("e" -> "f"), true)), "engineering")
        )
        spark.createDataFrame(spark.sparkContext.parallelize(data), sourceTableSchema)
          .createOrReplaceTempView("source")

        val schemaEvolutionClause = if (withSchemaEvolution) "WITH SCHEMA EVOLUTION" else ""
        val mergeStmt =
          s"""MERGE $schemaEvolutionClause
             |INTO $tableNameAsString t
             |USING source src
             |ON t.pk = src.pk
             |WHEN MATCHED THEN
             | UPDATE SET s.c1 = -1, s.c2.m = map('k', 'v'), s.c2.a = array(-1),
             | s.c2.c3 = src.s.c2.c3
             |WHEN NOT MATCHED THEN
             | INSERT (pk, s, dep) VALUES (src.pk,
             |   named_struct('c1', src.s.c1,
             |     'c2', named_struct('a', src.s.c2.a, 'm', map('g', 'h'), 'c3', true)), src.dep)
             |""".stripMargin

        if (withSchemaEvolution) {
          sql(mergeStmt)
          checkAnswer(
            sql(s"SELECT * FROM $tableNameAsString"),
            Seq(Row(1, Row(-1, Row(Seq(-1), Map("k" -> "v"), false)), "hr"),
              Row(2, Row(20, Row(Seq(4, 5), Map("g" -> "h"), true)), "engineering")))
        } else {
          val exception = intercept[org.apache.spark.sql.AnalysisException] {
            sql(mergeStmt)
          }
          assert(exception.errorClass.get == "FIELD_NOT_FOUND")
          assert(exception.getMessage.contains("No such struct field `c3` in `a`, `m`. "))
        }
      }
      sql(s"DROP TABLE IF EXISTS $tableNameAsString")
    }
  }
}
