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

import org.apache.spark.{SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.optimizer.ColumnPruning
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.connector.catalog.{InMemoryBaseTable, InMemoryRowLevelOperationTable}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class GroupBasedColumnUpdateTableSuite extends UpdateTableSuiteBase {

  override protected lazy val extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("column-update-cow", "true")
    props
  }

  test("column-update ReplaceData: column update schema has only the declared columns") {
    createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")

    assert(table.lastWriteInfo.columnUpdateSchema.get.fieldNames.toSeq ==
      Seq("pk", "dep", "salary"))
  }

  test("column-update ReplaceData: data correctness -- bonus preserved, salary updated") {
    createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
        |{ "pk": 3, "salary": 300, "bonus": 30, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, 10, "hr") ::
      Row(2, 200, 20, "software") ::
      Row(3, -1, 30, "hr") :: Nil)
    // CoW: pk=1 and pk=3 match (2 updates); the 'hr' partition has no other rows so no copies.
    checkUpdateMetrics(numUpdatedRows = 2, numCopiedRows = 0)
  }

  test("column-update ReplaceData: subquery WHERE condition data correctness") {
    createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
        |{ "pk": 3, "salary": 300, "bonus": 30, "dep": "hr" }
        |""".stripMargin)

    import testImplicits._
    val subqueryDF = Seq("hr").toDF()
    subqueryDF.createOrReplaceTempView("target_deps")

    sql(
      s"""UPDATE $tableNameAsString
         |SET salary = -1
         |WHERE dep IN (SELECT * FROM target_deps)
         |""".stripMargin)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, 10, "hr") ::
      Row(2, 200, 20, "software") ::
      Row(3, -1, 30, "hr") :: Nil)
    checkUpdateMetrics(numUpdatedRows = 2, numCopiedRows = 0)
  }

  test("column-update ReplaceData: runtime group filtering data correctness") {
    Seq(true, false).foreach { dppEnabled =>
      Seq(true, false).foreach { aqeEnabled =>
        withSQLConf(
            SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> dppEnabled.toString,
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled.toString) {
          withTable(tableNameAsString) {
            withTempView("matched_pk") {
              createAndInitTable("pk INT, salary INT, bonus INT, dep STRING",
                """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
                  |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "software" }
                  |{ "pk": 3, "salary": 300, "bonus": 30, "dep": "hr" }
                  |""".stripMargin)

              import testImplicits._
              val matchedPkDF = Seq(Some(1), None).toDF()
              matchedPkDF.createOrReplaceTempView("matched_pk")

              executeAndCheckScans(
                s"UPDATE $tableNameAsString SET salary = -1 " +
                  s"WHERE pk IN (SELECT * FROM matched_pk)",
                primaryScanSchema =
                  "pk INT, salary INT, dep STRING, _partition STRING, index INT",
                groupFilterScanSchema = Some("pk INT, dep STRING"))

              checkAnswer(
                sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
                Row(1, -1, 10, "hr") ::
                Row(2, 200, 20, "software") ::
                Row(3, 300, 30, "hr") :: Nil)

              checkReplacedPartitions(Seq("hr"))
            }
          }
        }
      }
    }
  }

  test("column-update ReplaceData: updated and copied rows reach writeColumnUpdate with " +
    "metadata") {
    createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "hr" }
        |{ "pk": 3, "salary": 300, "bonus": 30, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")

    checkLastColumnUpdateWriteLog(
      writeColumnUpdateWithMetadataLogEntry(metadata = Row("hr", null), data = Row(1, "hr", -1)),
      writeColumnUpdateWithMetadataLogEntry(metadata = Row("hr", 1), data = Row(2, "hr", 200)))
  }

  test("column-update ReplaceData: without metadata, rows reach writeColumnUpdate(record)") {
    createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "hr" }
        |{ "pk": 3, "salary": 300, "bonus": 30, "dep": "software" }
        |""".stripMargin,
      java.util.Map.of("column-update-cow", "true", "no-metadata", "true"))

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")

    checkLastColumnUpdateWriteLog(
      writeColumnUpdateLogEntry(data = Row(1, "hr", -1)),
      writeColumnUpdateLogEntry(data = Row(2, "hr", 200)))
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, 10, "hr") :: Row(2, 200, 20, "hr") :: Row(3, 300, 30, "software") :: Nil)
  }

  test("column-update ReplaceData: a writer overriding only writeColumnUpdate(record) " +
    "receives rows with metadata") {
    createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "hr" }
        |""".stripMargin,
      java.util.Map.of("column-update-cow", "true", "column-update-cow-record-only-writer", "true"))

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")

    checkLastColumnUpdateWriteLog(
      writeColumnUpdateLogEntry(data = Row(1, "hr", -1)),
      writeColumnUpdateLogEntry(data = Row(2, "hr", 200)))
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, 10, "hr") :: Row(2, 200, 20, "hr") :: Nil)
  }

  test("column-update ReplaceData: without metadata, column update schema is the declared " +
    "layout") {
    createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "hr" }
        |""".stripMargin,
      java.util.Map.of("column-update-cow", "true", "no-metadata", "true"))

    sql(s"UPDATE $tableNameAsString SET salary = salary + bonus WHERE pk = 1")

    checkLastWriteInfo(
      expectedColumnUpdateSchema = Some(StructType(Seq(PK_FIELD, DEP_FIELD,
        StructField("salary", IntegerType, nullable = true)))))
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 110, 10, "hr") :: Row(2, 200, 20, "hr") :: Nil)
  }

  test("column-update ReplaceData: connector missing writeColumnUpdate override is rejected") {
    createAndInitTableReplaceDataNoWriteColumnUpdate("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")
      },
      condition = "DATA_SOURCE_WRITE_COLUMN_UPDATE_NOT_IMPLEMENTED",
      parameters = Map(
        "class" -> "org.apache.spark.sql.connector.catalog.NoWriteColumnUpdateOverrideWriter"))
  }

  private def createAndInitTableReplaceDataNoWriteColumnUpdate(
      schemaString: String, jsonData: String): Unit = {
    createAndInitTable(schemaString, jsonData,
      java.util.Map.of("column-update-cow-no-write-column-update", "true"))
  }

  // ---------------------------------------------------------------------------
  // Write relation tests: verify that only the write relation is narrowed to the
  // declared columns, on both copy-on-write plan shapes.
  // ---------------------------------------------------------------------------

  private val reqAttrsOperationClass =
    classOf[InMemoryRowLevelOperationTable#PartitionBasedColumnUpdateOperationWithReqAttrs].getName

  // t(pk, id, dep, salary, bonus, extra) with CHECK (extra > 0), partitioned by dep
  private def createAndInitTableWithReqAttrs(reqAttrs: String): Unit = {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING, salary INT, bonus INT, extra INT",
      """{ "pk": 1, "id": 1, "dep": "hr", "salary": 100, "bonus": 10, "extra": 1 }
        |{ "pk": 2, "id": 2, "dep": "hr", "salary": 200, "bonus": 20, "extra": 1 }
        |{ "pk": 3, "id": 3, "dep": "software", "salary": 300, "bonus": 30, "extra": 1 }
        |""".stripMargin,
      java.util.Map.of("column-update-cow-req-attrs", reqAttrs))
    sql(s"ALTER TABLE $tableNameAsString ADD CONSTRAINT positive_extra CHECK (extra > 0)")
  }

  test("column-update ReplaceData: write relation has the declared columns in declared order") {
    createAndInitTableWithReqAttrs("salary,pk")

    val write = executeAndKeepAnalyzedWrite {
      sql(s"UPDATE $tableNameAsString SET salary = salary + bonus WHERE dep = 'hr'")
    }

    assert(!write.query.exists(_.isInstanceOf[Union]))
    checkColumnUpdateWrite(write, "salary", "pk")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 1, "hr", 110, 10, 1) ::
      Row(2, 2, "hr", 220, 20, 1) ::
      Row(3, 3, "software", 300, 30, 1) :: Nil)
  }

  test("column-update ReplaceData: subquery write relation has the declared columns") {
    withTempView("deps") {
      createAndInitTableWithReqAttrs("pk,salary")
      import testImplicits._
      Seq("hr").toDF("dep").createOrReplaceTempView("deps")

      val write = executeAndKeepAnalyzedWrite {
        sql(s"UPDATE $tableNameAsString SET salary = salary + bonus " +
          s"WHERE dep IN (SELECT dep FROM deps)")
      }

      assert(write.query.exists(_.isInstanceOf[Union]))
      checkColumnUpdateWrite(write, "pk", "salary")
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
        Row(1, 1, "hr", 110, 10, 1) ::
        Row(2, 2, "hr", 220, 20, 1) ::
        Row(3, 3, "software", 300, 30, 1) :: Nil)
    }
  }

  test("column-update ReplaceData: runtime group filtering on an undeclared condition column") {
    withTempView("matched_ids") {
      // `dep` is declared because the scan filters groups at runtime by the partition column
      createAndInitTableWithReqAttrs("pk,dep,salary")
      import testImplicits._
      Seq(1, 100).toDF().createOrReplaceTempView("matched_ids")

      // `id` is neither declared nor in the write relation; the group filter must still map it
      // onto the second read of the table that the subquery rewrite adds
      executeAndCheckScans(
        s"UPDATE $tableNameAsString SET salary = salary + bonus " +
          s"WHERE id IN (SELECT * FROM matched_ids)",
        primaryScanSchema = "pk INT, id INT, dep STRING, salary INT, bonus INT, extra INT, " +
          "_partition STRING, index INT",
        groupFilterScanSchema = Some("id INT, dep STRING"))

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
        Row(1, 1, "hr", 110, 10, 1) ::
        Row(2, 2, "hr", 200, 20, 1) ::
        Row(3, 3, "software", 300, 30, 1) :: Nil)
      checkReplacedPartitions(Seq("hr"))
    }
  }

  test("column-update ReplaceData: scan reads only the referenced columns") {
    createAndInitTableWithReqAttrs("pk,salary")

    sql(s"UPDATE $tableNameAsString SET salary = salary + bonus WHERE dep = 'hr'")

    // `id` is neither declared nor referenced; `dep`, `bonus` and `extra` are read for the
    // condition, the assignment and the CHECK constraint
    checkLastScanSchema(
      "pk INT, dep STRING, salary INT, bonus INT, extra INT, _partition STRING, index INT")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 1, "hr", 110, 10, 1) ::
      Row(2, 2, "hr", 220, 20, 1) ::
      Row(3, 3, "software", 300, 30, 1) :: Nil)
  }

  test("column-update ReplaceData: subquery scan reads only the referenced columns") {
    withTempView("deps") {
      createAndInitTableWithReqAttrs("pk,salary")
      import testImplicits._
      Seq("hr").toDF("dep").createOrReplaceTempView("deps")

      sql(s"UPDATE $tableNameAsString SET salary = salary + bonus " +
        s"WHERE dep IN (SELECT dep FROM deps)")

      checkLastScanSchema(
        "pk INT, dep STRING, salary INT, bonus INT, extra INT, _partition STRING, index INT")
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
        Row(1, 1, "hr", 110, 10, 1) ::
        Row(2, 2, "hr", 220, 20, 1) ::
        Row(3, 3, "software", 300, 30, 1) :: Nil)
    }
  }

  test("column-update ReplaceData: unread partition column skips runtime group filtering") {
    withTempView("matched_ids") {
      createAndInitTableWithReqAttrs("pk,salary")
      import testImplicits._
      Seq(1, 100).toDF().createOrReplaceTempView("matched_ids")

      // `dep` is neither declared nor referenced, so the scan doesn't read it and doesn't
      // report it from filterAttributes(); every group is read and replaced
      executeAndCheckScans(
        s"UPDATE $tableNameAsString SET salary = salary + bonus " +
          s"WHERE id IN (SELECT * FROM matched_ids)",
        primaryScanSchema = "pk INT, id INT, salary INT, bonus INT, extra INT, " +
          "_partition STRING, index INT",
        groupFilterScanSchema = None)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
        Row(1, 1, "hr", 110, 10, 1) ::
        Row(2, 2, "hr", 200, 20, 1) ::
        Row(3, 3, "software", 300, 30, 1) :: Nil)
      checkReplacedPartitions(Seq("hr", "software"))
    }
  }

  test("column-update ReplaceData: runtime filter attribute the scan doesn't read is rejected") {
    withTempView("matched_ids") {
      createAndInitTable("pk INT NOT NULL, id INT, dep STRING, salary INT",
        """{ "pk": 1, "id": 1, "dep": "hr", "salary": 100 }
          |""".stripMargin,
        java.util.Map.of(
          "column-update-cow-req-attrs", "pk,salary",
          "column-update-cow-unread-filter-attrs", "true"))
      import testImplicits._
      Seq(1).toDF().createOrReplaceTempView("matched_ids")

      val ex = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE id IN (SELECT * FROM matched_ids)")
      }
      assert(ex.getCondition == "DATA_SOURCE_INVALID_RUNTIME_FILTER_ATTRIBUTE.CANNOT_RESOLVE")
      assert(ex.getMessageParameters.get("attribute") == "`dep`")
      assert(ex.getMessageParameters.get("method") == "filterAttributes()")
    }
  }

  test("column-update ReplaceData: a connector without column updates reads every column") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin,
      java.util.Map.of[String, String]())

    sql(s"UPDATE $tableNameAsString SET salary = 0")

    checkLastScanSchema("pk INT, salary INT, dep STRING, _partition STRING, index INT")
    checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(1, 0, "hr") :: Nil)
  }

  test("column-update ReplaceData: undeclared distribution column is rejected") {
    // without metadata, the write clusters by `dep`
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin,
      java.util.Map.of("column-update-cow-req-attrs", "pk,salary", "no-metadata", "true"))

    val excludeColumnPruning = SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ColumnPruning.ruleName
    for {
      cond <- Seq("pk = 1", "dep = 'hr'")
      confs <- Seq(Nil, Seq(excludeColumnPruning))
    } withSQLConf(confs: _*) {
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE $cond")
        },
        condition = "COLUMN_UPDATE_UNDECLARED_WRITE_REQUIREMENT_COLUMNS",
        parameters = Map("connector" -> reqAttrsOperationClass, "columns" -> "[dep]"))
    }
  }

  test("column-update ReplaceData: excluding ColumnPruning keeps the write narrow") {
    createAndInitTableWithReqAttrs("pk,salary")

    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ColumnPruning.ruleName) {
      sql(s"UPDATE $tableNameAsString SET salary = salary + bonus WHERE dep = 'hr'")
    }

    assert(table.lastWriteInfo.columnUpdateSchema.get.fieldNames.toSeq == Seq("pk", "salary"))
    checkLastScanSchema("pk INT, id INT, dep STRING, salary INT, bonus INT, extra INT, " +
      "_partition STRING, index INT")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 1, "hr", 110, 10, 1) ::
      Row(2, 2, "hr", 220, 20, 1) ::
      Row(3, 3, "software", 300, 30, 1) :: Nil)
  }

  test("column-update ReplaceData: other operations read every column") {
    createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "hr" }
        |""".stripMargin,
      java.util.Map.of())
    val fullScanSchema = "pk INT, salary INT, bonus INT, dep STRING, _partition STRING, index INT"

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")
    checkLastScanSchema(fullScanSchema)

    sql(s"DELETE FROM $tableNameAsString WHERE pk > 1")
    checkLastScanSchema(fullScanSchema)

    withTempView("source") {
      import testImplicits._
      Seq((1, 150)).toDF("pk", "salary").createOrReplaceTempView("source")
      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN UPDATE SET salary = s.salary
           |""".stripMargin)
      checkLastScanSchema(fullScanSchema)
    }

    checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(1, 150, 10, "hr") :: Nil)
  }

  test("column-update ReplaceData: DELETE and MERGE on a column-update table read every column") {
    createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "hr" }
        |""".stripMargin)
    val fullScanSchema = "pk INT, salary INT, bonus INT, dep STRING, _partition STRING, index INT"

    sql(s"DELETE FROM $tableNameAsString WHERE pk > 1")
    checkLastScanSchema(fullScanSchema)

    withTempView("source") {
      import testImplicits._
      Seq((1, 150)).toDF("pk", "salary").createOrReplaceTempView("source")
      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN UPDATE SET salary = s.salary
           |""".stripMargin)
      checkLastScanSchema(fullScanSchema)
    }

    checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(1, 150, 10, "hr") :: Nil)
  }

  test("column-update ReplaceData: assigned column outside requiredDataAttributes is rejected") {
    createAndInitTableWithReqAttrs("pk,salary")

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"UPDATE $tableNameAsString SET bonus = 0 WHERE dep = 'hr'")
      },
      condition = "COLUMN_UPDATE_REQUIRED_DATA_ATTRIBUTES_MISSING_UPDATED_COLUMNS",
      parameters = Map(
        "connector" -> reqAttrsOperationClass,
        "missingColumns" -> "[bonus]"))
  }

  test("column-update ReplaceData: assigned column outside requiredDataAttributes is rejected " +
    "on the subquery path") {
    withTempView("deps") {
      createAndInitTableWithReqAttrs("pk,salary")
      import testImplicits._
      Seq("hr").toDF("dep").createOrReplaceTempView("deps")

      checkError(
        exception = intercept[AnalysisException] {
          sql(s"UPDATE $tableNameAsString SET bonus = 0 WHERE dep IN (SELECT dep FROM deps)")
        },
        condition = "COLUMN_UPDATE_REQUIRED_DATA_ATTRIBUTES_MISSING_UPDATED_COLUMNS",
        parameters = Map(
          "connector" -> reqAttrsOperationClass,
          "missingColumns" -> "[bonus]"))
    }
  }

  test("column-update ReplaceData: analyzed plan shows the declared columns as the write target") {
    createAndInitTableWithReqAttrs("pk,salary")

    val write = executeAndKeepAnalyzedWrite {
      sql(s"UPDATE $tableNameAsString SET salary = salary + bonus WHERE dep = 'hr'")
    }

    val header = write.simpleString(Int.MaxValue)
    assert(header.matches("""ReplaceData RelationV2\[pk#\d+, salary#\d+\] .*"""), header)
  }

  test("column-update ReplaceData: column update schema nullability covers UPDATE and COPY " +
    "rows") {
    createAndInitTable("pk INT NOT NULL, salary INT NOT NULL, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": null, "dep": "hr" }
        |""".stripMargin,
      java.util.Map.of("column-update-cow-req-attrs", "pk,salary,bonus"))

    sql(s"UPDATE $tableNameAsString SET salary = -1, bonus = 5 WHERE pk = 1")

    // UPDATE rows assign non-null literals, but the COPY row for pk = 2 carries a null bonus.
    checkLastWriteInfo(
      expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))),
      expectedColumnUpdateSchema = Some(StructType(Seq(
        StructField("pk", IntegerType, nullable = false),
        StructField("salary", IntegerType, nullable = false),
        StructField("bonus", IntegerType, nullable = true)))))
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, 5, "hr") :: Row(2, 200, null, "hr") :: Nil)
  }

  test("column-update ReplaceData: other operations keep a full-width write relation") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin,
      java.util.Map.of())

    val write = executeAndKeepAnalyzedWrite {
      sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")
    }

    assert(write.table.output == write.originalTable.output)
  }


  test("column-update ReplaceData: nested struct field update narrows to root struct column") {
    createAndInitTable("pk INT NOT NULL, s STRUCT<c1: INT, c2: INT>, dep STRING",
      """{ "pk": 1, "s": { "c1": 1, "c2": 2 }, "dep": "hr" }
        |{ "pk": 2, "s": { "c1": 3, "c2": 4 }, "dep": "hr" }
        |""".stripMargin)

    // `SET s.c1 = -1` is aligned into a whole-struct assignment, so updatedColumns is [s]
    // at root granularity.
    sql(s"UPDATE $tableNameAsString SET s.c1 = -1 WHERE pk = 1")

    checkLastUpdatedColumns("s")

    val columnUpdateSchema = table.lastWriteInfo.columnUpdateSchema().get()
    assert(columnUpdateSchema.fieldNames.contains("s"),
      s"s must be in column update schema: $columnUpdateSchema")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, Row(-1, 2), "hr") :: Row(2, Row(3, 4), "hr") :: Nil)
  }

  test("column-update ReplaceData: nested field identity update reports root struct as updated") {
    createAndInitTable("pk INT NOT NULL, s STRUCT<c1: INT, c2: INT>, dep STRING",
      """{ "pk": 1, "s": { "c1": 1, "c2": 2 }, "dep": "hr" }
        |""".stripMargin)

    // `SET s.c1 = s.c1` is semantically a no-op on the nested field, but the analyzer's
    // AssignmentUtils rewrites nested field assignments into whole-struct rebuilds:
    //     Assignment(s, named_struct("c1", s.c1, "c2", s.c2))
    // `isIdentityAssignment` operates at root-column granularity and does NOT recognize the
    // rebuilt struct as identity (its value is a `named_struct(...)`, not a plain Attribute
    // matching the key). So `s` is reported in updatedColumns even though the values don't
    // change. This documents the root-column granularity of `RowLevelOperationInfo` for the
    // CoW path.
    sql(s"UPDATE $tableNameAsString SET s.c1 = s.c1 WHERE pk = 1")

    checkLastUpdatedColumns("s")

    // Data correctness: the struct is rewritten but with equal values, so rows are unchanged.
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, Row(1, 2), "hr") :: Nil)
  }

  test("column-update ReplaceData: scan reads an unreferenced CHECK constraint column") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING, extra INT",
      """{ "pk": 1, "id": 1, "dep": "hr", "extra": 5 }
        |{ "pk": 2, "id": 2, "dep": "software", "extra": 5 }
        |""".stripMargin)
    sql(s"ALTER TABLE $tableNameAsString ADD CONSTRAINT positive_extra CHECK (extra > 0)")

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")

    checkLastScanIncludes("extra")

    val columnUpdateSchema = table.lastWriteInfo.columnUpdateSchema().get()
    assert(!columnUpdateSchema.fieldNames.contains("extra"),
      s"extra must not be in column update schema: $columnUpdateSchema")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, -1, "hr", 5) :: Row(2, 2, "software", 5) :: Nil)
  }

  test("column-update ReplaceData: CHECK constraint is still enforced on a narrow write") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING, extra INT",
      """{ "pk": 1, "id": 1, "dep": "hr", "extra": 5 }
        |""".stripMargin)
    sql(s"ALTER TABLE $tableNameAsString ADD CONSTRAINT positive_id CHECK (id > 0)")

    val ex = intercept[SparkRuntimeException] {
      sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")
    }
    assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION",
      s"expected CHECK_CONSTRAINT_VIOLATION but got: ${ex.getCondition}")
  }

  test("column-update ReplaceData: CHECK constraint on a nested field narrows correctly") {
    createAndInitTable("pk INT NOT NULL, id INT, s STRUCT<c1: INT, c2: INT>, dep STRING",
      """{ "pk": 1, "id": 1, "s": { "c1": 1, "c2": 2 }, "dep": "hr" }
        |""".stripMargin)
    sql(s"ALTER TABLE $tableNameAsString ADD CONSTRAINT positive_c1 CHECK (s.c1 > 0)")

    sql(s"UPDATE $tableNameAsString SET id = -1 WHERE pk = 1")
    checkLastScanIncludes("s")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, -1, Row(1, 2), "hr") :: Nil)

    val ex = intercept[SparkRuntimeException] {
      sql(s"UPDATE $tableNameAsString SET s.c1 = -1 WHERE pk = 1")
    }
    assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION",
      s"expected CHECK_CONSTRAINT_VIOLATION but got: ${ex.getCondition}")
  }


  test("column-update ReplaceData: undeclared, differently-cased columns in the RHS and WHERE " +
    "clause are read with the table's own spelling") {
    createAndInitTable(
      "pk INT NOT NULL, salary INT, bonus INT, extra INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "extra": 5, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": 20, "extra": 5, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = salary + BONUS WHERE pk = 1 AND EXTRA > 3")

    checkLastScanSchema(
      "pk INT, salary INT, bonus INT, extra INT, dep STRING, _partition STRING, index INT")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
      Row(1, 110, 10, 5, "hr") :: Row(2, 200, 20, 5, "hr") :: Nil)
  }

  test("column-update ReplaceData: scan that returns unrequested columns still writes narrow " +
    "rows") {
    createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |""".stripMargin,
      java.util.Map.of(
        "column-update-cow", "true",
        InMemoryBaseTable.SIMULATE_PARTIAL_COLUMN_PRUNING, "true"))

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")

    checkLastScanIncludes("bonus")
    val columnUpdateSchema = table.lastWriteInfo.columnUpdateSchema().get()
    assert(!columnUpdateSchema.fieldNames.contains("bonus"),
      s"bonus must not be in column update schema: $columnUpdateSchema")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, -1, 10, "hr") :: Nil)
  }

  test("column-update ReplaceData: row-level DELETE falls back to full-width rows") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "hr" }
        |""".stripMargin)

    sql(s"DELETE FROM $tableNameAsString WHERE pk < 2")

    assert(!table.lastWriteInfo.columnUpdateSchema().isPresent,
      s"DELETE must not report a column update schema: " +
        s"${table.lastWriteInfo.columnUpdateSchema()}")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, 200, "hr") :: Nil)
  }

  test("column-update ReplaceData: MERGE falls back to full-width rows") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    withTempView("source") {
      import testImplicits._
      Seq((1, 150, "hr"), (3, 300, "software")).toDF("pk", "salary", "dep")
        .createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN UPDATE SET salary = s.salary
           |WHEN NOT MATCHED THEN INSERT *
           |""".stripMargin)

      assert(!table.lastWriteInfo.columnUpdateSchema().isPresent,
        s"MERGE must not report a column update schema: " +
          s"${table.lastWriteInfo.columnUpdateSchema()}")
      checkLastUpdatedColumns()
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
        Row(1, 150, "hr") :: Row(2, 200, "software") :: Row(3, 300, "software") :: Nil)
    }
  }

  test("column-update ReplaceData: dynamic options reach the connector") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |""".stripMargin)

    checkRowLevelOperationOptions(
      sql(s"UPDATE $tableNameAsString WITH " +
        s"(`load-option` = 'load-value', `write-option` = 'write-value') " +
        s"SET id = -1 WHERE pk = 1"),
      "load-option" -> "load-value",
      "write-option" -> "write-value")
  }
}
