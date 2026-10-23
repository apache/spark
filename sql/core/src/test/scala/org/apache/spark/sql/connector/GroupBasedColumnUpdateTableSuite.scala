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

import java.util.regex.Pattern

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.optimizer.ColumnPruning
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTable
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, V2Writes}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class GroupBasedColumnUpdateTableSuite extends UpdateTableSuiteBase
  with ColumnUpdateTableSharedTests {

  override protected lazy val extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("column-update-cow", "true")
    props
  }

  override protected def reqAttrsTableProps(reqAttrs: String): java.util.Map[String, String] = {
    java.util.Map.of("column-update-cow-req-attrs", reqAttrs)
  }

  override protected def fullWidthTableProps: java.util.Map[String, String] = java.util.Map.of()

  test("column-update ReplaceData: runtime group filtering reads only the referenced columns") {
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

    checkLastWriteInfo(expectedColumnUpdateSchema = Some(StructType(Seq(
      PK_FIELD, DEP_FIELD, StructField("salary", IntegerType, nullable = true)))))
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

  test("column-update ReplaceData: connector missing writeColumnUpdate override is rejected") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin,
      java.util.Map.of(
        "column-update-cow", "true", "column-update-cow-no-write-column-update", "true"))

    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")
      },
      condition = "DATA_SOURCE_WRITE_COLUMN_UPDATE_NOT_IMPLEMENTED",
      parameters = Map(
        "class" -> "org.apache.spark.sql.connector.catalog.NoWriteColumnUpdateOverrideWriter"))
  }

  test("column-update ReplaceData: the subquery plan narrows the write relation and the scan") {
    withTempView("deps") {
      createAndInitTableWithProps(reqAttrsTableProps("pk,salary"))
      import testImplicits._
      Seq("hr").toDF("dep").createOrReplaceTempView("deps")

      val write = executeAndKeepAnalyzedWrite {
        executeAndCheckScan(
          s"UPDATE $tableNameAsString SET salary = salary + bonus " +
            s"WHERE dep IN (SELECT dep FROM deps)",
          "pk INT, dep STRING, salary INT, bonus INT, extra INT, _partition STRING, index INT")
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
      createAndInitTableWithProps(reqAttrsTableProps("pk,dep,salary"))
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

  test("column-update ReplaceData: unread partition column skips runtime group filtering") {
    withTempView("matched_ids") {
      createAndInitTableWithProps(reqAttrsTableProps("pk,salary"))
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

      checkError(
        exception = intercept[AnalysisException] {
          sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE id IN (SELECT * FROM matched_ids)")
        },
        condition = "DATA_SOURCE_INVALID_RUNTIME_FILTER_ATTRIBUTE.CANNOT_RESOLVE",
        sqlState = Some("KD000"),
        parameters = Map(
          "attribute" -> Pattern.quote("`dep`"),
          "method" -> Pattern.quote("filterAttributes()"),
          "scanClass" -> (Pattern.quote(
            classOf[InMemoryRowLevelOperationTable#PartitionBasedColumnUpdateOperation].getName) +
            """\$\$anon\$\d+\$\$anon\$\d+"""),
          "relationOutput" -> Pattern.quote("\"STRUCT<pk: INT NOT NULL, id: INT, salary: INT, " +
            "_partition: STRING NOT NULL COMMENT 'Partition key used to store the row', " +
            "index: INT NOT NULL COMMENT 'Metadata column used to conflict with a data " +
            "column'>\"")),
        matchPVals = true)
    }
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
        parameters = Map("connector" -> reqAttrsOperationClass, "columns" -> "[`dep`]"))
    }
  }

  test("column-update ReplaceData: UPDATE without column updates, DELETE and MERGE read every " +
    "column") {
    val fullScanSchema = "pk INT, salary INT, bonus INT, dep STRING, _partition STRING, index INT"
    Seq(fullWidthTableProps, extraTableProps).foreach { props =>
      withTable(tableNameAsString) {
        createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
          """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "hr" }
            |""".stripMargin,
          props)

        if (props == fullWidthTableProps) {
          executeAndCheckScan(
            s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1", fullScanSchema)
        }
        executeAndCheckScan(s"DELETE FROM $tableNameAsString WHERE pk > 1", fullScanSchema)
        withTempView("source") {
          import testImplicits._
          Seq((1, 150)).toDF("pk", "salary").createOrReplaceTempView("source")
          executeAndCheckScan(
            s"""MERGE INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN MATCHED THEN UPDATE SET salary = s.salary
               |""".stripMargin,
            fullScanSchema)
        }

        checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(1, 150, 10, "hr") :: Nil)
      }
    }
  }

  test("column-update ReplaceData: an operation rebuilt with withTable keeps the updated columns") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin)

    val write = executeAndKeepAnalyzedWrite {
      sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")
    }
    val relation = write.table.asInstanceOf[DataSourceV2Relation]
    val operationTable = relation.table.asInstanceOf[RowLevelOperationTable]
    val t = table
    t.lastUpdatedColumns = null

    val rebuilt = operationTable.withTable(t)

    checkLastUpdatedColumns("salary")
    assert(rebuilt.operationInfo eq operationTable.operationInfo)
    val rebuiltQuery = write.query.transform {
      case r: DataSourceV2Relation if r.table eq operationTable => r.copy(table = rebuilt)
    }
    assert(rebuiltQuery.exists {
      case r: DataSourceV2Relation => r.table eq rebuilt
      case _ => false
    })
    V2Writes(write.withNewTable(relation.copy(table = rebuilt)).withNewQuery(rebuiltQuery))
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

}
