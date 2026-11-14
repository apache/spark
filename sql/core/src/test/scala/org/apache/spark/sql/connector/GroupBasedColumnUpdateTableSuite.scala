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

import org.apache.spark.{SparkException, SparkUnsupportedOperationException}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.optimizer.ColumnPruning
import org.apache.spark.sql.catalyst.plans.logical.{ReplaceData, Union}
import org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTable
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.UPDATE
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.SupportsRuntimeCatalystFiltering
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

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

  // the scan schema of every column of t(pk, salary, bonus, dep) and its metadata columns
  private val fullScanSchema = s"pk INT, salary INT, bonus INT, dep STRING$scanMetadataColumns"

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
    Seq(
      "pk IN (1, 100)",
      "pk IN (1, 20) AND dep IN (SELECT * FROM updated_dep)"
    ).foreach { condition =>
      withTable(tableNameAsString) {
        withTempView("updated_dep") {
          createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
            """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
              |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "hr" }
              |{ "pk": 3, "salary": 300, "bonus": 30, "dep": "software" }
              |""".stripMargin)
          import testImplicits._
          Seq(Some("hr"), Some("it")).toDF().createOrReplaceTempView("updated_dep")

          val write = executeAndKeepAnalyzedWrite {
            sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE $condition")
          }

          assert(write.query.exists(_.isInstanceOf[Union]) == condition.contains("SELECT"))
          checkLastColumnUpdateWriteLog(
            writeColumnUpdateWithMetadataLogEntry(
              metadata = Row("hr", null), data = Row(1, "hr", -1)),
            writeColumnUpdateWithMetadataLogEntry(
              metadata = Row("hr", 1), data = Row(2, "hr", 200)))
          checkAnswer(
            sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
            Row(1, -1, 10, "hr") :: Row(2, 200, 20, "hr") :: Row(3, 300, 30, "software") :: Nil)
        }
      }
    }
  }

  test("column-update ReplaceData: rows reach writeColumnUpdate(record) without metadata, and " +
    "through the default writeColumnUpdate(metadata, record) with metadata") {
    Seq(
      tablePropsWith(extraTableProps, "no-metadata" -> "true") -> None,
      tablePropsWith(extraTableProps, "column-update-cow-record-only-writer" -> "true") ->
        Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE)))
    ).foreach { case (props, metadataSchema) =>
      withTable(tableNameAsString) {
        createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
          """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "hr" }
            |{ "pk": 3, "salary": 300, "bonus": 30, "dep": "software" }
            |""".stripMargin,
          props)

        sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")

        checkLastWriteInfo(
          expectedMetadataSchema = metadataSchema,
          expectedColumnUpdateSchema = Some(StructType(Seq(
            PK_FIELD, DEP_FIELD, StructField("salary", IntegerType, nullable = true)))))
        checkLastColumnUpdateWriteLog(
          writeColumnUpdateLogEntry(data = Row(1, "hr", -1)),
          writeColumnUpdateLogEntry(data = Row(2, "hr", 200)))
        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
          Row(1, -1, 10, "hr") :: Row(2, 200, 20, "hr") :: Row(3, 300, 30, "software") :: Nil)
      }
    }
  }

  test("column-update ReplaceData: connector missing writeColumnUpdate override is rejected") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin,
      tablePropsWith(extraTableProps, "column-update-cow-no-write-column-update" -> "true"))

    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")
      },
      condition = "DATA_SOURCE_WRITE_COLUMN_UPDATE_NOT_IMPLEMENTED",
      parameters = Map(
        "class" -> "org.apache.spark.sql.connector.catalog.BufferWriter"))
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

  test("column-update ReplaceData: runtime group filtering on a metadata column with a " +
    "subquery") {
    withTempView("updated_pk") {
      createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
        """{ "pk": 1, "salary": 300, "bonus": 7, "dep": "hr" }
          |{ "pk": 2, "salary": 150, "bonus": 7, "dep": "software" }
          |{ "pk": 3, "salary": 120, "bonus": 7, "dep": "hr" }
          |""".stripMargin)
      import testImplicits._
      Seq(1, 2).toDF("pk").createOrReplaceTempView("updated_pk")

      // the group filter of each read of the table must refer to the metadata column of that read
      sql(s"UPDATE $tableNameAsString SET salary = -1 " +
        "WHERE _partition = 'hr' AND pk IN (SELECT pk FROM updated_pk)")

      checkLastColumnUpdateSchema("pk", "dep", "salary")
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
        Row(1, -1, 7, "hr") :: Row(2, 150, 7, "software") :: Row(3, 120, 7, "hr") :: Nil)
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

  test("column-update ReplaceData: an IN subquery on the partition column with the group " +
    "filter disabled") {
    withSQLConf(SQLConf.RUNTIME_ROW_LEVEL_OPERATION_GROUP_FILTER_ENABLED.key -> "false") {
      withTempView("updated_dep") {
        createAndInitTableWithProps(reqAttrsTableProps("pk,dep,salary"))
        import testImplicits._
        Seq("hr").toDF().createOrReplaceTempView("updated_dep")

        // dynamic partition pruning of the read of the updated rows must not prune the scan
        // that the read of the copied rows shares
        sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep IN (SELECT * FROM updated_dep)")

        checkLastColumnUpdateSchema("pk", "dep", "salary")
        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
          Row(1, 1, "hr", -1, 10, 1) ::
          Row(2, 2, "hr", -1, 20, 1) ::
          Row(3, 3, "software", 300, 30, 1) :: Nil)
        checkReplacedPartitions(Seq("hr", "software"))
      }
    }
  }

  test("column-update ReplaceData: a NOT EXISTS subquery on the partition column") {
    withTempView("kept_dep") {
      createAndInitTableWithProps(reqAttrsTableProps("pk,dep,salary"))
      import testImplicits._
      Seq("hr").toDF("d").createOrReplaceTempView("kept_dep")

      sql(s"UPDATE $tableNameAsString t SET salary = -1 " +
        "WHERE NOT EXISTS (SELECT 1 FROM kept_dep WHERE d = t.dep)")

      checkLastColumnUpdateSchema("pk", "dep", "salary")
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
        Row(1, 1, "hr", 100, 10, 1) ::
        Row(2, 2, "hr", 200, 20, 1) ::
        Row(3, 3, "software", -1, 30, 1) :: Nil)
      checkReplacedPartitions(Seq("software"))
    }
  }

  test("column-update ReplaceData: a scalar subquery on the partition column with catalyst " +
    "runtime filtering") {
    withTempView("src") {
      createAndInitTableWithProps(tablePropsWith(reqAttrsTableProps("pk,dep,salary"),
        "use-catalyst-runtime-filtering" -> "true"))
      import testImplicits._
      Seq("hr").toDF("d").createOrReplaceTempView("src")

      sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = (SELECT max(d) FROM src)")

      checkLastColumnUpdateSchema("pk", "dep", "salary")
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
        Row(1, 1, "hr", -1, 10, 1) ::
        Row(2, 2, "hr", -1, 20, 1) ::
        Row(3, 3, "software", 300, 30, 1) :: Nil)
      checkReplacedPartitions(Seq("hr"))
    }
  }

  test("column-update ReplaceData: a stale projection ordinal fails the analysis check") {
    createAndInitTableWithProps(reqAttrsTableProps("pk,dep,salary"))
    val write = executeAndKeepAnalyzedWrite {
      sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1")
    }.asInstanceOf[ReplaceData]
    val numColumns = write.query.output.length
    val stale = write.copy(projections = write.projections.remapOrdinals(_ => numColumns))

    val message =
      s"A projection of ReplaceData reads ordinal $numColumns of a query with $numColumns columns."

    assert(stale.treeString.contains("ReplaceData"))
    checkError(
      exception = intercept[SparkException](spark.sessionState.analyzer.checkAnalysis(stale)),
      condition = "INTERNAL_ERROR",
      parameters = Map("message" -> message))
  }

  test("column-update ReplaceData: runtime filter attribute the scan doesn't read is rejected") {
    Seq(false, true).foreach { catalystFiltering =>
      withTable(tableNameAsString) {
        withTempView("matched_ids") {
          createAndInitTable("pk INT NOT NULL, id INT, dep STRING, salary INT",
            """{ "pk": 1, "id": 1, "dep": "hr", "salary": 100 }
              |""".stripMargin,
            tablePropsWith(reqAttrsTableProps("pk,salary"),
              "column-update-cow-unread-filter-attrs" -> "true",
              "use-catalyst-runtime-filtering" -> catalystFiltering.toString))
          val t = table
          val scan = new t.PartitionBasedColumnUpdateOperation(UPDATE, Nil)
            .newScanBuilder(CaseInsensitiveStringMap.empty()).build()
          assert(scan.isInstanceOf[SupportsRuntimeCatalystFiltering] == catalystFiltering)
          import testImplicits._
          Seq(1).toDF().createOrReplaceTempView("matched_ids")

          checkError(
            exception = intercept[AnalysisException] {
              sql(s"UPDATE $tableNameAsString SET salary = -1 " +
                "WHERE id IN (SELECT * FROM matched_ids)")
            },
            condition = "DATA_SOURCE_INVALID_RUNTIME_FILTER_ATTRIBUTE.CANNOT_RESOLVE",
            sqlState = Some("KD000"),
            parameters = Map(
              "attribute" -> Pattern.quote("`dep`"),
              "method" -> Pattern.quote("filterAttributes()"),
              "scanClass" -> (Pattern.quote(classOf[
                InMemoryRowLevelOperationTable#PartitionBasedColumnUpdateOperation].getName) +
                """\$\$anon\$\d+\$\$anon\$\d+"""),
              "relationOutput" -> Pattern.quote("\"STRUCT<pk: INT NOT NULL, id: INT, " +
                "salary: INT, _partition: STRING NOT NULL COMMENT 'Partition key used to " +
                "store the row', index: INT NOT NULL COMMENT 'Metadata column used to " +
                "conflict with a data column'>\"")),
            matchPVals = true)
        }
      }
    }
  }

  test("column-update ReplaceData: undeclared distribution column is rejected") {
    // without metadata, the write clusters by `dep`
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |""".stripMargin,
      tablePropsWith(reqAttrsTableProps("pk,salary"), "no-metadata" -> "true"))

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
        parameters = Map("connector" -> columnUpdateOperationClass, "columns" -> "[`dep`]"))
    }
  }

  test("column-update ReplaceData: the lookup column `pk` declared in another case") {
    Seq(
      ("salary = -1", Some(Seq("PK", "salary", "dep")), Row(1, -1, "hr")),
      // assigning the lookup column falls back to full width
      ("PK = 10", None, Row(10, 100, "hr"))
    ).foreach { case (assignment, writeColumns, updatedRow) =>
      withTable(tableNameAsString) {
        createAndInitTable("PK INT NOT NULL, salary INT, dep STRING",
          """{ "PK": 1, "salary": 100, "dep": "hr" }
            |{ "PK": 2, "salary": 200, "dep": "software" }
            |""".stripMargin,
          reqAttrsTableProps("pk,salary,dep"))

        sql(s"UPDATE $tableNameAsString SET $assignment WHERE PK = 1")

        writeColumns match {
          case Some(columns) => checkLastColumnUpdateSchema(columns: _*)
          case None => checkLastWriteIsFullWidth()
        }
        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          updatedRow :: Row(2, 200, "software") :: Nil)
      }
    }
  }

  test("column-update ReplaceData: an UPDATE without the mix-in reads every column") {
    createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |""".stripMargin,
      fullWidthTableProps)

    executeAndCheckScan(s"UPDATE $tableNameAsString SET salary = -1 WHERE pk = 1", fullScanSchema)

    checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(1, -1, 10, "hr") :: Nil)
  }

  test("column-update ReplaceData: DELETE and MERGE read every column") {
    Seq(fullWidthTableProps, extraTableProps).foreach { props =>
      withTable(tableNameAsString) {
        createAndInitTable("pk INT NOT NULL, salary INT, bonus INT, dep STRING",
          """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "bonus": 20, "dep": "hr" }
            |""".stripMargin,
          props)

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

  test("column-update ReplaceData: column update schema nullability covers UPDATE and COPY " +
    "rows") {
    createAndInitTable("pk INT NOT NULL, salary INT NOT NULL, bonus INT, dep STRING",
      """{ "pk": 1, "salary": 100, "bonus": 10, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "bonus": null, "dep": "hr" }
        |""".stripMargin,
      reqAttrsTableProps("pk,salary,bonus"))

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
