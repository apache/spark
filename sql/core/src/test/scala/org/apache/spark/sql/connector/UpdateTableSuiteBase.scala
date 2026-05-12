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

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.{sources, AnalysisException, Row}
import org.apache.spark.sql.connector.catalog.{Aborted, Column, ColumnDefaultValue, Committed, InMemoryTable, TableChange, TableInfo}
import org.apache.spark.sql.connector.expressions.{GeneralScalarExpression, LiteralValue}
import org.apache.spark.sql.connector.write.UpdateSummary
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType}

abstract class UpdateTableSuiteBase extends RowLevelOperationSuiteBase {

  import testImplicits._

  protected def deltaUpdate: Boolean = false

  protected def getUpdateSummary(): UpdateSummary = {
    val t = catalog.loadTable(ident).asInstanceOf[InMemoryTable]
    t.commits.last.writeSummary.get.asInstanceOf[UpdateSummary]
  }

  protected def checkUpdateMetrics(
      numUpdatedRows: Long,
      numCopiedRows: Long): Unit = {
    val summary = getUpdateSummary()
    assert(summary.numUpdatedRows() === numUpdatedRows,
      s"Expected numUpdatedRows=$numUpdatedRows, got ${summary.numUpdatedRows()}")
    val expectedCopied = if (deltaUpdate) 0L else numCopiedRows
    assert(summary.numCopiedRows() === expectedCopied,
      s"Expected numCopiedRows=$expectedCopied, got ${summary.numCopiedRows()}")
  }

  test("update table containing added column with default value") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    sql(s"ALTER TABLE $tableNameAsString ADD COLUMN txt STRING DEFAULT 'initial-text'")

    append("pk INT, salary INT, dep STRING, txt STRING",
      """{ "pk": 3, "salary": 300, "dep": "hr", "txt": "explicit-text" }
        |{ "pk": 4, "salary": 400, "dep": "software", "txt": "explicit-text" }
        |{ "pk": 5, "salary": 500, "dep": "hr" }
        |""".stripMargin)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, 100, "hr", "initial-text"),
        Row(2, 200, "software", "initial-text"),
        Row(3, 300, "hr", "explicit-text"),
        Row(4, 400, "software", "explicit-text"),
        Row(5, 500, "hr", null)))

    sql(s"ALTER TABLE $tableNameAsString ALTER COLUMN txt SET DEFAULT 'new-text'")

    sql(s"UPDATE $tableNameAsString SET txt = DEFAULT WHERE pk IN (2, 8, 11)")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, 100, "hr", "initial-text"),
        Row(2, 200, "software", "new-text"),
        Row(3, 300, "hr", "explicit-text"),
        Row(4, 400, "software", "explicit-text"),
        Row(5, 500, "hr", null)))

    checkUpdateMetrics(numUpdatedRows = 1, numCopiedRows = 1)
  }

  test("update table with expression-based default values") {
    val columns = Array(
      Column.create("pk", IntegerType),
      Column.create("salary", IntegerType),
      Column.create("dep", StringType))
    val tableInfo = new TableInfo.Builder().withColumns(columns).build()
    catalog.createTable(ident, tableInfo)

    append("pk INT, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    val addColumn = TableChange.addColumn(
      Array("value"),
      IntegerType,
      false, /* not nullable */
      null, /* no comment */
      null, /* no position */
      new ColumnDefaultValue(
        new GeneralScalarExpression(
          "+",
          Array(LiteralValue(100, IntegerType), LiteralValue(23, IntegerType))),
        LiteralValue(123, IntegerType)))
    catalog.alterTable(ident, addColumn)

    append("pk INT, salary INT, dep STRING, value INT",
      """{ "pk": 4, "salary": 400, "dep": "hr", "value": -4 }
        |{ "pk": 5, "salary": 500, "dep": "hr", "value": -5 }
        |""".stripMargin)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, 100, "hr", 123),
        Row(2, 200, "software", 123),
        Row(3, 300, "hr", 123),
        Row(4, 400, "hr", -4),
        Row(5, 500, "hr", -5)))

    sql(s"UPDATE $tableNameAsString SET value = DEFAULT WHERE pk >= 5")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, 100, "hr", 123),
        Row(2, 200, "software", 123),
        Row(3, 300, "hr", 123),
        Row(4, 400, "hr", -4),
        Row(5, 500, "hr", 123)))
  }

  test("EXPLAIN only update") {
    createAndInitTable("pk INT NOT NULL, dep STRING", """{ "pk": 1, "dep": "hr" }""")

    sql(s"EXPLAIN UPDATE $tableNameAsString SET dep = 'invalid' WHERE pk = 1")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, "hr") :: Nil)
  }

  test("update empty tables") {
    createTable("pk INT NOT NULL, salary INT, dep STRING")

    sql(s"UPDATE $tableNameAsString SET dep = 'invalid' WHERE salary <= 1")

    checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Nil)

    checkUpdateMetrics(numUpdatedRows = 0, numCopiedRows = 0)
  }

  test("update with basic filters") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET dep = 'invalid' WHERE salary <= 100")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, 100, "invalid") :: Row(2, 200, "software") :: Row(3, 300, "hr") :: Nil)

    checkUpdateMetrics(numUpdatedRows = 1, numCopiedRows = 1)
  }

  test("update with aliases") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString AS t SET t.salary = -1 WHERE t.salary <= 100 OR t.dep = 'hr'")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, -1, "hr") :: Row(2, 200, "software") :: Row(3, -1, "hr") :: Nil)

    checkUpdateMetrics(numUpdatedRows = 2, numCopiedRows = 0)
  }

  test("update aligns assignments") {
    createAndInitTable("pk INT NOT NULL, c1 INT, c2 INT, dep STRING",
      """{ "pk": 1, "c1": 11, "c2": 111, "dep": "hr" }
        |{ "pk": 2, "c1": 22, "c2": 222, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET `c2` = c2 - 2, c1 = `c1` - 1 WHERE pk <=> 1")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, 10, 109, "hr") :: Row(2, 22, 222, "hr") :: Nil)

    checkUpdateMetrics(numUpdatedRows = 1, numCopiedRows = 1)
  }

  test("update non-existing records") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "hardware" }
        |{ "pk": 3, "salary": null, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE salary > 1000")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, 100, "hr") :: Row(2, 200, "hardware") :: Row(3, null, "hr") :: Nil)

    checkUpdateMetrics(numUpdatedRows = 0, numCopiedRows = 0)
  }

  test("update with literal false condition") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "hardware" }
        |{ "pk": 3, "salary": null, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE false")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, 100, "hr") :: Row(2, 200, "hardware") :: Row(3, null, "hr") :: Nil)

    checkUpdateMetrics(numUpdatedRows = 0, numCopiedRows = 0)
  }

  test("update with literal true condition") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "hardware" }
        |{ "pk": 3, "salary": null, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE true")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, -1, "hr") :: Row(2, -1, "hardware") :: Row(3, -1, "hr") :: Nil)

    checkUpdateMetrics(numUpdatedRows = 3, numCopiedRows = 0)
  }

  test("update without condition") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "hardware" }
        |{ "pk": 3, "salary": null, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, -1, "hr") :: Row(2, -1, "hardware") :: Row(3, -1, "hr") :: Nil)

    checkUpdateMetrics(numUpdatedRows = 3, numCopiedRows = 0)
  }

  test("update with NULL conditions on partition columns") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": null }
        |{ "pk": 2, "salary": 200, "dep": "hr" }
        |{ "pk": 3, "salary": 300, "dep": "hardware" }
        |""".stripMargin)

    // should not update any rows as NULL is never equal to NULL
    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = NULL")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, 100, null) :: Row(2, 200, "hr") :: Row(3, 300, "hardware") :: Nil)
    checkUpdateMetrics(numUpdatedRows = 0, numCopiedRows = 0)

    // should update one matching row with a null-safe condition
    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep <=> NULL")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, -1, null) :: Row(2, 200, "hr") :: Row(3, 300, "hardware") :: Nil)
    checkUpdateMetrics(numUpdatedRows = 1, numCopiedRows = 0)
  }

  test("update with NULL conditions on data columns") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": null, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "hr" }
        |{ "pk": 3, "salary": 300, "dep": "hardware" }
        |""".stripMargin)

    // should not update any rows as NULL is never equal to NULL
    sql(s"UPDATE $tableNameAsString SET dep = 'invalid' WHERE salary = NULL")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, null, "hr") :: Row(2, 200, "hr") :: Row(3, 300, "hardware") :: Nil)
    checkUpdateMetrics(numUpdatedRows = 0, numCopiedRows = 0)

    // should update one matching row with a null-safe condition
    sql(s"UPDATE $tableNameAsString SET dep = 'invalid' WHERE salary <=> NULL")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, null, "invalid") :: Row(2, 200, "hr") :: Row(3, 300, "hardware") :: Nil)
    checkUpdateMetrics(numUpdatedRows = 1, numCopiedRows = 1)
  }

  test("update with IN and NOT IN predicates") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "hardware" }
        |{ "pk": 3, "salary": null, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE salary IN (100, null)")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, -1, "hr") :: Row(2, 200, "hardware") :: Row(3, null, "hr") :: Nil)
    checkUpdateMetrics(numUpdatedRows = 1, numCopiedRows = 1)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE salary NOT IN (null, 1)")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, -1, "hr") :: Row(2, 200, "hardware") :: Row(3, null, "hr") :: Nil)
    checkUpdateMetrics(numUpdatedRows = 0, numCopiedRows = 0)

    sql(s"UPDATE $tableNameAsString SET salary = 100 WHERE salary NOT IN (1, 10)")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, 100, "hr") :: Row(2, 100, "hardware") :: Row(3, null, "hr") :: Nil)
    checkUpdateMetrics(numUpdatedRows = 2, numCopiedRows = 1)
  }

  test("update nested struct fields") {
    createAndInitTable(
      s"""pk INT NOT NULL,
         |s STRUCT<c1: INT, c2: STRUCT<a: ARRAY<INT>, m: MAP<STRING, STRING>>>,
         |dep STRING""".stripMargin,
      """{ "pk": 1, "s": { "c1": 2, "c2": { "a": [1,2], "m": { "a": "b" } } }, "dep": "hr" }""")

    // update primitive, array, map columns inside a struct
    sql(s"UPDATE $tableNameAsString SET s.c1 = -1, s.c2.m = map('k', 'v'), s.c2.a = array(-1)")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, Row(-1, Row(Seq(-1), Map("k" -> "v"))), "hr") :: Nil)
    checkUpdateMetrics(numUpdatedRows = 1, numCopiedRows = 0)

    // set primitive, array, map columns to NULL (proper casts should be in inserted)
    sql(s"UPDATE $tableNameAsString SET s.c1 = NULL, s.c2 = NULL WHERE pk = 1")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, Row(null, null), "hr") :: Nil)
    checkUpdateMetrics(numUpdatedRows = 1, numCopiedRows = 0)

    // assign an entire struct
    sql(
      s"""UPDATE $tableNameAsString
         |SET s = named_struct('c1', 1, 'c2', named_struct('a', array(1), 'm', null))
         |""".stripMargin)
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, Row(1, Row(Seq(1), null)), "hr") :: Nil)
    checkUpdateMetrics(numUpdatedRows = 1, numCopiedRows = 0)
  }

  test("update fields inside NULL structs") {
    createAndInitTable("pk INT NOT NULL, s STRUCT<n1: INT, n2: INT>, dep STRING",
      """{ "pk": 1, "s": null, "dep": "hr" }""")

    sql(s"UPDATE $tableNameAsString SET s.n1 = -1 WHERE pk = 1")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, Row(-1, null), "hr") :: Nil)

    checkUpdateMetrics(numUpdatedRows = 1, numCopiedRows = 0)
  }

  test("update refreshes relation cache") {
    withTempView("temp") {
      withCache("temp") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 100, "dep": "hr" }
            |{ "pk": 3, "salary": 200, "dep": "hardware" }
            |{ "pk": 4, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        // define a view on top of the table
        val query = sql(s"SELECT * FROM $tableNameAsString WHERE salary = 100")
        query.createOrReplaceTempView("temp")

        // cache the view
        sql("CACHE TABLE temp")

        // verify the view returns expected results
        checkAnswer(
          sql("SELECT * FROM temp"),
          Row(1, 100, "hr") :: Row(2, 100, "hr") :: Nil)

        // update some records in the table
        sql(s"UPDATE $tableNameAsString SET salary = salary + 10 WHERE salary <= 100")

        // verify the update was successful
        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Row(1, 110, "hr") ::
            Row(2, 110, "hr") ::
            Row(3, 200, "hardware") ::
            Row(4, 300, "hr") :: Nil)

        checkUpdateMetrics(numUpdatedRows = 2, numCopiedRows = 1)

        // verify the view reflects the changes in the table
        checkAnswer(sql("SELECT * FROM temp"), Nil)
      }
    }
  }

  test("update with conditions on nested columns") {
    createAndInitTable("pk INT NOT NULL, salary INT, complex STRUCT<c1:INT,c2:STRING>, dep STRING",
      """{ "pk": 1, "salary": 100, "complex": { "c1": 300, "c2": "v1" }, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "complex": { "c1": 200, "c2": "v2" }, "dep": "software" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE complex.c1 = salary + 200")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, -1, Row(300, "v1"), "hr") :: Row(2, 200, Row(200, "v2"), "software") :: Nil)

    checkUpdateMetrics(numUpdatedRows = 1, numCopiedRows = 0)
  }

  test("update with IN subqueries") {
    withTempView("updated_id", "updated_dep") {
      createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
        """{ "pk": 1, "id": 1, "dep": "hr" }
          |{ "pk": 2, "id": 2, "dep": "hardware" }
          |{ "pk": 3, "id": null, "dep": "hr" }
          |""".stripMargin)

      val updatedIdDF = Seq(Some(0), Some(1), None).toDF()
      updatedIdDF.createOrReplaceTempView("updated_id")

      val updatedDepDF = Seq("software", "hr").toDF()
      updatedDepDF.createOrReplaceTempView("updated_dep")

      sql(
        s"""UPDATE $tableNameAsString
           |SET dep = 'invalid'
           |WHERE
           | id IN (SELECT * FROM updated_id)
           | AND
           | dep IN (SELECT * FROM updated_dep)
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, "invalid") :: Row(2, 2, "hardware") :: Row(3, null, "hr") :: Nil)
      checkUpdateMetrics(numUpdatedRows = 1, numCopiedRows = 1)

      sql(
        s"""UPDATE $tableNameAsString
           |SET dep = 'invalid'
           |WHERE
           | id IS NULL
           | OR
           | id IN (SELECT value + 2 FROM updated_id)
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, "invalid") :: Row(2, 2, "invalid") :: Row(3, null, "invalid") :: Nil)
      checkUpdateMetrics(numUpdatedRows = 2, numCopiedRows = 0)
    }
  }

  test("update with multi-column IN subqueries") {
    withTempView("updated_employee") {
      createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
        """{ "pk": 1, "id": 1, "dep": "hr" }
          |{ "pk": 2, "id": 2, "dep": "hardware" }
          |{ "pk": 3, "id": null, "dep": "hr" }
          |""".stripMargin)

      val updatedEmployeeDF = Seq((None, "hr"), (Some(1), "hr")).toDF()
      updatedEmployeeDF.createOrReplaceTempView("updated_employee")

      sql(
        s"""UPDATE $tableNameAsString
           |SET dep = 'invalid'
           |WHERE
           | (id, dep) IN (SELECT * FROM updated_employee)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, "invalid") :: Row(2, 2, "hardware") :: Row(3, null, "hr") :: Nil)

      checkUpdateMetrics(numUpdatedRows = 1, numCopiedRows = 1)
    }
  }

  test("update with NOT IN subqueries") {
    withTempView("updated_id", "updated_dep") {
      createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
        """{ "pk": 1, "id": 1, "dep": "hr" }
          |{ "pk": 2, "id": 2, "dep": "hardware" }
          |{ "pk": 3, "id": null, "dep": "hr" }
          |""".stripMargin)

      val updatedIdDF = Seq(Some(-1), Some(-2), None).toDF()
      updatedIdDF.createOrReplaceTempView("updated_id")

      val updatedDepDF = Seq("software", "hr").toDF()
      updatedDepDF.createOrReplaceTempView("updated_dep")

      sql(
        s"""UPDATE $tableNameAsString
           |SET dep = 'invalid'
           |WHERE
           | id NOT IN (SELECT * FROM updated_id)
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, "hr") :: Row(2, 2, "hardware") :: Row(3, null, "hr") :: Nil)
      checkUpdateMetrics(numUpdatedRows = 0, numCopiedRows = 0)

      sql(
        s"""UPDATE $tableNameAsString
           |SET dep = 'invalid'
           |WHERE
           | id NOT IN (SELECT * FROM updated_id WHERE value IS NOT NULL)
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, "invalid") :: Row(2, 2, "invalid") :: Row(3, null, "hr") :: Nil)
      checkUpdateMetrics(numUpdatedRows = 2, numCopiedRows = 1)

      sql(
        s"""UPDATE $tableNameAsString
           |SET dep = 'hr'
           |WHERE
           | id NOT IN (SELECT * FROM updated_id)
           | OR
           | dep IN ('software', 'invalid')
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, "hr") :: Row(2, 2, "hr") :: Row(3, null, "hr") :: Nil)
      checkUpdateMetrics(numUpdatedRows = 2, numCopiedRows = 0)
    }
  }

  test("update with EXISTS subquery") {
    withTempView("updated_id", "updated_dep") {
      createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
        """{ "pk": 1, "id": 1, "dep": "hr" }
          |{ "pk": 2, "id": 2, "dep": "hardware" }
          |{ "pk": 3, "id": null, "dep": "hr" }
          |""".stripMargin)

      val updatedIdDF = Seq(Some(-1), Some(-2), None).toDF()
      updatedIdDF.createOrReplaceTempView("updated_id")

      val updatedDepDF = Seq("software", "hr").toDF()
      updatedDepDF.createOrReplaceTempView("updated_dep")

      sql(
        s"""UPDATE $tableNameAsString t
           |SET dep = 'invalid'
           |WHERE
           | EXISTS (SELECT 1 FROM updated_id d WHERE t.id = d.value)
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, "hr") :: Row(2, 2, "hardware") :: Row(3, null, "hr") :: Nil)
      checkUpdateMetrics(numUpdatedRows = 0, numCopiedRows = 0)

      sql(
        s"""UPDATE $tableNameAsString t
           |SET dep = 'invalid'
           |WHERE
           | EXISTS (SELECT 1 FROM updated_id d WHERE t.id = d.value + 2)
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, "invalid") :: Row(2, 2, "hardware") :: Row(3, null, "hr") :: Nil)
      checkUpdateMetrics(numUpdatedRows = 1, numCopiedRows = 1)

      sql(
        s"""UPDATE $tableNameAsString t
           |SET dep = 'invalid'
           |WHERE
           | EXISTS (SELECT 1 FROM updated_id d WHERE t.id = d.value) OR t.id IS NULL
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, "invalid") :: Row(2, 2, "hardware") :: Row(3, null, "invalid") :: Nil)
      checkUpdateMetrics(numUpdatedRows = 1, numCopiedRows = 0)

      sql(
        s"""UPDATE $tableNameAsString t
           |SET dep = 'invalid'
           |WHERE
           | EXISTS (SELECT 1 FROM updated_id di WHERE t.id = di.value)
           | AND
           | EXISTS (SELECT 1 FROM updated_dep dd WHERE t.dep = dd.value)
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, "invalid") :: Row(2, 2, "hardware") :: Row(3, null, "invalid") :: Nil)
      checkUpdateMetrics(numUpdatedRows = 0, numCopiedRows = 0)
    }
  }

  test("update with NOT EXISTS subquery") {
    withTempView("updated_id", "updated_dep") {
      createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
        """{ "pk": 1, "id": 1, "dep": "hr" }
          |{ "pk": 2, "id": 2, "dep": "hardware" }
          |{ "pk": 3, "id": null, "dep": "hr" }
          |""".stripMargin)

      val updatedIdDF = Seq(Some(-1), Some(-2), None).toDF()
      updatedIdDF.createOrReplaceTempView("updated_id")

      val updatedDepDF = Seq("software", "hr").toDF()
      updatedDepDF.createOrReplaceTempView("updated_dep")

      sql(
        s"""UPDATE $tableNameAsString t
           |SET dep = 'invalid'
           |WHERE
           | NOT EXISTS (SELECT 1 FROM updated_id di WHERE t.id = di.value + 2)
           | AND
           | NOT EXISTS (SELECT 1 FROM updated_dep dd WHERE t.dep = dd.value)
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, "hr") :: Row(2, 2, "invalid") :: Row(3, null, "hr") :: Nil)
      checkUpdateMetrics(numUpdatedRows = 1, numCopiedRows = 0)

      sql(
        s"""UPDATE $tableNameAsString t
           |SET dep = 'invalid'
           |WHERE
           | NOT EXISTS (SELECT 1 FROM updated_id d WHERE t.id = d.value + 2)
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, "hr") :: Row(2, 2, "invalid") :: Row(3, null, "invalid") :: Nil)
      checkUpdateMetrics(numUpdatedRows = 2, numCopiedRows = 1)

      sql(
        s"""UPDATE $tableNameAsString t
           |SET dep = 'invalid'
           |WHERE
           | NOT EXISTS (SELECT 1 FROM updated_id d WHERE t.id = d.value + 2)
           | OR
           | t.id = 1
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, "invalid") :: Row(2, 2, "invalid") :: Row(3, null, "invalid") :: Nil)
      checkUpdateMetrics(numUpdatedRows = 3, numCopiedRows = 0)
    }
  }

  test("update with a scalar subquery") {
    withTempView("updated_id") {
      createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
        """{ "pk": 1, "id": 1, "dep": "hr" }
          |{ "pk": 2, "id": 2, "dep": "hardware" }
          |{ "pk": 3, "id": null, "dep": "hr" }
          |""".stripMargin)

      val updatedIdDF = Seq(Some(1), Some(100), None).toDF()
      updatedIdDF.createOrReplaceTempView("updated_id")

      sql(
        s"""UPDATE $tableNameAsString t
           |SET dep = 'invalid'
           |WHERE
           | id <= (SELECT min(value) FROM updated_id)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, "invalid") :: Row(2, 2, "hardware") :: Row(3, null, "hr") :: Nil)

      checkUpdateMetrics(numUpdatedRows = 1, numCopiedRows = 1)
    }
  }

  test("update with nondeterministic assignments") {
    createAndInitTable("pk INT NOT NULL, id INT, value DOUBLE, dep STRING",
      """{ "pk": 1, "id": 1, "value": 2.0, "dep": "hr" }
        |{ "pk": 2, "id": 2, "value": 2.0,  "dep": "software" }
        |{ "pk": 3, "id": 3, "value": 2.0, "dep": "hr" }
        |""".stripMargin)

    // rand() always generates values in [0, 1) range
    sql(s"UPDATE $tableNameAsString SET value = rand() WHERE id <= 2")

    checkAnswer(
      sql(s"SELECT count(*) FROM $tableNameAsString WHERE value < 2.0"),
      Row(2) :: Nil)

    checkUpdateMetrics(numUpdatedRows = 2, numCopiedRows = 1)
  }

  test("SPARK-53538: update with nondeterministic assignments and no wholestage codegen") {
    val extraColCount = SQLConf.get.wholeStageMaxNumFields - 4
    val schema = "pk INT NOT NULL, id INT, value DOUBLE, dep STRING, " +
      ((1 to extraColCount).map(i => s"col$i INT").mkString(", "))
    val data = (1 to 3).map { i =>
      s"""{ "pk": $i, "id": $i, "value": 2.0, "dep": "hr", """ +
        ((1 to extraColCount).map(j => s""""col$j": $i""").mkString(", ")) +
      "}"
    }.mkString("\n")
    createAndInitTable(schema, data)

    // rand() always generates values in [0, 1) range
    sql(s"UPDATE $tableNameAsString SET value = rand() WHERE id <= 2")

    checkAnswer(
      sql(s"SELECT count(*) FROM $tableNameAsString WHERE value < 2.0"),
      Row(2) :: Nil)

    checkUpdateMetrics(numUpdatedRows = 2, numCopiedRows = 1)
  }

  test("update with default values") {
    val idDefault = new ColumnDefaultValue("42", LiteralValue(42, IntegerType))
    val columns = Array(
      Column.create("pk", IntegerType, false, null, null),
      Column.create("id", IntegerType, true, null, idDefault, null),
      Column.create("dep", StringType, true, null, null))

    createTable(columns)

    append("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET id = DEFAULT WHERE dep = 'hr'")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, 42, "hr") :: Row(2, 2, "software") :: Row(3, 42, "hr") :: Nil)

    checkUpdateMetrics(numUpdatedRows = 2, numCopiedRows = 0)
  }

  test("update with current_timestamp default value using DEFAULT keyword") {
    sql(s"""CREATE TABLE $tableNameAsString
      | (pk INT NOT NULL, current_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""".stripMargin)
    append("pk INT NOT NULL, current_timestamp TIMESTAMP",
      """{ "pk": 1, "i": false, "current_timestamp": "2023-01-01 10:00:00" }
        |{ "pk": 2, "i": true, "current_timestamp": "2023-01-01 11:00:00" }
        |""".stripMargin)

    val initialResult = sql(s"SELECT * FROM $tableNameAsString").collect()
    assert(initialResult.length == 2)
    val initialTimestamp1 = initialResult(0).getTimestamp(1)
    val initialTimestamp2 = initialResult(1).getTimestamp(1)

    sql(s"UPDATE $tableNameAsString SET current_timestamp = DEFAULT WHERE pk = 1")

    val updatedResult = sql(s"SELECT * FROM $tableNameAsString").collect()
    assert(updatedResult.length == 2)

    val updatedRow = updatedResult.find(_.getInt(0) == 1).get
    val unchangedRow = updatedResult.find(_.getInt(0) == 2).get

    // The timestamp should be different (newer) after the update for pk=1
    assert(updatedRow.getTimestamp(1).getTime > initialTimestamp1.getTime)
    // The timestamp should remain unchanged for pk=2
    assert(unchangedRow.getTimestamp(1).getTime == initialTimestamp2.getTime)
  }

  test("update char/varchar columns") {
    createTable("pk INT NOT NULL, s STRUCT<n_c: CHAR(3), n_vc: VARCHAR(5)>, dep STRING")

    append("pk INT NOT NULL, s STRUCT<n_c: STRING, n_vc: STRING>, dep STRING",
      """{ "pk": 1, "s": { "n_c": "aaa", "n_vc": "aaa" }, "dep": "hr" }
        |{ "pk": 2, "s": { "n_c": "bbb", "n_vc": "bbb" }, "dep": "software" }
        |{ "pk": 3, "s": { "n_c": "ccc", "n_vc": "ccc" }, "dep": "hr" }
        |""".stripMargin)

    sql(s"UPDATE $tableNameAsString SET s.n_c = 'x', s.n_vc = 'y' WHERE dep = 'hr'")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, Row("x  ", "y"), "hr") ::
        Row(2, Row("bbb", "bbb"), "software") ::
        Row(3, Row("x  ", "y"), "hr") :: Nil)

    checkUpdateMetrics(numUpdatedRows = 2, numCopiedRows = 0)
  }

  test("update with NOT NULL checks") {
    createAndInitTable("pk INT NOT NULL, s STRUCT<n_i: INT NOT NULL, n_l: LONG>, dep STRING",
      """{ "pk": 1, "s": { "n_i": 1, "n_l": 11 }, "dep": "hr" }
        |{ "pk": 2, "s": { "n_i": 2, "n_l": 22 }, "dep": "software" }
        |{ "pk": 3, "s": { "n_i": 3, "n_l": 33 }, "dep": "hr" }
        |""".stripMargin)

    checkError(
      exception = intercept[SparkRuntimeException] {
        sql(s"UPDATE $tableNameAsString SET s = named_struct('n_i', null, 'n_l', -1L) WHERE pk = 1")
      },
      condition = "NOT_NULL_ASSERT_VIOLATION",
      sqlState = "42000",
      parameters = Map("walkedTypePath" -> "\ns\nn_i\n"))
  }



  test("update table with recursive CTE") {
    withTempView("source") {
      sql(
        s"""CREATE TABLE $tableNameAsString (
           | val INT)
           |""".stripMargin)

      append("val INT",
        """{ "val": 1 }
          |{ "val": 9 }
          |{ "val": 8 }
          |{ "val": 4 }
          |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1),
          Row(9),
          Row(8),
          Row(4)))

      sql(
        s"""WITH RECURSIVE s(val) AS (
           |  SELECT 1
           |  UNION ALL
           |  SELECT val + 1 FROM s WHERE val < 5
           |) UPDATE $tableNameAsString SET val = val + 1 WHERE val IN (SELECT val FROM s)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(2),
          Row(9),
          Row(8),
          Row(5)))
    }
  }

  test("update with analysis failure and transactional checks") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    val exception = intercept[AnalysisException] {
      sql(s"UPDATE $tableNameAsString SET invalid_column = -1")
    }

    assert(exception.getMessage.contains("invalid_column"))
    assert(catalog.lastTransaction.currentState == Aborted)
    assert(catalog.lastTransaction.isClosed)
  }

  test("update with CTE and transactional checks") {
    // create table
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    // create source table
    sql(s"CREATE TABLE $sourceNameAsString (pk INT NOT NULL, salary INT, dep STRING)")
    sql(s"INSERT INTO $sourceNameAsString VALUES (1, 150, 'hr'), (4, 400, 'finance')")

    // update using CTE
    val (txn, txnTables) = executeTransaction {
      sql(
        s"""WITH cte AS (
           |  SELECT pk, salary + 50 AS adjusted_salary, dep
           |  FROM $sourceNameAsString
           |  WHERE salary > 100
           |)
           |UPDATE $tableNameAsString t
           |SET salary = -1
           |WHERE t.dep = 'hr' AND EXISTS (SELECT 1 FROM cte WHERE cte.pk = t.pk)
           |""".stripMargin)
    }

    // check txn was properly committed and closed
    assert(txn.currentState == Committed)
    assert(txn.isClosed)
    assert(txnTables.size == 2)
    assert(table.version() == "2")

    // check target table was scanned correctly
    val targetTxnTable = txnTables(tableNameAsString)
    val expectedNumTargetScans = if (deltaUpdate) 2 else 3
    assert(targetTxnTable.scanEvents.size == expectedNumTargetScans)

    // check target table scans for UPDATE condition (dep = 'hr')
    val numUpdateTargetScans = targetTxnTable.scanEvents.flatten.count {
      case sources.EqualTo("dep", "hr") => true
      case _ => false
    }
    assert(numUpdateTargetScans == expectedNumTargetScans)

    // check source table was scanned correctly
    val sourceTxnTable = txnTables(sourceNameAsString)
    val expectedNumSourceScans = if (deltaUpdate) 2 else 4
    assert(sourceTxnTable.scanEvents.size == expectedNumSourceScans)

    // check source table scans in CTE (salary > 100)
    val numCteSourceScans = sourceTxnTable.scanEvents.flatten.count {
      case sources.GreaterThan("salary", 100) => true
      case _ => false
    }
    assert(numCteSourceScans == expectedNumSourceScans)

    // check txn state was propagated correctly
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, -1, "hr"), // updated
        Row(2, 200, "software"), // unchanged
        Row(3, 300, "hr"))) // unchanged (no matching pk in source)
  }

  test("update with subquery on source table and transactional checks") {
    // create target table
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    // create source table
    sql(s"CREATE TABLE $sourceNameAsString (pk INT NOT NULL, salary INT, dep STRING)")
    sql(s"INSERT INTO $sourceNameAsString VALUES (1, 150, 'hr'), (4, 400, 'finance')")

    // update using an uncorrelated IN subquery that reads from a transactional catalog table
    val (txn, txnTables) = executeTransaction {
      sql(
        s"""UPDATE $tableNameAsString
           |SET salary = -1
           |WHERE pk IN (SELECT pk FROM $sourceNameAsString WHERE dep = 'hr')
           |""".stripMargin)
    }

    // check txn was properly committed and closed
    assert(txn.currentState == Committed)
    assert(txn.isClosed)
    assert(txnTables.size == 2)
    assert(table.version() == "2")

    // check source table was scanned correctly (dep = 'hr' filter in the subquery)
    val sourceTxnTable = txnTables(sourceNameAsString)
    val expectedNumSourceScans = if (deltaUpdate) 2 else 4
    assert(sourceTxnTable.scanEvents.size == expectedNumSourceScans)

    val numSubquerySourceScans = sourceTxnTable.scanEvents.flatten.count {
      case sources.EqualTo("dep", "hr") => true
      case _ => false
    }
    assert(numSubquerySourceScans == expectedNumSourceScans)

    // check target table was scanned correctly
    val targetTxnTable = txnTables(tableNameAsString)
    val expectedNumTargetScans = if (deltaUpdate) 2 else 3
    assert(targetTxnTable.scanEvents.size == expectedNumTargetScans)

    // check txn state was propagated correctly
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, -1, "hr"), // updated (pk 1 is in subquery result)
        Row(2, 200, "software"), // unchanged
        Row(3, 300, "hr"))) // unchanged (pk 3 not in subquery result)
  }

  test("update with uncorrelated scalar subquery on source table and transactional checks") {
    // create target table
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    // create source table
    sql(s"CREATE TABLE $sourceNameAsString (pk INT NOT NULL, salary INT, dep STRING)")
    sql(s"INSERT INTO $sourceNameAsString VALUES (1, 150, 'hr'), (4, 400, 'finance')")

    // update using an uncorrelated scalar subquery in the SET clause that reads from a
    // transactional catalog table; scalar subqueries are executed as SubqueryExec at runtime
    // and cannot be rewritten as joins
    val (txn, txnTables) = executeTransaction {
      sql(
        s"""UPDATE $tableNameAsString
           |SET salary = (SELECT max(salary) FROM $sourceNameAsString WHERE dep = 'hr')
           |WHERE dep = 'hr'
           |""".stripMargin)
    }

    // check txn was properly committed and closed
    assert(txn.currentState == Committed)
    assert(txn.isClosed)
    assert(txnTables.size == 2)
    assert(table.version() == "2")

    // check source table was scanned via the transaction catalog
    val sourceTxnTable = txnTables(sourceNameAsString)
    assert(sourceTxnTable.scanEvents.nonEmpty)
    assert(sourceTxnTable.scanEvents.flatten.exists {
      case sources.EqualTo("dep", "hr") => true
      case _ => false
    })

    // check target table was scanned via the transaction catalog
    val targetTxnTable = txnTables(tableNameAsString)
    assert(targetTxnTable.scanEvents.nonEmpty)

    // check txn state was propagated correctly
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, 150, "hr"), // updated (max salary in source for 'hr' is 150)
        Row(2, 200, "software"), // unchanged
        Row(3, 150, "hr"))) // updated
  }

  test("update with constraint violation and transactional checks") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    val exception = intercept[SparkRuntimeException] {
      executeTransaction {
        sql(
          s"""UPDATE $tableNameAsString
             |SET pk = NULL
             |WHERE dep = 'hr'
             |""".stripMargin) // NULL violates NOT NULL constraint
      }
    }

    assert(exception.getMessage.contains("NOT_NULL_ASSERT_VIOLATION"))
    assert(catalog.lastTransaction.currentState == Aborted)
    assert(catalog.lastTransaction.isClosed)
  }

  test("update using view with transactional checks") {
    withView("temp_view") {
      // create target table
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |""".stripMargin)

      // create source table
      sql(s"CREATE TABLE $sourceNameAsString (pk INT NOT NULL, salary INT)")
      sql(s"INSERT INTO $sourceNameAsString (pk, salary) VALUES (1, 150), (4, 400)")

      // create view on top of source and target tables
      sql(
        s"""CREATE VIEW temp_view AS
           |SELECT s.pk, s.salary, t.dep
           |FROM $sourceNameAsString s
           |LEFT JOIN (
           | SELECT * FROM $tableNameAsString WHERE pk < 10
           |) t ON s.pk = t.pk
           |""".stripMargin)

      // update target table using view
      val (txn, txnTables) = executeTransaction {
        sql(
          s"""UPDATE $tableNameAsString t
             |SET salary = -1
             |WHERE t.dep = 'hr' AND EXISTS (SELECT 1 FROM temp_view v WHERE v.pk = t.pk)
             |""".stripMargin)
      }

      // check txn covers both tables and was properly committed and closed
      assert(txn.currentState == Committed)
      assert(txn.isClosed)
      assert(txnTables.size == 2)
      assert(table.version() == "2")

      // check target table was scanned correctly
      val targetTxnTable = txnTables(tableNameAsString)
      val expectedNumTargetScans = if (deltaUpdate) 4 else 7
      assert(targetTxnTable.scanEvents.size == expectedNumTargetScans)

      // check target table scans as UPDATE target (dep = 'hr')
      val numUpdateTargetScans = targetTxnTable.scanEvents.flatten.count {
        case sources.EqualTo("dep", "hr") => true
        case _ => false
      }
      val expectedNumUpdateTargetScans = if (deltaUpdate) 2 else 3
      assert(numUpdateTargetScans == expectedNumUpdateTargetScans)

      // check target table scans in view as source (pk < 10)
      val numViewTargetScans = targetTxnTable.scanEvents.flatten.count {
        case sources.LessThan("pk", 10L) => true
        case _ => false
      }
      val expectedNumViewTargetScans = if (deltaUpdate) 2 else 4
      assert(numViewTargetScans == expectedNumViewTargetScans)

      // check source table scans in view
      val sourceTxnTable = txnTables(sourceNameAsString)
      val expectedNumSourceScans = if (deltaUpdate) 2 else 4
      assert(sourceTxnTable.scanEvents.size == expectedNumSourceScans)

      // check txn state was propagated correctly
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, -1, "hr"), // updated from view
          Row(2, 200, "software"), // unchanged
          Row(3, 300, "hr"))) // unchanged (no matching pk in source)
    }
  }

  test("df.explain() on update with transactional checks") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    // sql() is lazy, but explain() forces executedPlan.
    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'").explain()

    assert(catalog.lastTransaction != null)
    assert(catalog.lastTransaction.currentState == Committed)
    assert(catalog.lastTransaction.isClosed)
    assert(table.version() == "2")

    // The UPDATE was actually executed, not just planned.
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, -1, "hr"), // updated
        Row(2, 200, "software"))) // unchanged
  }

  test("EXPLAIN UPDATE SQL with transactional checks") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |""".stripMargin)

    // EXPLAIN UPDATE only plans the command, it does not execute the write.
    sql(s"EXPLAIN UPDATE $tableNameAsString SET salary = -1 WHERE dep = 'hr'")

    // A transaction should not have started at all.
    assert(catalog.transaction === null)

    // The UPDATE was not executed. Data is unchanged.
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, 100, "hr"),
        Row(2, 200, "software")))
  }
}
