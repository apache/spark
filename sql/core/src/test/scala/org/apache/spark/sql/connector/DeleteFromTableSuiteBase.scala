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
import org.apache.spark.sql.execution.datasources.v2.{DeleteFromTableExec, ReplaceDataExec, WriteDeltaExec}

abstract class DeleteFromTableSuiteBase extends RowLevelOperationSuiteBase {

  import testImplicits._

  test("delete from table containing added column with default value") {
    createAndInitTable("pk INT NOT NULL, dep STRING", """{ "pk": 1, "dep": "hr" }""")

    sql(s"ALTER TABLE $tableNameAsString ADD COLUMN txt STRING DEFAULT 'initial-text'")

    append("pk INT, dep STRING",
      """{ "pk": 2, "dep": "hr" }
        |{ "pk": 3, "dep": "software" }
        |{ "pk": 4, "dep": "hr" }
        |""".stripMargin)

    sql(s"ALTER TABLE $tableNameAsString ALTER COLUMN txt SET DEFAULT 'new-text'")

    append("pk INT, dep STRING",
      """{ "pk": 5, "dep": "hr" }
        |{ "pk": 6, "dep": "hr" }
        |""".stripMargin)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, "hr", "initial-text"),
        Row(2, "hr", "initial-text"),
        Row(3, "software", "initial-text"),
        Row(4, "hr", "initial-text"),
        Row(5, "hr", "new-text"),
        Row(6, "hr", "new-text")))

    sql(s"DELETE FROM $tableNameAsString WHERE pk IN (2, 5)")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, "hr", "initial-text"),
        Row(3, "software", "initial-text"),
        Row(4, "hr", "initial-text"),
        Row(6, "hr", "new-text")))
  }

  test("delete from table containing struct column with default value") {
    sql(
      s"""CREATE TABLE $tableNameAsString (
         | pk INT NOT NULL,
         | complex STRUCT<c1:INT,c2:STRING> DEFAULT struct(-1, 'unknown'),
         | dep STRING)
         |PARTITIONED BY (dep)
         |""".stripMargin)

    append("pk INT NOT NULL, dep STRING",
      """{ "pk": 1, "dep": "hr" }
        |{ "pk": 2, "dep": "hr" }
        |{ "pk": 3, "dep": "hr" }
        |""".stripMargin)

    append("pk INT NOT NULL, complex STRUCT<c1:INT,c2:STRING>, dep STRING",
      """{ "pk": 4, "dep": "hr" }
        |{ "pk": 5, "complex": { "c1": 5, "c2": "v5" }, "dep": "hr" }
        |""".stripMargin)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, Row(-1, "unknown"), "hr"),
        Row(2, Row(-1, "unknown"), "hr"),
        Row(3, Row(-1, "unknown"), "hr"),
        Row(4, null, "hr"),
        Row(5, Row(5, "v5"), "hr")))

    sql(s"DELETE FROM $tableNameAsString WHERE pk < 3")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(3, Row(-1, "unknown"), "hr"),
        Row(4, null, "hr"),
        Row(5, Row(5, "v5"), "hr")))
  }

  test("EXPLAIN only delete") {
    createAndInitTable("id INT, dep STRING", """{ "id": 1, "dep": "hr" }""")

    sql(s"EXPLAIN DELETE FROM $tableNameAsString WHERE id <= 10")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, "hr") :: Nil)
  }

  test("delete from empty tables") {
    createTable("pk INT NOT NULL, id INT, dep STRING")

    sql(s"DELETE FROM $tableNameAsString WHERE id <= 1")

    checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Nil)
  }

  test("delete with basic filters") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"DELETE FROM $tableNameAsString WHERE id <= 1")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, 2, "software") :: Row(3, 3, "hr") :: Nil)
  }

  test("delete with aliases") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": 3, "dep": "hr" }
        |""".stripMargin)

    sql(s"DELETE FROM $tableNameAsString AS t WHERE t.id <= 1 OR t.dep = 'hr'")

    checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(2, 2, "software") :: Nil)
  }

  test("delete with IN predicates") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": null, "dep": "hr" }
        |""".stripMargin)

    sql(s"DELETE FROM $tableNameAsString WHERE id IN (1, null)")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, 2, "software") :: Row(3, null, "hr") :: Nil)
  }

  test("delete with NOT IN predicates") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "software" }
        |{ "pk": 3, "id": null, "dep": "hr" }
        |""".stripMargin)

    sql(s"DELETE FROM $tableNameAsString WHERE id NOT IN (null, 1)")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, 1, "hr") :: Row(2, 2, "software") :: Row(3, null, "hr") :: Nil)

    sql(s"DELETE FROM $tableNameAsString WHERE id NOT IN (1, 10)")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, 1, "hr") :: Row(3, null, "hr") :: Nil)
  }

  test("delete with conditions on nested columns") {
    createAndInitTable("pk INT NOT NULL, id INT, complex STRUCT<c1:INT,c2:STRING>, dep STRING",
      """{ "pk": 1, "id": 1, "complex": { "c1": 3, "c2": "v1" }, "dep": "hr" }
        |{ "pk": 2, "id": 2, "complex": { "c1": 2, "c2": "v2" }, "dep": "software" }
        |""".stripMargin)

    sql(s"DELETE FROM $tableNameAsString WHERE complex.c1 = id + 2")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, 2, Row(2, "v2"), "software") :: Nil)

    sql(s"DELETE FROM $tableNameAsString t WHERE t.complex.c1 = id")

    checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Nil)
  }

  test("delete with IN subqueries") {
    withTempView("deleted_id", "deleted_dep") {
      createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
        """{ "pk": 1, "id": 1, "dep": "hr" }
          |{ "pk": 2, "id": 2, "dep": "hardware" }
          |{ "pk": 3, "id": null, "dep": "hr" }
          |""".stripMargin)

      val deletedIdDF = Seq(Some(0), Some(1), None).toDF()
      deletedIdDF.createOrReplaceTempView("deleted_id")

      val deletedDepDF = Seq("software", "hr").toDF()
      deletedDepDF.createOrReplaceTempView("deleted_dep")

      sql(
        s"""DELETE FROM $tableNameAsString
           |WHERE
           | id IN (SELECT * FROM deleted_id)
           | AND
           | dep IN (SELECT * FROM deleted_dep)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(2, 2, "hardware") :: Row(3, null, "hr") :: Nil)

      append("pk INT NOT NULL, id INT, dep STRING",
        """{ "pk": 4, "id": 1, "dep": "hr" }
          |{ "pk": 5, "id": -1, "dep": "hr" }
          |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(5, -1, "hr") :: Row(4, 1, "hr") :: Row(2, 2, "hardware") :: Row(3, null, "hr") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString
           |WHERE
           | id IS NULL
           | OR
           | id IN (SELECT value + 2 FROM deleted_id)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(5, -1, "hr") :: Row(4, 1, "hr") :: Nil)

      append("pk INT NOT NULL, id INT, dep STRING",
        """{ "pk": 6, "id": null, "dep": "hr" }
          |{ "pk": 7, "id": 2, "dep": "hr" }
          |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(5, -1, "hr") :: Row(4, 1, "hr") :: Row(7, 2, "hr") :: Row(6, null, "hr") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString
           |WHERE
           | id IN (SELECT value + 2 FROM deleted_id)
           | AND
           | dep = 'hr'
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(5, -1, "hr") :: Row(4, 1, "hr") :: Row(6, null, "hr") :: Nil)
    }
  }

  test("delete with multi-column IN subqueries") {
    withTempView("deleted_employee") {
      createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
        """{ "pk": 1, "id": 1, "dep": "hr" }
          |{ "pk": 2, "id": 2, "dep": "hardware" }
          |{ "pk": 3, "id": null, "dep": "hr" }
          |""".stripMargin)

      val deletedEmployeeDF = Seq((None, "hr"), (Some(1), "hr")).toDF()
      deletedEmployeeDF.createOrReplaceTempView("deleted_employee")

      sql(
        s"""DELETE FROM $tableNameAsString
           |WHERE
           | (id, dep) IN (SELECT * FROM deleted_employee)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(2, 2, "hardware") :: Row(3, null, "hr") :: Nil)
    }
  }

  test("delete with NOT IN subqueries") {
    withTempView("deleted_id", "deleted_dep") {
      createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
        """{ "pk": 1, "id": 1, "dep": "hr" }
          |{ "pk": 2, "id": 2, "dep": "hardware" }
          |{ "pk": 3, "id": null, "dep": "hr" }
          |""".stripMargin)

      val deletedIdDF = Seq(Some(-1), Some(-2), None).toDF()
      deletedIdDF.createOrReplaceTempView("deleted_id")

      val deletedDepDF = Seq("software", "hr").toDF()
      deletedDepDF.createOrReplaceTempView("deleted_dep")

      sql(
        s"""DELETE FROM $tableNameAsString
           |WHERE
           | id NOT IN (SELECT * FROM deleted_id)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, "hr") :: Row(2, 2, "hardware") :: Row(3, null, "hr") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString
           |WHERE
           | id NOT IN (SELECT * FROM deleted_id WHERE value IS NOT NULL)
           |""".stripMargin)

      checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(3, null, "hr") :: Nil)

      append("pk INT NOT NULL, id INT, dep STRING",
        """{ "pk": 4, "id": 1, "dep": "hr" }
          |{ "pk": 5, "id": 2, "dep": "hardware" }
          |{ "pk": 6, "id": null, "dep": "hr" }
          |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(4, 1, "hr") :: Row(5, 2, "hardware") :: Row(3, null, "hr") :: Row(6, null, "hr") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString
           |WHERE
           | id NOT IN (SELECT * FROM deleted_id)
           | OR
           | dep IN ('software', 'hr')
           |""".stripMargin)

      checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(5, 2, "hardware") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString
           |WHERE
           | id NOT IN (SELECT * FROM deleted_id WHERE value IS NOT NULL)
           | AND
           | EXISTS (SELECT 1 FROM FROM deleted_dep WHERE dep = deleted_dep.value)
           |""".stripMargin)

      checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(5, 2, "hardware") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString t
           |WHERE
           | t.id NOT IN (SELECT * FROM deleted_id WHERE value IS NOT NULL)
           | OR
           | EXISTS (SELECT 1 FROM FROM deleted_dep WHERE t.dep = deleted_dep.value)
           |""".stripMargin)

      checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Nil)
    }
  }

  test("delete with EXISTS subquery") {
    withTempView("deleted_id", "deleted_dep") {
      createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
        """{ "pk": 1, "id": 1, "dep": "hr" }
          |{ "pk": 2, "id": 2, "dep": "hardware" }
          |{ "pk": 3, "id": null, "dep": "hr" }
          |""".stripMargin)

      val deletedIdDF = Seq(Some(-1), Some(-2), None).toDF()
      deletedIdDF.createOrReplaceTempView("deleted_id")

      val deletedDepDF = Seq("software", "hr").toDF()
      deletedDepDF.createOrReplaceTempView("deleted_dep")

      sql(
        s"""DELETE FROM $tableNameAsString t
           |WHERE
           | EXISTS (SELECT 1 FROM deleted_id d WHERE t.id = d.value)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, "hr") :: Row(2, 2, "hardware") :: Row(3, null, "hr") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString t
           |WHERE
           | EXISTS (SELECT 1 FROM deleted_id d WHERE t.id = d.value + 2)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(2, 2, "hardware") :: Row(3, null, "hr") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString t
           |WHERE
           | EXISTS (SELECT 1 FROM deleted_id d WHERE t.id = d.value) OR t.id IS NULL
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(2, 2, "hardware") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString t
           |WHERE
           | EXISTS (SELECT 1 FROM deleted_id di WHERE t.id = di.value)
           | AND
           | EXISTS (SELECT 1 FROM deleted_dep dd WHERE t.dep = dd.value)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(2, 2, "hardware") :: Nil)
    }
  }

  test("delete with NOT EXISTS subquery") {
    withTempView("deleted_id", "deleted_dep") {
      createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
        """{ "pk": 1, "id": 1, "dep": "hr" }
          |{ "pk": 2, "id": 2, "dep": "hardware" }
          |{ "pk": 3, "id": null, "dep": "hr" }
          |""".stripMargin)

      val deletedIdDF = Seq(Some(-1), Some(-2), None).toDF()
      deletedIdDF.createOrReplaceTempView("deleted_id")

      val deletedDepDF = Seq("software", "hr").toDF()
      deletedDepDF.createOrReplaceTempView("deleted_dep")

      sql(
        s"""DELETE FROM $tableNameAsString t
           |WHERE
           | NOT EXISTS (SELECT 1 FROM deleted_id di WHERE t.id = di.value + 2)
           | AND
           | NOT EXISTS (SELECT 1 FROM deleted_dep dd WHERE t.dep = dd.value)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, "hr") :: Row(3, null, "hr") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString t
           |WHERE
           | NOT EXISTS (SELECT 1 FROM deleted_id d WHERE t.id = d.value + 2)
           |""".stripMargin)

      checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Row(1, 1, "hr") :: Nil)

      sql(
        s"""DELETE FROM $tableNameAsString t
           |WHERE
           | NOT EXISTS (SELECT 1 FROM deleted_id d WHERE t.id = d.value + 2)
           | OR
           | t.id = 1
           |""".stripMargin)

      checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Nil)
    }
  }

  test("delete with a scalar subquery") {
    withTempView("deleted_id") {
      createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
        """{ "pk": 1, "id": 1, "dep": "hr" }
          |{ "pk": 2, "id": 2, "dep": "hardware" }
          |{ "pk": 3, "id": null, "dep": "hr" }
          |""".stripMargin)

      val deletedIdDF = Seq(Some(1), Some(100), None).toDF()
      deletedIdDF.createOrReplaceTempView("deleted_id")

      sql(
        s"""DELETE FROM $tableNameAsString t
           |WHERE
           | id <= (SELECT min(value) FROM deleted_id)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(2, 2, "hardware") :: Row(3, null, "hr") :: Nil)
    }
  }

  test("delete refreshes relation cache") {
    withTempView("temp") {
      withCache("temp") {
        createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
          """{ "pk": 1, "id": 1, "dep": "hr" }
            |{ "pk": 2, "id": 1, "dep": "hardware" }
            |{ "pk": 3, "id": 2, "dep": "hardware" }
            |{ "pk": 4, "id": 3, "dep": "hr" }
            |""".stripMargin)

        // define a view on top of the table
        val query = sql(s"SELECT * FROM $tableNameAsString WHERE id = 1")
        query.createOrReplaceTempView("temp")

        // cache the view
        sql("CACHE TABLE temp")

        // verify the view returns expected results
        checkAnswer(
          sql("SELECT * FROM temp"),
          Row(1, 1, "hr") :: Row(2, 1, "hardware") :: Nil)

        // delete some records from the table
        sql(s"DELETE FROM $tableNameAsString WHERE id <= 1")

        // verify the delete was successful
        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Row(3, 2, "hardware") :: Row(4, 3, "hr") :: Nil)

        // verify the view reflects the changes in the table
        checkAnswer(sql("SELECT * FROM temp"), Nil)
      }
    }
  }

  test("delete without condition executed as delete with filters") {
    createAndInitTable("pk INT NOT NULL, id INT, dep INT",
      """{ "pk": 1, "id": 1, "dep": 100 }
        |{ "pk": 2, "id": 2, "dep": 200 }
        |{ "pk": 3, "id": 3, "dep": 100 }
        |""".stripMargin)

    executeDeleteWithFilters(s"DELETE FROM $tableNameAsString")

    checkAnswer(sql(s"SELECT * FROM $tableNameAsString"), Nil)
  }

  test("delete with supported predicates gets converted into delete with filters") {
    createAndInitTable("pk INT NOT NULL, id INT, dep INT",
      """{ "pk": 1, "id": 1, "dep": 100 }
        |{ "pk": 2, "id": 2, "dep": 200 }
        |{ "pk": 3, "id": 3, "dep": 100 }
        |""".stripMargin)

    executeDeleteWithFilters(s"DELETE FROM $tableNameAsString WHERE dep = 100")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, 2, 200) :: Nil)
  }

  test("delete with unsupported predicates cannot be converted into delete with filters") {
    createAndInitTable("pk INT NOT NULL, id INT, dep INT",
      """{ "pk": 1, "id": 1, "dep": 100 }
        |{ "pk": 2, "id": 2, "dep": 200 }
        |{ "pk": 3, "id": 3, "dep": 100 }
        |""".stripMargin)

    executeDeleteWithRewrite(s"DELETE FROM $tableNameAsString WHERE dep = 100 OR dep < 200")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, 2, 200) :: Nil)
  }

  test("delete with subquery cannot be converted into delete with filters") {
    withTempView("deleted_id") {
      createAndInitTable("pk INT NOT NULL, id INT, dep INT",
        """{ "pk": 1, "id": 1, "dep": 100 }
          |{ "pk": 2, "id": 2, "dep": 200 }
          |{ "pk": 3, "id": 3, "dep": 100 }
          |""".stripMargin)

      val deletedIdDF = Seq(Some(1), Some(100), None).toDF()
      deletedIdDF.createOrReplaceTempView("deleted_id")

      val q = s"DELETE FROM $tableNameAsString WHERE dep = 100 AND id IN (SELECT * FROM deleted_id)"
      executeDeleteWithRewrite(q)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(2, 2, 200) :: Row(3, 3, 100) :: Nil)
    }
  }

  private def executeDeleteWithFilters(query: String): Unit = {
    val executedPlan = executeAndKeepPlan {
      sql(query)
    }

    executedPlan match {
      case _: DeleteFromTableExec =>
        // OK
      case other =>
        fail("unexpected executed plan: " + other)
    }
  }

  private def executeDeleteWithRewrite(query: String): Unit = {
    val executedPlan = executeAndKeepPlan {
      sql(query)
    }

    executedPlan match {
      case _: ReplaceDataExec =>
        // OK
      case _: WriteDeltaExec =>
        // OK
      case other =>
        fail("unexpected executed plan: " + other)
    }
  }
}
