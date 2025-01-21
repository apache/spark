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
import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{Column, ColumnDefaultValue}
import org.apache.spark.sql.connector.expressions.LiteralValue
import org.apache.spark.sql.types.{IntegerType, StringType}

abstract class UpdateTableSuiteBase extends RowLevelOperationSuiteBase {

  import testImplicits._

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

    // should update one matching row with a null-safe condition
    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE dep <=> NULL")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, -1, null) :: Row(2, 200, "hr") :: Row(3, 300, "hardware") :: Nil)
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

    // should update one matching row with a null-safe condition
    sql(s"UPDATE $tableNameAsString SET dep = 'invalid' WHERE salary <=> NULL")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, null, "invalid") :: Row(2, 200, "hr") :: Row(3, 300, "hardware") :: Nil)
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

    sql(s"UPDATE $tableNameAsString SET salary = -1 WHERE salary NOT IN (null, 1)")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, -1, "hr") :: Row(2, 200, "hardware") :: Row(3, null, "hr") :: Nil)

    sql(s"UPDATE $tableNameAsString SET salary = 100 WHERE salary NOT IN (1, 10)")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, 100, "hr") :: Row(2, 100, "hardware") :: Row(3, null, "hr") :: Nil)
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

    // set primitive, array, map columns to NULL (proper casts should be in inserted)
    sql(s"UPDATE $tableNameAsString SET s.c1 = NULL, s.c2 = NULL WHERE pk = 1")
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, Row(null, null), "hr") :: Nil)

    // assign an entire struct
    sql(
      s"""UPDATE $tableNameAsString
         |SET s = named_struct('c1', 1, 'c2', named_struct('a', array(1), 'm', null))
         |""".stripMargin)
    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, Row(1, Row(Seq(1), null)), "hr") :: Nil)
  }

  test("update fields inside NULL structs") {
    createAndInitTable("pk INT NOT NULL, s STRUCT<n1: INT, n2: INT>, dep STRING",
      """{ "pk": 1, "s": null, "dep": "hr" }""")

    sql(s"UPDATE $tableNameAsString SET s.n1 = -1 WHERE pk = 1")

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(1, Row(-1, null), "hr") :: Nil)
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

      sql(
        s"""UPDATE $tableNameAsString
           |SET dep = 'invalid'
           |WHERE
           | id NOT IN (SELECT * FROM updated_id WHERE value IS NOT NULL)
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, "invalid") :: Row(2, 2, "invalid") :: Row(3, null, "hr") :: Nil)

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

      sql(
        s"""UPDATE $tableNameAsString t
           |SET dep = 'invalid'
           |WHERE
           | EXISTS (SELECT 1 FROM updated_id d WHERE t.id = d.value + 2)
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, "invalid") :: Row(2, 2, "hardware") :: Row(3, null, "hr") :: Nil)

      sql(
        s"""UPDATE $tableNameAsString t
           |SET dep = 'invalid'
           |WHERE
           | EXISTS (SELECT 1 FROM updated_id d WHERE t.id = d.value) OR t.id IS NULL
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, "invalid") :: Row(2, 2, "hardware") :: Row(3, null, "invalid") :: Nil)

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

      sql(
        s"""UPDATE $tableNameAsString t
           |SET dep = 'invalid'
           |WHERE
           | NOT EXISTS (SELECT 1 FROM updated_id d WHERE t.id = d.value + 2)
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, 1, "hr") :: Row(2, 2, "invalid") :: Row(3, null, "invalid") :: Nil)

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
}
