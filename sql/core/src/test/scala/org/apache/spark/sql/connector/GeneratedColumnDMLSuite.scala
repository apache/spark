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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.connector.catalog.{Column, GenerationExpression, Identifier, InMemoryRowLevelOperationTableCatalog, TableInfo}
import org.apache.spark.sql.connector.expressions.{Cast => V2Cast, Expression => V2Expression, Expressions, FieldReference, GeneralScalarExpression, LiteralValue}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType}

class GeneratedColumnDMLSuite extends QueryTest with SharedSparkSession {

  private val targetTableName = "rowlevelcat.t"

  private def testDML(name: String)(f: Map[String, String] => Unit): Unit = {
    Seq("group", "delta", "split").foreach { mode =>
      test(s"$name ($mode)") {
        withSQLConf("spark.sql.catalog.rowlevelcat" ->
            classOf[InMemoryRowLevelOperationTableCatalog].getName) {
          withTable(targetTableName) {
            f(Map("supports-deltas" -> (mode != "group").toString,
              "split-updates" -> (mode == "split").toString))
          }
        }
      }
    }
  }

  private def createTable(columns: String, properties: Map[String, String]): Unit = {
    val props = properties.map { case (key, value) => s"'$key' = '$value'" }.mkString(", ")
    sql(s"""CREATE TABLE $targetTableName (
           |  pk INT NOT NULL,
           |  part STRING,
           |  $columns
           |) USING foo PARTITIONED BY (part) TBLPROPERTIES ($props)""".stripMargin)
  }

  private def createTable(columns: Array[Column], properties: Map[String, String]): Unit = {
    val catalog = spark.sessionState.catalogManager.catalog("rowlevelcat")
      .asInstanceOf[InMemoryRowLevelOperationTableCatalog]
    val info = new TableInfo.Builder()
      .withColumns(Array(Column.create("pk", IntegerType, false),
        Column.create("part", StringType)) ++ columns)
      .withPartitions(Array(Expressions.identity("part")))
      .withProperties(properties.asJava)
      .build()
    catalog.createTable(Identifier.of(Array.empty[String], "t"), info)
  }

  testDML("UPDATE generates values from stored assignments and preserves other rows") { props =>
    createTable("a INT, g BIGINT GENERATED ALWAYS AS (a * 2)", props)
    sql(s"INSERT INTO $targetTableName (pk, part, a) VALUES (1, 'p', 1), (2, 'p', 2), (3, 'q', 3)")
    sql(s"UPDATE $targetTableName SET a = 5.9 WHERE pk = 1")
    checkAnswer(spark.table(targetTableName),
      Seq(Row(1, "p", 5, 10L), Row(2, "p", 2, 4L), Row(3, "q", 3, 6L)))
  }

  testDML("UPDATE evaluates a nondeterministic assignment once") { props =>
    createTable("a DOUBLE, g DOUBLE GENERATED ALWAYS AS (a + a)", props)
    sql(s"INSERT INTO $targetTableName (pk, part, a) VALUES (1, 'p', 1), (2, 'p', 2)")
    sql(s"UPDATE $targetTableName SET a = rand()")
    val rows = spark.table(targetTableName).collect()
    assert(rows.length == 2)
    rows.foreach { row =>
      assert(row.getDouble(2) >= 0 && row.getDouble(2) < 1)
      assert(row.getDouble(3) == row.getDouble(2) * 2)
    }
  }

  testDML("UPDATE generates values from nested assignments and lambda inputs") { props =>
    createTable(
      "s STRUCT<f: INT>, a ARRAY<INT>, " +
        "g INT GENERATED ALWAYS AS (s.f * 2), " +
        "h ARRAY<INT> GENERATED ALWAYS AS (transform(a, x -> x + s.f))", props)
    sql(s"""INSERT INTO $targetTableName (pk, part, s, a)
           |VALUES (1, 'p', named_struct('f', 1), array(1, 2))""".stripMargin)
    sql(s"UPDATE $targetTableName SET s.f = 5.9, a = array(3.9, 4.9)")
    checkAnswer(spark.table(targetTableName), Row(1, "p", Row(5), Seq(3, 4), 10, Seq(8, 9)))
  }

  testDML("UPDATE generates values from simultaneous assignments and changed row IDs") { props =>
    createTable("a INT, g INT GENERATED ALWAYS AS (pk + a)", props)
    sql(s"INSERT INTO $targetTableName (pk, part, a) VALUES (1, 'p', 10)")
    sql(s"UPDATE $targetTableName SET pk = pk + 1, a = pk + a")
    checkAnswer(spark.table(targetTableName), Row(2, "p", 11, 13))
  }

  testDML("DML assignments use original values regardless of SET order") { props =>
    createTable("a INT, b INT, g INT GENERATED ALWAYS AS (a * 10 + b)", props)
    sql(s"INSERT INTO $targetTableName (pk, part, a, b) VALUES (1, 'p', 1, 2)")
    Seq("a = b, b = a", "b = a, a = b").foreach { assignments =>
      sql(s"UPDATE $targetTableName SET $assignments")
      checkAnswer(spark.table(targetTableName), Row(1, "p", 2, 1, 21))
      sql(s"""MERGE INTO $targetTableName t
             |USING (SELECT 1 AS pk) s ON t.pk = s.pk
             |WHEN MATCHED THEN UPDATE SET $assignments
             |""".stripMargin)
      checkAnswer(spark.table(targetTableName), Row(1, "p", 1, 2, 12))
    }
  }

  testDML("DML preserves original row IDs when their new values are generated") { props =>
    val properties = props.map { case (key, value) => s"'$key' = '$value'" }.mkString(", ")
    sql(s"""CREATE TABLE $targetTableName (
           |  pk INT GENERATED ALWAYS AS (a) NOT NULL, part STRING, a INT NOT NULL
           |) USING foo PARTITIONED BY (part) TBLPROPERTIES ($properties)
           |""".stripMargin)
    sql(s"INSERT INTO $targetTableName (part, a) VALUES ('p', 1)")
    sql(s"UPDATE $targetTableName SET a = 2")
    checkAnswer(spark.table(targetTableName), Row(2, "p", 2))
    sql(s"""MERGE INTO $targetTableName t
           |USING (SELECT 2 AS pk, 3 AS a) s ON t.pk = s.pk
           |WHEN MATCHED THEN UPDATE SET a = s.a
           |""".stripMargin)
    checkAnswer(spark.table(targetTableName), Row(3, "p", 3))
  }

  testDML("DML moves rows to their generated partitions") { props =>
    val properties = props.map { case (key, value) => s"'$key' = '$value'" }.mkString(", ")
    sql(s"""CREATE TABLE $targetTableName (
           |  pk INT NOT NULL, a INT, part INT GENERATED ALWAYS AS (a % 2)
           |) USING foo PARTITIONED BY (part) TBLPROPERTIES ($properties)
           |""".stripMargin)
    sql(s"INSERT INTO $targetTableName (pk, a) VALUES (1, 1), (2, 2)")
    sql(s"UPDATE $targetTableName SET a = 4 WHERE pk = 1")
    checkAnswer(spark.table(targetTableName), Seq(Row(1, 4, 0), Row(2, 2, 0)))
    sql(s"""MERGE INTO $targetTableName t
           |USING (SELECT 2 AS pk, 5 AS a) s ON t.pk = s.pk
           |WHEN MATCHED THEN UPDATE SET a = s.a
           |""".stripMargin)
    checkAnswer(spark.table(targetTableName), Seq(Row(1, 4, 0), Row(2, 5, 1)))
  }

  testDML("MERGE validates supplied generated values regardless of SET order") { props =>
    createTable(
      "a INT, g INT GENERATED ALWAYS AS (a * 2), h INT GENERATED ALWAYS AS (a + 1)", props)
    sql(s"INSERT INTO $targetTableName (pk, part, a) VALUES (1, 'p', 1)")
    Seq("a = s.a, g = s.g", "g = s.g, a = s.a").foreach { assignments =>
      sql(s"""MERGE INTO $targetTableName t
             |USING (SELECT 1 AS pk, 10 AS a, 20 AS g) s ON t.pk = s.pk
             |WHEN MATCHED THEN UPDATE SET $assignments
             |""".stripMargin)
      checkAnswer(spark.table(targetTableName), Row(1, "p", 10, 20, 11))
      val e = intercept[Exception] {
        sql(s"""MERGE INTO $targetTableName t
               |USING (SELECT 1 AS pk, 20 AS a, 999 AS g) s ON t.pk = s.pk
               |WHEN MATCHED THEN UPDATE SET $assignments
               |""".stripMargin)
      }
      assert(e.getMessage.contains("CHECK_CONSTRAINT_VIOLATION"))
      checkAnswer(spark.table(targetTableName), Row(1, "p", 10, 20, 11))
    }
  }

  testDML("MERGE generates values only for the selected action") { props =>
    createTable("a INT, g BIGINT GENERATED ALWAYS AS (100 DIV a)", props)
    sql(s"INSERT INTO $targetTableName (pk, part, a) VALUES (1, 'p', 1), (2, 'p', 2)")
    sql(s"""MERGE INTO $targetTableName t
           |USING (SELECT 1 AS pk, 10 AS a) s ON t.pk = s.pk
           |WHEN MATCHED AND s.a < 0 THEN UPDATE SET a = 0
           |WHEN MATCHED THEN UPDATE SET a = s.a
           |WHEN NOT MATCHED THEN INSERT (pk, part, a) VALUES (s.pk, 'p', 0)
           |""".stripMargin)
    checkAnswer(spark.table(targetTableName), Seq(Row(1, "p", 10, 10L), Row(2, "p", 2, 50L)))
    val e = intercept[Exception] {
      sql(s"""MERGE INTO $targetTableName t
             |USING (SELECT 1 AS pk, -1 AS a) s ON t.pk = s.pk
             |WHEN MATCHED AND s.a < 0 THEN UPDATE SET a = 0
             |WHEN MATCHED THEN UPDATE SET a = s.a
             |""".stripMargin)
    }
    assert(e.getMessage.contains("DIVIDE_BY_ZERO"))
    checkAnswer(spark.table(targetTableName), Seq(Row(1, "p", 10, 10L), Row(2, "p", 2, 50L)))
  }

  testDML("UPDATE validates explicit generated values") { props =>
    createTable(
      "a INT, g INT GENERATED ALWAYS AS (a * 2), h INT GENERATED ALWAYS AS (a + 1)", props)
    sql(s"INSERT INTO $targetTableName (pk, part, a) VALUES (1, 'p', 1)")
    sql(s"UPDATE $targetTableName SET a = 10, g = 20")
    checkAnswer(spark.table(targetTableName), Row(1, "p", 10, 20, 11))
    val e = intercept[Exception] {
      sql(s"UPDATE $targetTableName SET a = 20, g = 999")
    }
    assert(e.getMessage.contains("CHECK_CONSTRAINT_VIOLATION"))
    checkAnswer(spark.table(targetTableName), Row(1, "p", 10, 20, 11))
  }

  testDML("UPDATE with a subquery generates only matching rows") { props =>
    createTable("a INT, g INT GENERATED ALWAYS AS (a * 2)", props)
    sql(s"INSERT INTO $targetTableName (pk, part, a) VALUES (1, 'p', 1), (2, 'p', 2)")
    sql(s"UPDATE $targetTableName SET a = 10 WHERE pk IN (SELECT * FROM VALUES (1) AS s(pk))")
    checkAnswer(spark.table(targetTableName), Seq(Row(1, "p", 10, 20), Row(2, "p", 2, 4)))
  }

  testDML("MERGE generates values for each action and skips deletes") { props =>
    createTable("a INT, g INT GENERATED ALWAYS AS (a * 2), h INT GENERATED ALWAYS AS (1)",
      props)
    sql(s"""INSERT INTO $targetTableName (pk, part, a)
           |VALUES (1, 'p', 1), (2, 'p', 2), (3, 'p', 3), (4, 'p', 4), (5, 'p', 5)
           |""".stripMargin)
    sql(s"""MERGE INTO $targetTableName t
           |USING (SELECT * FROM VALUES (1, 10), (2, -1), (6, 60), (7, 70)
           |  AS v(pk, a)) AS s
           |ON t.pk = s.pk
           |WHEN MATCHED AND s.a < 0 THEN DELETE
           |WHEN MATCHED THEN UPDATE SET a = s.a + 0.9
           |WHEN NOT MATCHED AND s.pk = 6
           |  THEN INSERT (pk, part, a) VALUES (s.pk, 'p', s.a)
           |WHEN NOT MATCHED
           |  THEN INSERT (pk, part, a, g) VALUES (s.pk, 'p', s.a, s.a * 2)
           |WHEN NOT MATCHED BY SOURCE AND t.pk = 3 THEN UPDATE SET a = t.a + 10
           |WHEN NOT MATCHED BY SOURCE AND t.pk = 4 THEN DELETE
           |""".stripMargin)
    checkAnswer(spark.table(targetTableName), Seq(
      Row(1, "p", 10, 20, 1), Row(3, "p", 13, 26, 1), Row(5, "p", 5, 10, 1),
      Row(6, "p", 60, 120, 1), Row(7, "p", 70, 140, 1)))
  }

  testDML("MERGE uses the stored nondeterministic source values") { props =>
    createTable("a DOUBLE, g DOUBLE GENERATED ALWAYS AS (a + a)", props)
    sql(s"INSERT INTO $targetTableName (pk, part, a) VALUES (1, 'p', 1)")
    withSQLConf(SQLConf.RUNTIME_ROW_LEVEL_OPERATION_GROUP_FILTER_ENABLED.key -> "false") {
      sql(s"""MERGE INTO $targetTableName t
             |USING (SELECT pk, rand() AS a FROM VALUES (1), (2) AS v(pk)) s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN UPDATE SET a = s.a
             |WHEN NOT MATCHED THEN INSERT (pk, part, a) VALUES (s.pk, 'p', s.a)
             |""".stripMargin)
    }
    val rows = spark.table(targetTableName).collect()
    assert(rows.length == 2)
    rows.foreach { row =>
      assert(row.getDouble(2) >= 0 && row.getDouble(2) < 1)
      assert(row.getDouble(3) == row.getDouble(2) * 2)
    }
  }

  testDML("DML enforces NOT NULL on generated values") { props =>
    createTable("a INT, g INT GENERATED ALWAYS AS (a + 1) NOT NULL", props)
    sql(s"INSERT INTO $targetTableName (pk, part, a) VALUES (1, 'p', 1)")
    sql(s"UPDATE $targetTableName SET a = 10")
    checkAnswer(spark.table(targetTableName), Row(1, "p", 10, 11))
    val updateError = intercept[Exception] {
      sql(s"UPDATE $targetTableName SET a = NULL")
    }
    assert(updateError.getMessage.contains("NOT_NULL_ASSERT_VIOLATION"))
    val mergeError = intercept[Exception] {
      sql(s"""MERGE INTO $targetTableName t
             |USING (SELECT * FROM VALUES (2)
             |  AS v(pk)) AS s ON t.pk = s.pk
             |WHEN NOT MATCHED THEN INSERT (pk, part, a) VALUES (s.pk, 'p', NULL)
             |""".stripMargin)
    }
    assert(mergeError.getMessage.contains("NOT_NULL_ASSERT_VIOLATION"))
    checkAnswer(spark.table(targetTableName), Row(1, "p", 10, 11))
  }

  testDML("DML resolves dotted column names and case-insensitive assignments") { props =>
    createTable("`A.b` INT, `G.c` INT GENERATED ALWAYS AS (`A.b` * 2)", props)
    sql(s"INSERT INTO $targetTableName (pk, part, `a.B`) VALUES (1, 'p', 1)")
    sql(s"UPDATE $targetTableName SET `a.B` = 10")
    sql(s"""MERGE INTO $targetTableName t
           |USING (SELECT * FROM VALUES (1, 20), (2, 30)
           |  AS v(pk, a)) AS s ON t.pk = s.pk
           |WHEN MATCHED THEN UPDATE SET `a.B` = s.a
           |WHEN NOT MATCHED THEN INSERT (pk, part, `a.B`) VALUES (s.pk, 'p', s.a)
           |""".stripMargin)
    checkAnswer(spark.table(targetTableName), Seq(Row(1, "p", 20, 40), Row(2, "p", 30, 60)))
  }

  Seq(false, true).foreach { multipleActions =>
    testDML(s"insert-only MERGE generates values (multiple actions: $multipleActions)") { props =>
      createTable("a INT, g INT GENERATED ALWAYS AS (a * 2)", props)
      sql(s"INSERT INTO $targetTableName (pk, part, a) VALUES (1, 'p', 1)")
      val firstAction = if (multipleActions) {
        "WHEN NOT MATCHED AND s.pk = 2 " +
          "THEN INSERT (pk, part, a, g) VALUES (s.pk, 'p', s.a, s.a * 2)"
      } else {
        ""
      }
      sql(s"""MERGE INTO $targetTableName t
             |USING (SELECT * FROM VALUES (1, 10), (2, 20), (3, 30)
             |  AS v(pk, a)) AS s
             |ON t.pk = s.pk
             |$firstAction
             |WHEN NOT MATCHED THEN INSERT (pk, part, a) VALUES (s.pk, 'p', s.a)
             |""".stripMargin)
      checkAnswer(spark.table(targetTableName),
        Seq(Row(1, "p", 1, 2), Row(2, "p", 20, 40), Row(3, "p", 30, 60)))
    }
  }

  testDML("MERGE star actions generate columns omitted from the source") { props =>
    createTable("a INT, g INT GENERATED ALWAYS AS (a * 2)", props)
    sql(s"INSERT INTO $targetTableName (pk, part, a) VALUES (1, 'p', 1)")
    sql(s"""MERGE INTO $targetTableName t
           |USING (SELECT * FROM VALUES (1, 'p', 10), (2, 'p', 20)
           |  AS v(pk, part, a)) AS s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN UPDATE SET *
           |WHEN NOT MATCHED THEN INSERT *
           |""".stripMargin)
    checkAnswer(spark.table(targetTableName), Seq(Row(1, "p", 10, 20), Row(2, "p", 20, 40)))
  }

  testDML("MERGE star actions validate generated columns supplied by the source") { props =>
    createTable("a INT, g INT GENERATED ALWAYS AS (a * 2)", props)
    sql(s"INSERT INTO $targetTableName (pk, part, a) VALUES (1, 'p', 1)")
    val e = intercept[Exception] {
      sql(s"""MERGE INTO $targetTableName t
             |USING (SELECT * FROM VALUES (1, 'p', 10, 999)
             |  AS v(pk, part, a, g)) AS s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN UPDATE SET *
             |WHEN NOT MATCHED THEN INSERT *
             |""".stripMargin)
    }
    assert(e.getMessage.contains("CHECK_CONSTRAINT_VIOLATION"))
    checkAnswer(spark.table(targetTableName), Row(1, "p", 1, 2))
  }

  testDML("MERGE validates explicit values when another action generates the column") { props =>
    createTable("a INT, g INT GENERATED ALWAYS AS (a * 2)", props)
    sql(s"INSERT INTO $targetTableName (pk, part, a) VALUES (1, 'p', 1)")
    val e = intercept[Exception] {
      sql(s"""MERGE INTO $targetTableName t
             |USING (SELECT * FROM VALUES (1, 10), (2, 20) AS v(pk, a)) s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN UPDATE SET a = s.a
             |WHEN NOT MATCHED THEN INSERT (pk, part, a, g) VALUES (s.pk, 'p', s.a, 999)
             |""".stripMargin)
    }
    assert(e.getMessage.contains("CHECK_CONSTRAINT_VIOLATION"))
    checkAnswer(spark.table(targetTableName), Row(1, "p", 1, 2))
  }

  testDML("MERGE generates values from defaults and handles nulls") { props =>
    createTable(
      "a INT DEFAULT 5, g INT GENERATED ALWAYS AS (a * 2), " +
        "h INT GENERATED ALWAYS AS (coalesce(a, 0) + 1)", props)
    sql(s"""MERGE INTO $targetTableName t
           |USING (SELECT * FROM VALUES (1), (2)
           |  AS v(pk)) AS s
           |ON t.pk = s.pk
           |WHEN NOT MATCHED AND s.pk = 1 THEN INSERT (pk, part) VALUES (s.pk, 'p')
           |WHEN NOT MATCHED THEN INSERT (pk, part, a) VALUES (s.pk, 'p', NULL)
           |""".stripMargin)
    checkAnswer(spark.table(targetTableName),
      Seq(Row(1, "p", 5, 10, 6), Row(2, "p", null, null, 1)))
  }

  testDML("DML generation uses stored expression settings and hides internal metadata") { props =>
    createTable("a STRING, g STRING GENERATED ALWAYS AS (concat(a, 'x\\\\y'))", props)
    sql(s"INSERT INTO $targetTableName (pk, part, a) VALUES (1, 'p', 'a')")
    withSQLConf(SQLConf.ESCAPED_STRING_LITERALS.key -> "true") {
      sql(s"UPDATE $targetTableName SET a = 'b'")
      sql(s"""MERGE INTO $targetTableName t
             |USING (SELECT * FROM VALUES (2, 'c')
             |  AS v(pk, a)) AS s ON t.pk = s.pk
             |WHEN NOT MATCHED THEN INSERT (pk, part, a) VALUES (s.pk, 'p', s.a)
             |""".stripMargin)
    }
    checkAnswer(spark.table(targetTableName),
      Seq(Row(1, "p", "b", "bx\\y"), Row(2, "p", "c", "cx\\y")))
    assert(spark.table(targetTableName).schema.fields.forall(_.metadata.json == "{}"))
  }

  testDML("DML generates values from stored CHAR values after padding") { props =>
    createTable("a CHAR(4), g STRING GENERATED ALWAYS AS (a)", props)
    sql(s"INSERT INTO $targetTableName (pk, part, a) VALUES (1, 'p', 'ab')")
    sql(s"UPDATE $targetTableName SET a = 'cd'")
    checkAnswer(spark.table(targetTableName), Row(1, "p", "cd  ", "cd  "))
    sql(s"""MERGE INTO $targetTableName t
           |USING (SELECT * FROM VALUES (1, 'e'), (2, 'f')
           |  AS v(pk, a)) AS s ON t.pk = s.pk
           |WHEN MATCHED THEN UPDATE SET a = s.a
           |WHEN NOT MATCHED THEN INSERT (pk, part, a) VALUES (s.pk, 'p', s.a)
           |""".stripMargin)
    checkAnswer(spark.table(targetTableName),
      Seq(Row(1, "p", "e   ", "e   "), Row(2, "p", "f   ", "f   ")))
  }

  testDML("DML leaves generation to connectors without the write capability") { props =>
    createTable("a INT, g INT GENERATED ALWAYS AS (a * 2)",
      props + ("generate-column-values-on-write" -> "false"))
    sql(s"INSERT INTO $targetTableName VALUES (1, 'p', 1, 999)")
    sql(s"UPDATE $targetTableName SET a = 2")
    sql(s"""MERGE INTO $targetTableName t
           |USING (SELECT * FROM VALUES (1, 10), (2, 20)
           |  AS v(pk, a)) AS s ON t.pk = s.pk
           |WHEN MATCHED THEN UPDATE SET a = s.a
           |WHEN NOT MATCHED THEN INSERT (pk, part, a) VALUES (s.pk, 'p', s.a)
           |""".stripMargin)
    checkAnswer(spark.table(targetTableName), Seq(Row(1, "p", 10, 999), Row(2, "p", 20, null)))
  }

  testDML("DML resolves SQL session fallback against assigned values") { props =>
    createTable(Array(
      Column.create("a", BooleanType),
      Column.builderFor("g", BooleanType).generationExpression("a = 1").build()), props)
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false",
        SQLConf.USE_CURRENT_SQL_CONFIGS_FOR_GENERATED_COLUMN.key -> "false") {
      sql(s"INSERT INTO $targetTableName (pk, part, a) VALUES (1, 'p', true)")
      sql(s"UPDATE $targetTableName SET a = NOT a")
      checkAnswer(spark.table(targetTableName), Row(1, "p", false, false))
      sql(s"""MERGE INTO $targetTableName t
             |USING (SELECT * FROM VALUES (1, true), (2, false) AS v(pk, a)) s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN UPDATE SET a = s.a
             |WHEN NOT MATCHED THEN INSERT (pk, part, a) VALUES (s.pk, 'p', s.a)
             |""".stripMargin)
      checkAnswer(spark.table(targetTableName),
        Seq(Row(1, "p", true, true), Row(2, "p", false, false)))
    }
  }

  Seq("modeled", "both", "invalid SQL", "unconvertible", "malformed").foreach { form =>
    testDML(s"DML shares modeled expression selection with INSERT ($form)") { props =>
      val children = Array[V2Expression](
        FieldReference(Seq("a.b")), LiteralValue(100, IntegerType))
      val modeled = form match {
        case "unconvertible" => new GeneralScalarExpression("NOT_A_FUNCTION", children)
        case "malformed" => new GeneralScalarExpression("WIDTH_BUCKET", children)
        case _ => new GeneralScalarExpression("+", children)
      }
      val generation = form match {
        case "modeled" => new GenerationExpression(modeled)
        case "invalid SQL" => new GenerationExpression("NOT VALID SQL", modeled)
        case _ => new GenerationExpression("`a.b` + 1", modeled)
      }
      createTable(Array(
        Column.create("a.b", IntegerType),
        Column.builderFor("g", IntegerType).generationExpression(generation).build()), props)
      val offset = if (form == "unconvertible" || form == "malformed") 1 else 100
      sql(s"INSERT INTO $targetTableName (pk, part, `a.b`) VALUES (1, 'p', 1)")
      sql(s"UPDATE $targetTableName SET `a.b` = 10.9")
      checkAnswer(spark.table(targetTableName), Row(1, "p", 10, 10 + offset))
      sql(s"""MERGE INTO $targetTableName t
             |USING (SELECT * FROM VALUES (1, 20), (2, 30), (3, 40) AS v(pk, `a.b`)) s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN UPDATE SET `a.b` = s.`a.b`
             |WHEN NOT MATCHED AND s.pk = 2 THEN INSERT (pk, part, `a.b`, g)
             |  VALUES (s.pk, 'p', s.`a.b`, s.`a.b` + $offset)
             |WHEN NOT MATCHED THEN INSERT (pk, part, `a.b`)
             |  VALUES (s.pk, 'p', s.`a.b`)
             |""".stripMargin)
      sql(s"UPDATE $targetTableName SET g = `a.b` + $offset")
      val e = intercept[Exception] {
        sql(s"UPDATE $targetTableName SET g = `a.b` + ${101 - offset}")
      }
      assert(e.getMessage.contains("CHECK_CONSTRAINT_VIOLATION"))
      checkAnswer(spark.table(targetTableName), Seq(
        Row(1, "p", 20, 20 + offset), Row(2, "p", 30, 30 + offset),
        Row(3, "p", 40, 40 + offset)))
    }
  }

  testDML("DML does not fall back to SQL after modeled expression analysis fails") { props =>
    createTable(Array(
      Column.create("a", IntegerType),
      Column.builderFor("g", IntegerType).generationExpression(
        new GenerationExpression("a + 1", FieldReference(Seq("missing")))).build()), props)
    Seq(
      s"UPDATE $targetTableName SET a = 1",
      s"UPDATE $targetTableName SET a = 1, g = 2",
      s"""MERGE INTO $targetTableName t USING (SELECT 1 AS pk) s ON t.pk = s.pk
         |WHEN MATCHED THEN UPDATE SET a = 1
         |WHEN NOT MATCHED THEN INSERT (pk, part, a) VALUES (s.pk, 'p', 1)
         |""".stripMargin).foreach { statement =>
      val e = intercept[AnalysisException] { sql(statement) }
      assert(e.getCondition == "INVALID_GENERATED_COLUMN_EXPRESSION")
      assert(e.getCause.asInstanceOf[AnalysisException].getCondition ==
        "UNRESOLVED_COLUMN.WITH_SUGGESTION")
    }
  }

  testDML("DML does not use session coercion for modeled expressions") { props =>
    val modeled = new GeneralScalarExpression("=", Array[V2Expression](
      FieldReference(Seq("a")), LiteralValue(1, IntegerType)))
    createTable(Array(
      Column.create("a", BooleanType),
      Column.builderFor("g", BooleanType)
        .generationExpression(new GenerationExpression("true", modeled)).build()), props)
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false",
        SQLConf.USE_CURRENT_SQL_CONFIGS_FOR_GENERATED_COLUMN.key -> "true") {
      Seq(
        s"UPDATE $targetTableName SET a = true",
        s"UPDATE $targetTableName SET a = true, g = true",
        s"""MERGE INTO $targetTableName t USING (SELECT 1 AS pk) s ON t.pk = s.pk
           |WHEN MATCHED THEN UPDATE SET a = true
           |WHEN NOT MATCHED THEN INSERT (pk, part, a) VALUES (s.pk, 'p', true)
           |""".stripMargin).foreach { statement =>
        val e = intercept[AnalysisException] { sql(statement) }
        assert(e.getCondition == "INVALID_GENERATED_COLUMN_EXPRESSION")
        assert(e.getCause.asInstanceOf[AnalysisException].getCondition ==
          "DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES")
      }
    }
  }

  testDML("DML keeps modeled ANSI errors without falling back to SQL") { props =>
    val modeled = new GeneralScalarExpression("+", Array[V2Expression](
      new V2Cast(FieldReference(Seq("a")), StringType, IntegerType), LiteralValue(1, IntegerType)))
    createTable(Array(
      Column.create("a", StringType),
      Column.builderFor("g", IntegerType)
        .generationExpression(new GenerationExpression("1", modeled)).build()), props)
    sql(s"INSERT INTO $targetTableName (pk, part, a) VALUES (1, 'p', '1')")
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false",
        SQLConf.USE_CURRENT_SQL_CONFIGS_FOR_GENERATED_COLUMN.key -> "true") {
      Seq("bad" -> "CAST_INVALID_INPUT", "2147483647" -> "ARITHMETIC_OVERFLOW")
        .foreach { case (value, error) =>
          Seq(
            s"UPDATE $targetTableName SET a = '$value'",
            s"UPDATE $targetTableName SET a = '$value', g = 1",
            s"""MERGE INTO $targetTableName t
               |USING (SELECT * FROM VALUES (1), (2) AS v(pk)) s ON t.pk = s.pk
               |WHEN MATCHED THEN UPDATE SET a = '$value'
               |WHEN NOT MATCHED THEN INSERT (pk, part, a) VALUES (s.pk, 'p', '$value')
               |""".stripMargin).foreach { statement =>
            val e = intercept[Exception] { sql(statement) }
            assert(e.getMessage.contains(error))
          }
        }
    }
    checkAnswer(spark.table(targetTableName), Row(1, "p", "1", 2))
  }
}
