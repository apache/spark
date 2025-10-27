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
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, In, Not}
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column, ColumnDefaultValue, InMemoryTable, TableInfo}
import org.apache.spark.sql.connector.expressions.{GeneralScalarExpression, LiteralValue}
import org.apache.spark.sql.connector.write.MergeSummary
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.MergeRowsExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, CartesianProductExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BooleanType, IntegerType, LongType, MapType, StringType, StructField, StructType}

abstract class MergeIntoTableSuiteBase extends RowLevelOperationSuiteBase
  with AdaptiveSparkPlanHelper {

  import testImplicits._

  protected def deltaMerge: Boolean = false

  test("merge into table with expression-based default values") {
    val columns = Array(
      Column.create("pk", IntegerType),
      Column.create("salary", IntegerType),
      Column.create("dep", StringType),
      Column.create(
        "value",
        IntegerType,
        false, /* not nullable */
        null, /* no comment */
        new ColumnDefaultValue(
          new GeneralScalarExpression(
            "+",
            Array(LiteralValue(100, IntegerType), LiteralValue(23, IntegerType))),
          LiteralValue(123, IntegerType)),
        "{}"))
    val tableInfo = new TableInfo.Builder().withColumns(columns).build()
    catalog.createTable(ident, tableInfo)

    withTempView("source") {
      val sourceRows = Seq(
        (1, 500, "eng"),
        (2, 600, "hr"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      sql(s"INSERT INTO $tableNameAsString (pk, salary, dep, value) VALUES (1, 200, 'eng', 999)")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET value = DEFAULT
           |WHEN NOT MATCHED THEN
           | INSERT (pk, salary, dep) VALUES (s.pk, s.salary, s.dep)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 200, "eng", 123), // update
          Row(2, 600, "hr", 123))) // insert
    }
  }

  test("merge into table containing added column with default value") {
    withTempView("source") {
      sql(
        s"""CREATE TABLE $tableNameAsString (
           | pk INT NOT NULL,
           | salary INT NOT NULL DEFAULT -1,
           | dep STRING)
           |PARTITIONED BY (dep)
           |""".stripMargin)

      append("pk INT NOT NULL, dep STRING",
        """{ "pk": 1, "dep": "hr" }
          |{ "pk": 2, "dep": "hr" }
          |{ "pk": 3, "dep": "hr" }
          |""".stripMargin)

      sql(s"ALTER TABLE $tableNameAsString ADD COLUMN txt STRING DEFAULT 'initial-text'")

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, -1, "hr", "initial-text"),
          Row(2, -1, "hr", "initial-text"),
          Row(3, -1, "hr", "initial-text")))

      val sourceRows = Seq(
        (1, 100, "hr"),
        (4, 400, "hr"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET t.salary = s.salary, t.txt = DEFAULT
           |WHEN NOT MATCHED THEN
           | INSERT (pk, salary, dep) VALUES (s.pk, DEFAULT, s.dep)
           |WHEN NOT MATCHED BY SOURCE THEN
           | UPDATE SET salary = DEFAULT
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 100, "hr", "initial-text"),
          Row(2, -1, "hr", "initial-text"),
          Row(3, -1, "hr", "initial-text"),
          Row(4, -1, "hr", "initial-text")))
    }
  }

  test("SPARK-45974: merge into non filter attributes table") {
    val tableName: String = "cat.ns1.non_partitioned_table"
    withTable(tableName) {
      withTempView("source") {
        val sourceRows = Seq(
          (1, 100, "hr"),
          (2, 200, "finance"),
          (3, 300, "hr"))
        sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

        sql(s"CREATE TABLE $tableName (pk INT NOT NULL, salary INT, dep STRING)".stripMargin)

        val df = sql(
          s"""MERGE INTO $tableName t
             |USING (select * from source) s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | UPDATE SET t.salary = s.salary
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)

        checkAnswer(
          sql(s"SELECT * FROM $tableName"),
          Seq(
            Row(1, 100, "hr"), // insert
            Row(2, 200, "finance"), // insert
            Row(3, 300, "hr"))) // insert
      }
    }
  }

  test("merge into empty table with NOT MATCHED clause") {
    withTempView("source") {
      createTable("pk INT NOT NULL, salary INT, dep STRING")

      val sourceRows = Seq(
        (1, 100, "hr"),
        (2, 200, "finance"),
        (3, 300, "hr"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN NOT MATCHED THEN
           | INSERT *
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 100, "hr"), // insert
          Row(2, 200, "finance"), // insert
          Row(3, 300, "hr"))) // insert
    }
  }

  test("merge into empty table with conditional NOT MATCHED clause") {
    withTempView("source") {
      createTable("pk INT NOT NULL, salary INT, dep STRING")

      val sourceRows = Seq(
        (1, 100, "hr"),
        (2, 200, "finance"),
        (3, 300, "hr"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN NOT MATCHED AND s.pk >= 2 THEN
           | INSERT *
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(2, 200, "finance"), // insert
          Row(3, 300, "hr"))) // insert
    }
  }

  test("merge into empty table with multiple NOT MATCHED clause") {
    withTempView("source") {
      createTable("pk INT NOT NULL, salary INT, dep STRING")

      val sourceRows = Seq(
        (1, 100, "hr"),
        (2, 200, "finance"),
        (3, 300, "hr"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN NOT MATCHED AND s.pk >= 2 THEN
           | INSERT *
           |WHEN NOT MATCHED THEN
           | INSERT *
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 100, "hr"), // insert
          Row(2, 200, "finance"), // insert
          Row(3, 300, "hr"))) // insert
    }
  }

  test("merge into with conditional WHEN MATCHED clause (update)") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "corrupted" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, 100, "software"),
        (2, 200, "finance"),
        (3, 300, "software"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED AND s.pk = 2 THEN
           | UPDATE SET *
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 100, "hr"), // unchanged
          Row(2, 200, "finance"))) // update
    }
  }

  test("merge into with conditional WHEN MATCHED clause (delete)") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "corrupted" }
          |""".stripMargin)

      Seq(1, 2, 3).toDF("pk").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED AND t.salary = 200 THEN
           | DELETE
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(Row(1, 100, "hr"))) // unchanged
    }
  }

  test("merge into with assignments to primary key in NOT MATCHED BY SOURCE") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "finance" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, 100, "software"),
        (5, 500, "finance"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET t.salary = -1
           |WHEN NOT MATCHED BY SOURCE THEN
           | UPDATE SET t.pk = -1
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, -1, "hr"), // update (matched)
          Row(-1, 200, "finance"))) // update (not matched by source)
    }
  }

  test("merge into with assignments to primary key in MATCHED") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "finance" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, 100, "software"),
        (5, 500, "finance"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET t.pk = -1
           |WHEN NOT MATCHED BY SOURCE THEN
           | UPDATE SET t.salary = -1
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(-1, 100, "hr"), // update (matched)
          Row(2, -1, "finance"))) // update (not matched by source)
    }
  }

  test("merge with all types of clauses") {
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
           |WHEN NOT MATCHED BY SOURCE THEN
           | DELETE
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(3, 301, "hr"), // update
          Row(4, 401, "hr"), // update
          Row(5, 501, "hr"), // update
          Row(6, 0, "new"))) // insert
    }
  }

  test("merge with all types of clauses (update and insert star)") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, 101, "support"),
        (2, 201, "support"),
        (4, 401, "support"),
        (5, 501, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED AND t.pk = 1 THEN
           | UPDATE SET *
           |WHEN NOT MATCHED AND s.pk = 4 THEN
           | INSERT *
           |WHEN NOT MATCHED BY SOURCE AND t.pk = t.salary / 100 THEN
           | DELETE
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 101, "support"), // update
          Row(2, 200, "software"), // unchanged
          Row(4, 401, "support"))) // insert
    }
  }

  test("merge with all types of conditional clauses") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |{ "pk": 4, "salary": 400, "dep": "hr" }
          |{ "pk": 5, "salary": 500, "dep": "hr" }
          |""".stripMargin)

      val sourceDF = Seq(3, 4, 5, 6, 7).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED AND t.pk = 4 THEN
           | UPDATE SET t.salary = t.salary + 1
           |WHEN NOT MATCHED AND pk = 6 THEN
           | INSERT (pk, salary, dep) VALUES (s.pk, 0, 'new')
           |WHEN NOT MATCHED BY SOURCE AND salary = 100 THEN
           | DELETE
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(2, 200, "software"), // unchanged
          Row(3, 300, "hr"), // unchanged
          Row(4, 401, "hr"), // update
          Row(5, 500, "hr"), // unchanged
          Row(6, 0, "new"))) // insert
    }
  }

  test("merge with one NOT MATCHED BY SOURCE clause") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |""".stripMargin)

      val sourceDF = Seq(1, 2).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN NOT MATCHED BY SOURCE THEN
           | DELETE
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 100, "hr"), // unchanged
          Row(2, 200, "software"))) // unchanged
    }
  }

  test("merge with one conditional NOT MATCHED BY SOURCE clause") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |""".stripMargin)

      val sourceDF = Seq(2).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN NOT MATCHED BY SOURCE AND salary = 100 THEN
           | UPDATE SET salary = -1
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, -1, "hr"), // updated
          Row(2, 200, "software"), // unchanged
          Row(3, 300, "hr"))) // unchanged
    }
  }

  test("merge with MATCHED and NOT MATCHED BY SOURCE clauses") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |""".stripMargin)

      val sourceDF = Seq(2).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | DELETE
           |WHEN NOT MATCHED BY SOURCE AND salary = 100 THEN
           | UPDATE SET salary = -1
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, -1, "hr"), // updated
          Row(3, 300, "hr"))) // unchanged
    }
  }

  test("merge with NOT MATCHED and NOT MATCHED BY SOURCE clauses") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |""".stripMargin)

      val sourceDF = Seq(2, 3, 4).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN NOT MATCHED THEN
           | INSERT (pk, salary, dep) VALUES (pk, -1, 'new')
           |WHEN NOT MATCHED BY SOURCE THEN
           | DELETE
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(2, 200, "software"), // unchanged
          Row(3, 300, "hr"), // unchanged
          Row(4, -1, "new"))) // insert
    }
  }

  test("merge with multiple NOT MATCHED BY SOURCE clauses") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |""".stripMargin)

      val sourceDF = Seq(5, 6, 7).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN NOT MATCHED BY SOURCE AND salary = 100 THEN
           | UPDATE SET salary = salary + 1
           |WHEN NOT MATCHED BY SOURCE AND salary = 300 THEN
           | DELETE
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 101, "hr"), // update
          Row(2, 200, "software"))) // unchanged
    }
  }

  test("merge with MATCHED BY SOURCE clause and NULL values") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
        """{ "pk": 1, "id": null, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "id": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "id": 3, "salary": 300, "dep": "hr" }
          |""".stripMargin)

      val sourceRows = Seq(
        (2, 2, 201, "support"),
        (1, 1, 101, "support"),
        (3, 3, 301, "support"))
      sourceRows.toDF("pk", "id", "salary", "dep").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.id = s.id AND t.id < 3
           |WHEN MATCHED THEN
           | UPDATE SET *
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, null, 100, "hr"), // unchanged
          Row(2, 2, 201, "support"), // update
          Row(3, 3, 300, "hr"))) // unchanged
    }
  }

  test("merge with CTE") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (0, 101, "support"),
        (2, 301, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      sql(
        s"""WITH cte1 AS (SELECT pk + 1 as pk, salary, dep FROM source)
           |MERGE INTO $tableNameAsString AS t
           |USING cte1 AS s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET *
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 101, "support"), // unchanged
          Row(2, 200, "software"))) // unchanged
    }
  }

  test("merge with subquery as source") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 6, "salary": 600, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (2, 201, "support"),
        (1, 101, "support"),
        (3, 301, "support"),
        (6, 601, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      val subquery =
        s"""
           |SELECT * FROM source WHERE pk = 2
           |UNION ALL
           |SELECT * FROM source WHERE pk = 1 OR pk = 6
           |""".stripMargin

      sql(
        s"""MERGE INTO $tableNameAsString AS t
           |USING ($subquery) AS s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET *
           |WHEN NOT MATCHED THEN
           | INSERT *
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 101, "support"), // update
          Row(2, 201, "support"), // insert
          Row(6, 601, "support"))) // update
    }
  }

  test("merge cardinality check with conditional MATCHED clause (delete)") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 6, "salary": 600, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, 101, "support"),
        (1, 102, "support"),
        (2, 201, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      assertCardinalityError(
        s"""MERGE INTO $tableNameAsString AS t
           |USING source AS s
           |ON t.pk = s.pk
           |WHEN MATCHED AND s.salary = 101 THEN
           | DELETE
           |""".stripMargin)
    }
  }

  test("merge cardinality check with unconditional MATCHED clause (delete)") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 6, "salary": 600, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, 101, "support"),
        (1, 102, "support"),
        (2, 201, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString AS t
           |USING source AS s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | DELETE
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(Row(6, 600, "software"))) // unchanged
    }
  }

  test("merge cardinality check with only NOT MATCHED clauses") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 6, "salary": 600, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, 101, "support"),
        (1, 102, "support"),
        (2, 201, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString AS t
           |USING source AS s
           |ON t.pk = s.pk
           |WHEN NOT MATCHED THEN
           | INSERT *
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 100, "hr"), // unchanged
          Row(2, 201, "support"), // insert
          Row(6, 600, "software"))) // unchanged
    }
  }

  test("merge cardinality check with small target and large source (broadcast enabled)") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = (1 to 1000).map(pk => (pk, pk * 100, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> Long.MaxValue.toString) {
        assertCardinalityError(
          s"""MERGE INTO $tableNameAsString AS t
             |USING (SELECT * FROM source UNION ALL SELECT * FROM source) AS s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | UPDATE SET *
             |""".stripMargin)

        assertNoLeftBroadcastOrReplication(
          s"""MERGE INTO $tableNameAsString AS t
             |USING source AS s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | UPDATE SET *
             |""".stripMargin)

        assert(sql(s"SELECT * FROM $tableNameAsString").count() == 2)
      }
    }
  }

  test("merge cardinality check with small target and large source (broadcast disabled)") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = (1 to 1000).map(pk => (pk, pk * 100, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        assertCardinalityError(
          s"""MERGE INTO $tableNameAsString AS t
             |USING (SELECT * FROM source UNION ALL SELECT * FROM source) AS s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | UPDATE SET *
             |""".stripMargin)

        assertNoLeftBroadcastOrReplication(
          s"""MERGE INTO $tableNameAsString AS t
             |USING source AS s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | UPDATE SET *
             |""".stripMargin)

        assert(sql(s"SELECT * FROM $tableNameAsString").count() == 2)
      }
    }
  }

  test("merge cardinality check with small target and large source (shuffle hash enabled)") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = (1 to 1000).map(pk => (pk, pk * 100, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      withSQLConf(
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.PREFER_SORTMERGEJOIN.key -> "false") {

        assertCardinalityError(
          s"""MERGE INTO $tableNameAsString AS t
             |USING (SELECT * FROM source UNION ALL SELECT * FROM source) AS s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | UPDATE SET *
             |""".stripMargin)

        assertNoLeftBroadcastOrReplication(
          s"""MERGE INTO $tableNameAsString AS t
             |USING source AS s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | UPDATE SET *
             |""".stripMargin)

        assert(sql(s"SELECT * FROM $tableNameAsString").count() == 2)
      }
    }
  }

  test("merge cardinality check without equality condition and only MATCHED clauses") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = (1 to 1000).map(pk => (pk, pk * 100, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        assertCardinalityError(
          s"""MERGE INTO $tableNameAsString AS t
             |USING (SELECT * FROM source UNION ALL SELECT * FROM source) AS s
             |ON t.pk > s.pk
             |WHEN MATCHED THEN
             | UPDATE SET *
             |""".stripMargin)

        assert(sql(s"SELECT * FROM $tableNameAsString").count() == 2)
      }
    }
  }

  test("merge cardinality check without equality condition") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = (1 to 1000).map(pk => (pk, pk * 100, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        assertCardinalityError(
          s"""MERGE INTO $tableNameAsString AS t
             |USING (SELECT * FROM source UNION ALL SELECT * FROM source) AS s
             |ON t.pk > s.pk
             |WHEN MATCHED THEN
             | UPDATE SET *
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)

        assert(sql(s"SELECT * FROM $tableNameAsString").count() == 2)
      }
    }
  }

  test("self merge") {
    createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
      """{ "pk": 1, "salary": 100, "dep": "hr" }
        |{ "pk": 2, "salary": 200, "dep": "software" }
        |{ "pk": 3, "salary": 300, "dep": "hr" }
        |""".stripMargin)

    sql(
      s"""MERGE INTO $tableNameAsString t
         |USING $tableNameAsString s
         |ON t.pk = s.pk
         |WHEN MATCHED AND t.salary = 100 THEN
         | UPDATE SET salary = t.salary + 1
         |WHEN NOT MATCHED THEN
         | INSERT *
         |""".stripMargin)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, 101, "hr"), // update
        Row(2, 200, "software"), // unchanged
        Row(3, 300, "hr"))) // unchanged
  }

  test("merge with self subquery") {
    withTempView("ids") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |""".stripMargin)

      Seq(1, 2).toDF("value").createOrReplaceTempView("ids")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING (SELECT pk FROM $tableNameAsString r JOIN ids ON r.pk = ids.value) s
           |ON t.pk = s.pk
           |WHEN MATCHED AND t.salary = 100 THEN
           | UPDATE SET salary = t.salary + 1
           |WHEN NOT MATCHED THEN
           | INSERT (dep, salary, pk) VALUES ('new', 300, 1)
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 101, "hr"), // update
          Row(2, 200, "software"), // unchanged
          Row(3, 300, "hr"))) // unchanged
    }
  }

  test("merge with extra columns in source") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, "smth", 101, "support"),
        (2, "smth", 201, "support"),
        (4, "smth", 401, "support"))
      sourceRows.toDF("pk", "extra", "salary", "dep").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET salary = s.salary + 1
           |WHEN NOT MATCHED THEN
           | INSERT (pk, salary, dep) VALUES (s.pk, s.salary, s.dep)
           |WHEN NOT MATCHED BY SOURCE THEN
           | DELETE
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 102, "hr"), // update
          Row(2, 202, "software"), // update
          Row(4, 401, "support"))) // insert
    }
  }

  test("merge with NULL values in target and source") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
        """{ "pk": 1, "id": null, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "id": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (5, None, 501, "support"),
        (6, Some(6), 601, "support"))
      sourceRows.toDF("pk", "id", "salary", "dep").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.id = s.id
           |WHEN MATCHED THEN
           | UPDATE SET *
           |WHEN NOT MATCHED THEN
           | INSERT *
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, null, 100, "hr"), // unchanged
          Row(2, 2, 200, "software"), // unchanged
          Row(5, null, 501, "support"), // insert
          Row(6, 6, 601, "support"))) // insert
    }
  }

  test("merge with <=>") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
        """{ "pk": 1, "id": null, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "id": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (5, None, 501, "support"),
        (6, Some(6), 601, "support"))
      sourceRows.toDF("pk", "id", "salary", "dep").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.id <=> s.id
           |WHEN MATCHED THEN
           | UPDATE SET *
           |WHEN NOT MATCHED THEN
           | INSERT *
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(2, 2, 200, "software"), // unchanged
          Row(5, null, 501, "support"), // updated
          Row(6, 6, 601, "support"))) // insert
    }
  }

  test("merge with NULL ON condition") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, id INT, salary INT, dep STRING",
        """{ "pk": 1, "id": null, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "id": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (5, None, 501, "support"),
        (6, Some(2), 201, "support"))
      sourceRows.toDF("pk", "id", "salary", "dep").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.id = s.id AND NULL
           |WHEN MATCHED THEN
           | UPDATE SET salary = s.salary
           |WHEN NOT MATCHED THEN
           | INSERT *
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, null, 100, "hr"), // unchanged
          Row(2, 2, 200, "software"), // unchanged
          Row(5, null, 501, "support"), // new
          Row(6, 2, 201, "support"))) // new
    }
  }

  test("merge with NULL clause conditions") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, 101, "support"),
        (3, 301, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED AND NULL THEN
           | UPDATE SET salary = s.salary
           |WHEN NOT MATCHED AND NULL THEN
           | INSERT *
           |WHEN NOT MATCHED BY SOURCE AND NULL THEN
           | DELETE
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 100, "hr"), // unchanged
          Row(2, 200, "software"))) // unchanged
    }
  }

  test("merge with multiple matching clauses") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, 101, "support"),
        (3, 301, "support"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED AND t.pk = 1 THEN
           | UPDATE SET salary = t.salary + 5
           |WHEN MATCHED AND t.salary = 100 THEN
           | UPDATE SET salary = t.salary + 2
           |WHEN NOT MATCHED BY SOURCE AND t.pk = 2 THEN
           | UPDATE SET salary = salary - 1
           |WHEN NOT MATCHED BY SOURCE AND t.salary = 200 THEN
           | DELETE
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 105, "hr"), // updated (matched)
          Row(2, 199, "software"))) // updated (not matched by source)
    }
  }

  test("merge resolves and aligns columns by name") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        ("support", 1, 101),
        ("support", 3, 301))
      sourceRows.toDF("dep", "pk", "salary").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET *
           |WHEN NOT MATCHED THEN
           | INSERT *
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 101, "support"), // update
          Row(2, 200, "software"), // unchanged
          Row(3, 301, "support"))) // insert
    }
  }

  test("merge refreshed relation cache") {
    withTempView("temp", "source") {
      withCache("temp") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 100, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        // define a view on top of the table
        val query = sql(s"SELECT * FROM $tableNameAsString WHERE salary = 100")
        query.createOrReplaceTempView("temp")

        // cache the view
        sql("CACHE TABLE temp")

        // verify the view returns expected results
        checkAnswer(
          sql("SELECT * FROM temp"),
          Row(1, 100, "hr") :: Row(2, 100, "software") :: Nil)

        val sourceRows = Seq(
          ("support", 1, 101),
          ("support", 3, 301))
        sourceRows.toDF("dep", "pk", "salary").createOrReplaceTempView("source")

        // merge changes into the table
        sql(
          s"""MERGE INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | UPDATE SET *
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)

        // verify the merge was successful
        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(
            Row(1, 101, "support"), // update
            Row(2, 100, "software"), // unchanged
            Row(3, 301, "support"))) // insert

        // verify the view reflects the changes in the table
        checkAnswer(sql("SELECT * FROM temp"), Row(2, 100, "software") :: Nil)
      }
    }
  }

  test("merge with updates to nested struct fields in MATCHED clauses") {
    withTempView("source") {
      createAndInitTable(
        s"""pk INT NOT NULL,
           |s STRUCT<c1: INT, c2: STRUCT<a: ARRAY<INT>, m: MAP<STRING, STRING>>>,
           |dep STRING""".stripMargin,
        """{ "pk": 1, "s": { "c1": 2, "c2": { "a": [1,2], "m": { "a": "b" } } }, "dep": "hr" }""")

      Seq(1, 3).toDF("pk").createOrReplaceTempView("source")

      // update primitive, array, map columns inside a struct
      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source src
           |ON t.pk = src.pk
           |WHEN MATCHED THEN
           | UPDATE SET s.c1 = -1, s.c2.m = map('k', 'v'), s.c2.a = array(-1)
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(Row(1, Row(-1, Row(Seq(-1), Map("k" -> "v"))), "hr")))

      // set primitive, array, map columns to NULL (proper casts should be in inserted)
      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source src
           |ON t.pk = src.pk
           |WHEN MATCHED THEN
           | UPDATE SET s.c1 = NULL, s.c2 = NULL
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, Row(null, null), "hr") :: Nil)

      // assign an entire struct
      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source src
           |ON t.pk = src.pk
           |WHEN MATCHED THEN
           | UPDATE SET s = named_struct('c1', 1, 'c2', named_struct('a', array(1), 'm', null))
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, Row(1, Row(Seq(1), null)), "hr") :: Nil)
    }
  }

  test("merge with updates to nested struct fields in NOT MATCHED BY SOURCE clauses") {
    withTempView("source") {
      createAndInitTable(
        s"""pk INT NOT NULL,
           |s STRUCT<c1: INT, c2: STRUCT<a: ARRAY<INT>, m: MAP<STRING, STRING>>>,
           |dep STRING""".stripMargin,
        """{ "pk": 1, "s": { "c1": 2, "c2": { "a": [1,2], "m": { "a": "b" } } }, "dep": "hr" }""")

      Seq(2, 4).toDF("pk").createOrReplaceTempView("source")

      // update primitive, array, map columns inside a struct
      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source src
           |ON t.pk = src.pk
           |WHEN NOT MATCHED BY SOURCE THEN
           | UPDATE SET s.c1 = -1, s.c2.m = map('k', 'v'), s.c2.a = array(-1)
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(Row(1, Row(-1, Row(Seq(-1), Map("k" -> "v"))), "hr")))

      // set primitive, array, map columns to NULL (proper casts should be in inserted)
      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source src
           |ON t.pk = src.pk
           |WHEN NOT MATCHED BY SOURCE THEN
           | UPDATE SET s.c1 = NULL, s.c2 = NULL
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, Row(null, null), "hr") :: Nil)

      // assign an entire struct
      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source src
           |ON t.pk = src.pk
           |WHEN NOT MATCHED BY SOURCE THEN
           | UPDATE SET s = named_struct('c1', 1, 'c2', named_struct('a', array(1), 'm', null))
           |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Row(1, Row(1, Row(Seq(1), null)), "hr") :: Nil)
    }
  }

  test("merge with default values") {
    withTempView("source") {
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

      Seq(1, 2, 4).toDF("pk").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET id = DEFAULT
           |WHEN NOT MATCHED THEN
           | INSERT (pk, id, dep) VALUES (s.pk, DEFAULT, 'new')
           |WHEN NOT MATCHED BY SOURCE THEN
           | UPDATE SET id = DEFAULT
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 42, "hr"), // update (matched)
          Row(2, 42, "software"), // update (matched)
          Row(3, 42, "hr"), // update (not matched by source)
          Row(4, 42, "new"))) // insert
    }
  }

  test("merge with char/varchar columns") {
    withTempView("source") {
      createTable("pk INT NOT NULL, s STRUCT<n_c: CHAR(3), n_vc: VARCHAR(5)>, dep STRING")

      append("pk INT NOT NULL, s STRUCT<n_c: STRING, n_vc: STRING>, dep STRING",
        """{ "pk": 1, "s": { "n_c": "aaa", "n_vc": "aaa" }, "dep": "hr" }
          |{ "pk": 2, "s": { "n_c": "bbb", "n_vc": "bbb" }, "dep": "software" }
          |{ "pk": 3, "s": { "n_c": "ccc", "n_vc": "ccc" }, "dep": "hr" }
          |""".stripMargin)

      Seq(1, 2, 4).toDF("pk").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET s.n_c = 'x1', s.n_vc = 'x2'
           |WHEN NOT MATCHED BY SOURCE THEN
           | UPDATE SET s.n_c = 'y1', s.n_vc = 'y2'
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, Row("x1 ", "x2"), "hr"), // update (matched)
          Row(2, Row("x1 ", "x2"), "software"), // update (matched)
          Row(3, Row("y1 ", "y2"), "hr"))) // update (not matched by source)
    }
  }

  test("merge with NOT NULL checks") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, s STRUCT<n_i: INT NOT NULL, n_l: LONG>, dep STRING",
        """{ "pk": 1, "s": { "n_i": 1, "n_l": 11 }, "dep": "hr" }
          |{ "pk": 2, "s": { "n_i": 2, "n_l": 22 }, "dep": "software" }
          |{ "pk": 3, "s": { "n_i": 3, "n_l": 33 }, "dep": "hr" }
          |""".stripMargin)

      Seq(1, 4).toDF("pk").createOrReplaceTempView("source")

      val e1 = intercept[SparkRuntimeException] {
        sql(
          s"""MERGE INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | UPDATE SET s = named_struct('n_i', null, 'n_l', -1L)
             |""".stripMargin)
      }
      assert(e1.getCondition == "NOT_NULL_ASSERT_VIOLATION")

      val e2 = intercept[SparkRuntimeException] {
        sql(
          s"""MERGE INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED BY SOURCE THEN
             | UPDATE SET s = named_struct('n_i', null, 'n_l', -1L)
             |""".stripMargin)
      }
      assert(e2.getCondition == "NOT_NULL_ASSERT_VIOLATION")

      val e3 = intercept[SparkRuntimeException] {
        sql(
          s"""MERGE INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED THEN
             | INSERT (pk, s, dep) VALUES (s.pk, named_struct('n_i', null, 'n_l', -1L), 'invalid')
             |""".stripMargin)
      }
      assert(e3.getCondition == "NOT_NULL_ASSERT_VIOLATION")
    }
  }

  test("unsupported merge into conditions") {
    withTempView("source") {
      createTable("pk INT NOT NULL, salary INT, dep STRING")

      val sourceRows = Seq(
        (1, 100, "hr"),
        (2, 200, "finance"),
        (3, 300, "hr"))
      sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

      val unsupportedSourceExprs = Map(
        "s.pk < rand()" -> "Non-deterministic expressions are not allowed",
        "max(s.pk) < 10" -> "Aggregates are not allowed",
        s"s.pk IN (SELECT pk FROM $tableNameAsString)" -> "Subqueries are not allowed")

      unsupportedSourceExprs.map { case (expr, errMsg) =>
        val e1 = intercept[AnalysisException] {
          sql(
            s"""MERGE INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk AND $expr
               |WHEN MATCHED THEN
               | UPDATE SET *
               |""".stripMargin)
        }
        assert(e1.message.contains("unsupported SEARCH condition") && e1.message.contains(errMsg))

        val e2 = intercept[AnalysisException] {
          sql(
            s"""MERGE INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN MATCHED AND $expr THEN
               | UPDATE SET *
               |""".stripMargin)
        }
        assert(e2.message.contains("unsupported UPDATE condition") && e2.message.contains(errMsg))

        val e3 = intercept[AnalysisException] {
          sql(
            s"""MERGE INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN MATCHED AND $expr THEN
               | DELETE
               |""".stripMargin)
        }
        assert(e3.message.contains("unsupported DELETE condition") && e3.message.contains(errMsg))

        val e4 = intercept[AnalysisException] {
          sql(
            s"""MERGE INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN NOT MATCHED AND $expr THEN
               | INSERT *
               |""".stripMargin)
        }
        assert(e4.message.contains("unsupported INSERT condition") && e4.message.contains(errMsg))
      }

      val unsupportedTargetExprs = Map(
        "t.pk < rand()" -> "Non-deterministic expressions are not allowed",
        "max(t.pk) < 10" -> "Aggregates are not allowed",
        s"t.pk IN (SELECT pk FROM $tableNameAsString)" -> "Subqueries are not allowed")

      unsupportedTargetExprs.map { case (expr, errMsg) =>
        val e1 = intercept[AnalysisException] {
          sql(
            s"""MERGE INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk AND $expr
               |WHEN MATCHED THEN
               | UPDATE SET *
               |""".stripMargin)
        }
        assert(e1.message.contains("unsupported SEARCH condition") && e1.message.contains(errMsg))

        val e2 = intercept[AnalysisException] {
          sql(
            s"""MERGE INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN NOT MATCHED BY SOURCE AND $expr THEN
               | UPDATE SET t.pk = -1
               |""".stripMargin)
        }
        assert(e2.message.contains("unsupported UPDATE condition") && e2.message.contains(errMsg))

        val e3 = intercept[AnalysisException] {
          sql(
            s"""MERGE INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN NOT MATCHED BY SOURCE AND $expr THEN
               | DELETE
               |""".stripMargin)
        }
        assert(e3.message.contains("unsupported DELETE condition") && e3.message.contains(errMsg))
      }
    }
  }

  test("all target filters are evaluated on data source side") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "hr" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |{ "pk": 4, "salary": 400, "dep": "software" }
          |{ "pk": 5, "salary": 500, "dep": "software" }
          |""".stripMargin)

      val sourceDF = Seq(1, 2, 3, 6).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      val executedPlan = executeAndKeepPlan {
        sql(
          s"""MERGE INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk AND t.DeP IN ('hr', 'software')
             |WHEN MATCHED THEN
             | UPDATE SET t.salary = t.salary + 1
             |WHEN NOT MATCHED THEN
             | INSERT (pk, salary, dep) VALUES (s.pk, 0, 'hr')
             |""".stripMargin)
      }

      val expressions = flatMap(executedPlan)(_.expressions.flatMap(splitConjunctivePredicates))
      val inFilterPushed = expressions.forall {
        case In(attr: AttributeReference, _) if attr.name == "DeP" => false
        case _ => true
      }
      assert(inFilterPushed, "IN filter must be evaluated on data source side")

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 101, "hr"), // update
          Row(2, 201, "hr"), // update
          Row(3, 301, "hr"), // update
          Row(4, 400, "software"), // unchanged
          Row(5, 500, "software"), // unchanged
          Row(6, 0, "hr"))) // insert
    }
  }

  test("some target filters are evaluated on data source side") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "hr" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |{ "pk": 4, "salary": 400, "dep": "software" }
          |{ "pk": 5, "salary": 500, "dep": "software" }
          |""".stripMargin)

      val sourceDF = Seq(1, 2, 3, 6).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      val executedPlan = executeAndKeepPlan {
        sql(
          s"""MERGE INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk AND t.dep IN ('hr', 'software') AND t.salary != -1
             |WHEN MATCHED THEN
             | UPDATE SET t.salary = t.salary + 1
             |WHEN NOT MATCHED THEN
             | INSERT (pk, salary, dep) VALUES (s.pk, 0, 'hr')
             |""".stripMargin)
      }

      val expressions = flatMap(executedPlan)(_.expressions.flatMap(splitConjunctivePredicates))

      val inFilterPushed = expressions.forall {
        case In(attr: AttributeReference, _) if attr.name == "dep" => false
        case _ => true
      }
      assert(inFilterPushed, "IN filter must be evaluated on data source side")

      val notEqualFilterPreserved = expressions.exists {
        case Not(EqualTo(attr: AttributeReference, _)) if attr.name == "salary" => true
        case _ => false
      }
      assert(notEqualFilterPreserved, "NOT filter must be evaluated on Spark side")

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 101, "hr"), // update
          Row(2, 201, "hr"), // update
          Row(3, 301, "hr"), // update
          Row(4, 400, "software"), // unchanged
          Row(5, 500, "software"), // unchanged
          Row(6, 0, "hr"))) // insert
    }
  }

  test("pushable target filters are preserved with NOT MATCHED BY SOURCE clause") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "hr" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |{ "pk": 4, "salary": 400, "dep": "software" }
          |{ "pk": 5, "salary": 500, "dep": "software" }
          |""".stripMargin)

      val sourceDF = Seq(1, 2, 3, 6).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      val executedPlan = executeAndKeepPlan {
        sql(
          s"""MERGE INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk AND DeP IN ('hr', 'software')
             |WHEN MATCHED THEN
             | UPDATE SET t.salary = t.salary + 1
             |WHEN NOT MATCHED THEN
             | INSERT (pk, salary, dep) VALUES (s.pk, 0, 'hr')
             |WHEN NOT MATCHED BY SOURCE THEN
             | DELETE
             |""".stripMargin)
      }

      val expressions = flatMap(executedPlan)(_.expressions.flatMap(splitConjunctivePredicates))
      val inFilterPreserved = expressions.exists {
        case In(attr: AttributeReference, _) if attr.name == "DeP" => true
        case _ => false
      }
      assert(inFilterPreserved, "IN filter must be preserved")

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 101, "hr"), // update
          Row(2, 201, "hr"), // update
          Row(3, 301, "hr"), // update
          Row(6, 0, "hr"))) // insert
    }
  }

  test("merge into table with recursive CTE") {
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
           |) MERGE INTO $tableNameAsString t
           |USING s
           |ON t.val = s.val
           |WHEN MATCHED THEN
           | UPDATE SET t.val = t.val - 1
           |WHEN NOT MATCHED THEN
           | INSERT (val) VALUES (-s.val)
           |WHEN NOT MATCHED BY SOURCE THEN
           | UPDATE SET t.val = t.val + 1
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(0),
          Row(10),
          Row(9),
          Row(3),
          Row(-2),
          Row(-3),
          Row(-5)))
    }
  }

  test("Merge metrics with matched clause") {
    Seq("true", "false").foreach { aqeEnabled: String =>
      withTempView("source") {
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled) {
          createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
            """{ "pk": 1, "salary": 100, "dep": "hr" }
              |{ "pk": 2, "salary": 200, "dep": "software" }
              |{ "pk": 3, "salary": 300, "dep": "hr" }
              |""".stripMargin)

          val sourceDF = Seq(1, 2, 10).toDF("pk")
          sourceDF.createOrReplaceTempView("source")

          val mergeExec = findMergeExec {
            s"""MERGE INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN MATCHED AND salary < 200 THEN
               | UPDATE SET salary = 1000
               |""".stripMargin
          }

          assertMetric(mergeExec, "numTargetRowsCopied", if (deltaMerge) 0 else 2)
          assertMetric(mergeExec, "numTargetRowsInserted", 0)
          assertMetric(mergeExec, "numTargetRowsUpdated", 1)
          assertMetric(mergeExec, "numTargetRowsDeleted", 0)
          assertMetric(mergeExec, "numTargetRowsMatchedUpdated", 1)
          assertMetric(mergeExec, "numTargetRowsMatchedDeleted", 0)
          assertMetric(mergeExec, "numTargetRowsNotMatchedBySourceUpdated", 0)
          assertMetric(mergeExec, "numTargetRowsNotMatchedBySourceDeleted", 0)

          checkAnswer(
            sql(s"SELECT * FROM $tableNameAsString"),
            Seq(
              Row(1, 1000, "hr"), // updated
              Row(2, 200, "software"),
              Row(3, 300, "hr")))

      val mergeSummary = getMergeSummary()
      assert(mergeSummary.numSourceRows === 3L)
      assert(mergeSummary.numTargetRowsCopied === (if (deltaMerge) 0L else 2L))
      assert(mergeSummary.numTargetRowsInserted === 0L)
      assert(mergeSummary.numTargetRowsUpdated === 1L)
      assert(mergeSummary.numTargetRowsDeleted === 0L)
      assert(mergeSummary.numTargetRowsMatchedUpdated === 1L)
      assert(mergeSummary.numTargetRowsMatchedDeleted === 0L)
      assert(mergeSummary.numTargetRowsNotMatchedBySourceUpdated === 0L)
      assert(mergeSummary.numTargetRowsNotMatchedBySourceDeleted === 0L)

      sql(s"DROP TABLE $tableNameAsString")
    }
  }

  test("Merge metrics with matched and not matched clause") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |""".stripMargin)

      val sourceDF = Seq(
        (4, 100, "marketing"),
        (5, 400, "executive"),
        (6, 100, "hr")
      ).toDF("pk", "salary", "dep")
      sourceDF.createOrReplaceTempView("source")

      val mergeExec = findMergeExec {
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET salary = 9999
           |WHEN NOT MATCHED AND salary > 200 THEN
           | INSERT *
           |""".stripMargin
      }

      assertMetric(mergeExec, "numTargetRowsCopied", 0)
      assertMetric(mergeExec, "numTargetRowsInserted", 1)
      assertMetric(mergeExec, "numTargetRowsUpdated", 0)
      assertMetric(mergeExec, "numTargetRowsDeleted", 0)
      assertMetric(mergeExec, "numTargetRowsMatchedUpdated", 0)
      assertMetric(mergeExec, "numTargetRowsMatchedDeleted", 0)
      assertMetric(mergeExec, "numTargetRowsNotMatchedBySourceUpdated", 0)
      assertMetric(mergeExec, "numTargetRowsNotMatchedBySourceDeleted", 0)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 100, "hr"),
          Row(2, 200, "software"),
          Row(3, 300, "hr"),
          Row(5, 400, "executive"))) // inserted

      val mergeSummary = getMergeSummary()
      // TODO SPARK-52578: Handle this case when optimizer removes Join due to no matching pks
      assert(commitProps("merge.numSourceRows") === (if (deltaMerge) 3L else -1L))
      assert(mergeSummary.numTargetRowsCopied === 0L)
      assert(mergeSummary.numTargetRowsInserted === 1L)
      assert(mergeSummary.numTargetRowsUpdated === 0L)
      assert(mergeSummary.numTargetRowsDeleted === 0L)
      assert(mergeSummary.numTargetRowsMatchedUpdated === 0L)
      assert(mergeSummary.numTargetRowsMatchedDeleted === 0L)
      assert(mergeSummary.numTargetRowsNotMatchedBySourceUpdated === 0L)
      assert(mergeSummary.numTargetRowsNotMatchedBySourceDeleted === 0L)
    }
  }

  test("Merge metrics with matched and not matched by source clauses: update") {
    Seq("true", "false").foreach { aqeEnabled: String =>
      withTempView("source") {
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled) {
          createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
            """{ "pk": 1, "salary": 100, "dep": "hr" }
              |{ "pk": 2, "salary": 200, "dep": "software" }
              |{ "pk": 3, "salary": 300, "dep": "hr" }
              |{ "pk": 4, "salary": 400, "dep": "marketing" }
              |{ "pk": 5, "salary": 500, "dep": "executive" }
              |""".stripMargin)

          val sourceDF = Seq(1, 2, 10).toDF("pk")
          sourceDF.createOrReplaceTempView("source")

          val mergeExec = findMergeExec {
            s"""MERGE INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN MATCHED AND salary < 200 THEN
               | UPDATE SET salary = 1000
               |WHEN NOT MATCHED BY SOURCE AND salary > 400 THEN
               | UPDATE SET salary = -1
               |""".stripMargin
          }

          assertMetric(mergeExec, "numTargetRowsCopied", if (deltaMerge) 0 else 3)
          assertMetric(mergeExec, "numTargetRowsInserted", 0)
          assertMetric(mergeExec, "numTargetRowsUpdated", 2)
          assertMetric(mergeExec, "numTargetRowsDeleted", 0)
          assertMetric(mergeExec, "numTargetRowsMatchedUpdated", 1)
          assertMetric(mergeExec, "numTargetRowsMatchedDeleted", 0)
          assertMetric(mergeExec, "numTargetRowsNotMatchedBySourceUpdated", 1)
          assertMetric(mergeExec, "numTargetRowsNotMatchedBySourceDeleted", 0)

          checkAnswer(
            sql(s"SELECT * FROM $tableNameAsString"),
            Seq(
              Row(1, 1000, "hr"), // updated
              Row(2, 200, "software"),
              Row(3, 300, "hr"),
              Row(4, 400, "marketing"),
              Row(5, -1, "executive"))) // updated

      val mergeSummary = getMergeSummary()
      assert(mergeSummary.numSourceRows === 3L)
      assert(mergeSummary.numTargetRowsCopied === (if (deltaMerge) 0L else 3L))
      assert(mergeSummary.numTargetRowsInserted === 0L)
      assert(mergeSummary.numTargetRowsUpdated === 2L)
      assert(mergeSummary.numTargetRowsDeleted === 0L)
      assert(mergeSummary.numTargetRowsMatchedUpdated === 1L)
      assert(mergeSummary.numTargetRowsMatchedDeleted === 0L)
      assert(mergeSummary.numTargetRowsNotMatchedBySourceUpdated === 1L)
      assert(mergeSummary.numTargetRowsNotMatchedBySourceDeleted === 0L)

      sql(s"DROP TABLE $tableNameAsString")
    }
  }

  test("Merge metrics with matched and not matched by source clauses: delete") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |{ "pk": 4, "salary": 400, "dep": "marketing" }
          |{ "pk": 5, "salary": 500, "dep": "executive" }
          |""".stripMargin)

      val sourceDF = Seq(1, 2, 10).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      val mergeExec = findMergeExec {
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED AND salary < 200 THEN
           | DELETE
           |WHEN NOT MATCHED BY SOURCE AND salary > 400 THEN
           | DELETE
           |""".stripMargin
      }


      assertMetric(mergeExec, "numTargetRowsCopied", if (deltaMerge) 0 else 3)
      assertMetric(mergeExec, "numTargetRowsInserted", 0)
      assertMetric(mergeExec, "numTargetRowsUpdated", 0)
      assertMetric(mergeExec, "numTargetRowsDeleted", 2)
      assertMetric(mergeExec, "numTargetRowsMatchedUpdated", 0)
      assertMetric(mergeExec, "numTargetRowsMatchedDeleted", 1)
      assertMetric(mergeExec, "numTargetRowsNotMatchedBySourceUpdated", 0)
      assertMetric(mergeExec, "numTargetRowsNotMatchedBySourceDeleted", 1)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          // Row(1, 100, "hr") deleted
          Row(2, 200, "software"),
          Row(3, 300, "hr"),
          Row(4, 400, "marketing"))
          // Row(5, 500, "executive") deleted
      )

      val mergeSummary = getMergeSummary()
      assert(mergeSummary.numSourceRows === 3L)
      assert(mergeSummary.numTargetRowsCopied === (if (deltaMerge) 0L else 3L))
      assert(mergeSummary.numTargetRowsInserted === 0L)
      assert(mergeSummary.numTargetRowsUpdated === 0L)
      assert(mergeSummary.numTargetRowsDeleted === 2L)
      assert(mergeSummary.numTargetRowsMatchedUpdated === 0L)
      assert(mergeSummary.numTargetRowsMatchedDeleted === 1L)
      assert(mergeSummary.numTargetRowsNotMatchedBySourceUpdated === 0L)
      assert(mergeSummary.numTargetRowsNotMatchedBySourceDeleted === 1L)
    }
  }

  test("Merge metrics with matched, not matched, and not matched by source clauses: update") {
    Seq("true", "false").foreach { aqeEnabled: String =>
      withTempView("source") {
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled) {
          createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
            """{ "pk": 1, "salary": 100, "dep": "hr" }
              |{ "pk": 2, "salary": 200, "dep": "software" }
              |{ "pk": 3, "salary": 300, "dep": "hr" }
              |{ "pk": 4, "salary": 400, "dep": "marketing" }
              |{ "pk": 5, "salary": 500, "dep": "executive" }
              |""".stripMargin)

          val sourceDF = Seq(1, 2, 6, 10).toDF("pk")
          sourceDF.createOrReplaceTempView("source")

          val mergeExec = findMergeExec {
            s"""MERGE INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN MATCHED AND salary < 200 THEN
               | UPDATE SET salary = 1000
               |WHEN NOT MATCHED AND s.pk < 10 THEN
               | INSERT (pk, salary, dep) VALUES (s.pk, -1, "dummy")
               |WHEN NOT MATCHED BY SOURCE AND salary > 400 THEN
               | UPDATE SET salary = -1
               |""".stripMargin
          }

          assertMetric(mergeExec, "numTargetRowsCopied", if (deltaMerge) 0 else 3)
          assertMetric(mergeExec, "numTargetRowsInserted", 1)
          assertMetric(mergeExec, "numTargetRowsUpdated", 2)
          assertMetric(mergeExec, "numTargetRowsDeleted", 0)
          assertMetric(mergeExec, "numTargetRowsMatchedUpdated", 1)
          assertMetric(mergeExec, "numTargetRowsMatchedDeleted", 0)
          assertMetric(mergeExec, "numTargetRowsNotMatchedBySourceUpdated", 1)
          assertMetric(mergeExec, "numTargetRowsNotMatchedBySourceDeleted", 0)

          checkAnswer(
            sql(s"SELECT * FROM $tableNameAsString"),
            Seq(
              Row(1, 1000, "hr"), // updated
              Row(2, 200, "software"),
              Row(3, 300, "hr"),
              Row(4, 400, "marketing"),
              Row(5, -1, "executive"), // updated
              Row(6, -1, "dummy"))) // inserted

      val mergeSummary = getMergeSummary()
      assert(mergeSummary.numSourceRows === 4L)
      assert(mergeSummary.numTargetRowsCopied === (if (deltaMerge) 0L else 3L))
      assert(mergeSummary.numTargetRowsInserted === 1L)
      assert(mergeSummary.numTargetRowsUpdated === 2L)
      assert(mergeSummary.numTargetRowsDeleted === 0L)
      assert(mergeSummary.numTargetRowsMatchedUpdated === 1L)
      assert(mergeSummary.numTargetRowsMatchedDeleted === 0L)
      assert(mergeSummary.numTargetRowsNotMatchedBySourceUpdated === 1L)
      assert(mergeSummary.numTargetRowsNotMatchedBySourceDeleted === 0L)

      sql(s"DROP TABLE $tableNameAsString")
    }
  }

  test("Merge metrics with matched, not matched, and not matched by source clauses: delete") {
    Seq("true", "false").foreach { aqeEnabled: String =>
      withTempView("source") {
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled) {
          createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
            """{ "pk": 1, "salary": 100, "dep": "hr" }
              |{ "pk": 2, "salary": 200, "dep": "software" }
              |{ "pk": 3, "salary": 300, "dep": "hr" }
              |{ "pk": 4, "salary": 400, "dep": "marketing" }
              |{ "pk": 5, "salary": 500, "dep": "executive" }
              |""".stripMargin)

          val sourceDF = Seq(1, 2, 6, 10).toDF("pk")
          sourceDF.createOrReplaceTempView("source")

          val mergeExec = findMergeExec {
            s"""MERGE INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN MATCHED AND salary < 200 THEN
               | DELETE
               |WHEN NOT MATCHED AND s.pk < 10 THEN
               | INSERT (pk, salary, dep) VALUES (s.pk, -1, "dummy")
               |WHEN NOT MATCHED BY SOURCE AND salary > 400 THEN
               | DELETE
               |""".stripMargin
          }

          assertMetric(mergeExec, "numTargetRowsCopied", if (deltaMerge) 0 else 3)
          assertMetric(mergeExec, "numTargetRowsInserted", 1)
          assertMetric(mergeExec, "numTargetRowsUpdated", 0)
          assertMetric(mergeExec, "numTargetRowsDeleted", 2)
          assertMetric(mergeExec, "numTargetRowsMatchedUpdated", 0)
          assertMetric(mergeExec, "numTargetRowsMatchedDeleted", 1)
          assertMetric(mergeExec, "numTargetRowsNotMatchedBySourceUpdated", 0)
          assertMetric(mergeExec, "numTargetRowsNotMatchedBySourceDeleted", 1)

          checkAnswer(
            sql(s"SELECT * FROM $tableNameAsString"),
            Seq(
              // Row(1, 100, "hr") deleted
              Row(2, 200, "software"),
              Row(3, 300, "hr"),
              Row(4, 400, "marketing"),
              // Row(5, 500, "executive") deleted
              Row(6, -1, "dummy"))) // inserted

      val mergeSummary = getMergeSummary()
      assert(mergeSummary.numSourceRows === 4L)
      assert(mergeSummary.numTargetRowsCopied === (if (deltaMerge) 0L else 3L))
      assert(mergeSummary.numTargetRowsInserted === 1L)
      assert(mergeSummary.numTargetRowsUpdated === 0L)
      assert(mergeSummary.numTargetRowsDeleted === 2L)
      assert(mergeSummary.numTargetRowsMatchedUpdated === 0L)
      assert(mergeSummary.numTargetRowsMatchedDeleted === 1L)
      assert(mergeSummary.numTargetRowsNotMatchedBySourceUpdated === 0L)
      assert(mergeSummary.numTargetRowsNotMatchedBySourceDeleted === 1L)

      sql(s"DROP TABLE $tableNameAsString")
    }
  }

  test("SPARK-52689: V2 write metrics for merge") {
    Seq("true", "false").foreach { aqeEnabled: String =>
      withTempView("source") {
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled) {
          createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
            """{ "pk": 1, "salary": 100, "dep": "hr" }
              |{ "pk": 2, "salary": 200, "dep": "software" }
              |{ "pk": 3, "salary": 300, "dep": "hr" }
              |{ "pk": 4, "salary": 400, "dep": "marketing" }
              |{ "pk": 5, "salary": 500, "dep": "executive" }
              |""".stripMargin)

          val sourceDF = Seq(1, 2, 6, 10).toDF("pk")
          sourceDF.createOrReplaceTempView("source")

          sql(
            s"""MERGE INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN MATCHED AND salary < 200 THEN
               | DELETE
               |WHEN NOT MATCHED AND s.pk < 10 THEN
               | INSERT (pk, salary, dep) VALUES (s.pk, -1, "dummy")
               |WHEN NOT MATCHED BY SOURCE AND salary > 400 THEN
               | DELETE
               |""".stripMargin
          )

          val mergeMetrics = getMergeSummary()
          assert(mergeMetrics.numSourceRows === 4L)
          assert(mergeMetrics.numTargetRowsCopied === (if (deltaMerge) 0L else 3L))
          assert(mergeMetrics.numTargetRowsInserted === 1L)
          assert(mergeMetrics.numTargetRowsUpdated === 0L)
          assert(mergeMetrics.numTargetRowsDeleted === 2L)
          assert(mergeMetrics.numTargetRowsMatchedUpdated === 0L)
          assert(mergeMetrics.numTargetRowsMatchedDeleted === 1L)
          assert(mergeMetrics.numTargetRowsNotMatchedBySourceUpdated === 0L)
          assert(mergeMetrics.numTargetRowsNotMatchedBySourceDeleted === 1L)

          sql(s"DROP TABLE $tableNameAsString")
        }
      }
    }
  }

  test("Merge metrics with numSourceRows for empty source") {
    Seq("true", "false").foreach { aqeEnabled: String =>
      withTempView("source") {
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled) {
          createAndInitTable(
            "pk INT NOT NULL, salary INT, dep STRING",
            """{ "pk": 1, "salary": 100, "dep": "hr" }
              |{ "pk": 2, "salary": 200, "dep": "software" }
              |{ "pk": 3, "salary": 300, "dep": "hr" }
              |""".stripMargin)

          // source is empty
          Seq.empty[Int].toDF("pk").createOrReplaceTempView("source")

          sql(s"""MERGE INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN MATCHED THEN
               | UPDATE SET salary = 1000
               |WHEN NOT MATCHED BY SOURCE THEN
               | DELETE
               |""".stripMargin)

          val table = catalog.loadTable(ident)
          val commitProps = table.asInstanceOf[InMemoryTable].commits.last.properties
          assert(commitProps("merge.numSourceRows") === "-1") // if no numOutputRows, should be -1
          assert(commitProps("merge.numTargetRowsCopied") === (if (deltaMerge) "0" else "0"))
          assert(commitProps("merge.numTargetRowsInserted") === "0")
          assert(commitProps("merge.numTargetRowsUpdated") === "0")
          assert(commitProps("merge.numTargetRowsDeleted") === "3")
          assert(commitProps("merge.numTargetRowsMatchedUpdated") === "0")
          assert(commitProps("merge.numTargetRowsMatchedDeleted") === "0")
          assert(commitProps("merge.numTargetRowsNotMatchedBySourceUpdated") === "0")
          assert(commitProps("merge.numTargetRowsNotMatchedBySourceDeleted") === "3")

          sql(s"DROP TABLE $tableNameAsString")
        }
      }
    }
  }

  test("Merge schema evolution new column with set explicit column") {
    Seq((true, true), (false, true), (true, false)).foreach {
      case (withSchemaEvolution, schemaEvolutionEnabled) =>
        withTempView("source") {
          createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
            """{ "pk": 1, "salary": 100, "dep": "hr" }
              |{ "pk": 2, "salary": 200, "dep": "software" }
              |{ "pk": 3, "salary": 300, "dep": "hr" }
              |{ "pk": 4, "salary": 400, "dep": "marketing" }
              |{ "pk": 5, "salary": 500, "dep": "executive" }
              |""".stripMargin)

          if (!schemaEvolutionEnabled) {
            sql(s"""ALTER TABLE $tableNameAsString SET TBLPROPERTIES
                   | ('auto-schema-evolution' = 'false')""".stripMargin)
          }

          val sourceDF = Seq((4, 150, "dummy", true),
            (5, 250, "dummy", true),
            (6, 350, "dummy", false)).toDF("pk", "salary", "dep", "active")
          sourceDF.createOrReplaceTempView("source")

          val schemaEvolutionClause = if (withSchemaEvolution) "WITH SCHEMA EVOLUTION" else ""
          val mergeStmt = s"""MERGE $schemaEvolutionClause
                             |INTO $tableNameAsString t
                             |USING source s
                             |ON t.pk = s.pk
                             |WHEN MATCHED THEN
                             | UPDATE SET dep='software', active=s.active
                             |WHEN NOT MATCHED THEN
                             | INSERT (pk, salary, dep, active) VALUES (s.pk, 0, s.dep, s.active)
                             |""".stripMargin

          if (withSchemaEvolution && schemaEvolutionEnabled) {
            sql(mergeStmt)
            checkAnswer(
              sql(s"SELECT * FROM $tableNameAsString"),
              Seq(
                Row(1, 100, "hr", null),
                Row(2, 200, "software", null),
                Row(3, 300, "hr", null),
                Row(4, 400, "software", true),
                Row(5, 500, "software", true),
                Row(6, 0, "dummy", false)))
          } else {
            val e = intercept[org.apache.spark.sql.AnalysisException] {
              sql(mergeStmt)
            }
            assert(e.errorClass.get == "UNRESOLVED_COLUMN.WITH_SUGGESTION")
            assert(e.getMessage.contains("A column, variable, or function parameter with name " +
              "`active` cannot be resolved"))
          }

          sql(s"DROP TABLE $tableNameAsString")
        }
    }
  }

  test("Merge schema evolution new column with set all columns") {
    Seq((true, true), (false, true), (true, false)).foreach {
      case (withSchemaEvolution, schemaEvolutionEnabled) =>
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |{ "pk": 4, "salary": 400, "dep": "marketing" }
            |{ "pk": 5, "salary": 500, "dep": "executive" }
            |""".stripMargin)


        if (!schemaEvolutionEnabled) {
          sql(s"""ALTER TABLE $tableNameAsString SET TBLPROPERTIES
                 | ('auto-schema-evolution' = 'false')""".stripMargin)
        }

        val sourceDF = Seq((4, 150, "finance", true),
          (5, 250, "finance", false),
          (6, 350, "finance", true)).toDF("pk", "salary", "dep", "active")
        sourceDF.createOrReplaceTempView("source")

        val schemaEvolutionClause = if (withSchemaEvolution) "WITH SCHEMA EVOLUTION" else ""
        sql(
          s"""MERGE $schemaEvolutionClause
             |INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN
             | UPDATE SET *
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)

        if (withSchemaEvolution && schemaEvolutionEnabled) {
          checkAnswer(
            sql(s"SELECT * FROM $tableNameAsString"),
            Seq(
              Row(1, 100, "hr", null),
              Row(2, 200, "software", null),
              Row(3, 300, "hr", null),
              Row(4, 150, "finance", true),
              Row(5, 250, "finance", false),
              Row(6, 350, "finance", true)))
        } else {
          // Without schema evolution, the new columns are not added
          checkAnswer(
            sql(s"SELECT * FROM $tableNameAsString"),
            Seq(
              Row(1, 100, "hr"),
              Row(2, 200, "software"),
              Row(3, 300, "hr"),
              Row(4, 150, "finance"),
              Row(5, 250, "finance"),
              Row(6, 350, "finance")))
        }
      }
      sql(s"DROP TABLE $tableNameAsString")
    }
  }

  test("Merge schema evolution replacing column with set all column") {
    Seq((true, true), (false, true), (true, false)).foreach {
      case (withSchemaEvolution, schemaEvolutionEnabled) =>
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |{ "pk": 4, "salary": 400, "dep": "marketing" }
            |{ "pk": 5, "salary": 500, "dep": "executive" }
            |""".stripMargin)

        if (!schemaEvolutionEnabled) {
          sql(s"""ALTER TABLE $tableNameAsString SET TBLPROPERTIES
                 | ('auto-schema-evolution' = 'false')""".stripMargin)
        }

        val sourceDF = Seq((4, 150, true),
          (5, 250, true),
          (6, 350, false)).toDF("pk", "salary", "active")
        sourceDF.createOrReplaceTempView("source")

        val schemaEvolutionClause = if (withSchemaEvolution) "WITH SCHEMA EVOLUTION" else ""
        sql(s"""MERGE $schemaEvolutionClause
                           |INTO $tableNameAsString t
                           |USING source s
                           |ON t.pk = s.pk
                           |WHEN MATCHED THEN
                           | UPDATE SET *
                           |WHEN NOT MATCHED THEN
                           | INSERT *
                           |""".stripMargin)
        if (withSchemaEvolution && schemaEvolutionEnabled) {
          checkAnswer(
            sql(s"SELECT * FROM $tableNameAsString"),
            Seq(
              Row(1, 100, "hr", null),
              Row(2, 200, "software", null),
              Row(3, 300, "hr", null),
              Row(4, 150, "marketing", true),
              Row(5, 250, "executive", true),
              Row(6, 350, null, false)))
        } else {
          checkAnswer(
            sql(s"SELECT * FROM $tableNameAsString"),
            Seq(
              Row(1, 100, "hr"),
              Row(2, 200, "software"),
              Row(3, 300, "hr"),
              Row(4, 150, "marketing"),
              Row(5, 250, "executive"),
              Row(6, 350, null)))
        }
        sql(s"DROP TABLE $tableNameAsString")
      }
    }
  }

  test("Merge schema evolution replacing column with default value and set all column") {
    Seq((true, true), (false, true), (true, false)).foreach {
      case (withSchemaEvolution, schemaEvolutionEnabled) =>
        withTempView("source") {
          createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
            """{ "pk": 1, "salary": 100, "dep": "hr" }
              |{ "pk": 2, "salary": 200, "dep": "software" }
              |{ "pk": 3, "salary": 300, "dep": "hr" }
              |{ "pk": 4, "salary": 400, "dep": "marketing" }
              |{ "pk": 5, "salary": 500, "dep": "executive" }
              |""".stripMargin)

          if (!schemaEvolutionEnabled) {
            sql(s"""ALTER TABLE $tableNameAsString SET TBLPROPERTIES
                   | ('auto-schema-evolution' = 'false')""".stripMargin)
          }
          sql(s"""ALTER TABLE $tableNameAsString ALTER COLUMN dep SET DEFAULT 'unknown'""")

          val sourceDF = Seq((4, 150, true),
            (5, 250, true),
            (6, 350, false)).toDF("pk", "salary", "active")
          sourceDF.createOrReplaceTempView("source")

          val schemaEvolutionClause = if (withSchemaEvolution) "WITH SCHEMA EVOLUTION" else ""
          sql(s"""MERGE $schemaEvolutionClause
                 |INTO $tableNameAsString t
                 |USING source s
                 |ON t.pk = s.pk
                 |WHEN MATCHED THEN
                 | UPDATE SET *
                 |WHEN NOT MATCHED THEN
                 | INSERT *
                 |""".stripMargin)
          if (withSchemaEvolution && schemaEvolutionEnabled) {
            checkAnswer(
              sql(s"SELECT * FROM $tableNameAsString"),
              Seq(
                Row(1, 100, "hr", null),
                Row(2, 200, "software", null),
                Row(3, 300, "hr", null),
                Row(4, 150, "marketing", true),
                Row(5, 250, "executive", true),
                Row(6, 350, "unknown", false)))
          } else {
            checkAnswer(
              sql(s"SELECT * FROM $tableNameAsString"),
              Seq(
                Row(1, 100, "hr"),
                Row(2, 200, "software"),
                Row(3, 300, "hr"),
                Row(4, 150, "marketing"),
                Row(5, 250, "executive"),
                Row(6, 350, "unknown")))
          }
          sql(s"DROP TABLE $tableNameAsString")
        }
    }
  }

  test("Merge schema evolution replacing column with set explicit column") {
    Seq((true, true), (false, true), (true, false)).foreach {
      case (withSchemaEvolution, schemaEvolutionEnabled) =>
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |{ "pk": 4, "salary": 400, "dep": "marketing" }
            |{ "pk": 5, "salary": 500, "dep": "executive" }
            |""".stripMargin)

        if (!schemaEvolutionEnabled) {
          sql(s"""ALTER TABLE $tableNameAsString SET TBLPROPERTIES
                 | ('auto-schema-evolution' = 'false')""".stripMargin)
        }

        val sourceDF = Seq((4, 150, true),
          (5, 250, true),
          (6, 350, false)).toDF("pk", "salary", "active")
        sourceDF.createOrReplaceTempView("source")

        val schemaEvolutionClause = if (withSchemaEvolution) "WITH SCHEMA EVOLUTION" else ""
        val mergeStmt = s"""MERGE $schemaEvolutionClause
                           |INTO $tableNameAsString t
                           |USING source s
                           |ON t.pk = s.pk
                           |WHEN MATCHED THEN
                           | UPDATE SET dep = 'finance', active = s.active
                           |WHEN NOT MATCHED THEN
                           | INSERT (pk, salary, dep, active) VALUES
                           | (s.pk, s.salary, 'finance', s.active)
                           |""".stripMargin

        if (withSchemaEvolution && schemaEvolutionEnabled) {
          sql(mergeStmt)
          checkAnswer(
            sql(s"SELECT * FROM $tableNameAsString"),
            Seq(
              Row(1, 100, "hr", null),
              Row(2, 200, "software", null),
              Row(3, 300, "hr", null),
              Row(4, 400, "finance", true),
              Row(5, 500, "finance", true),
              Row(6, 350, "finance", false)))
        } else {
          val e = intercept[org.apache.spark.sql.AnalysisException] {
            sql(mergeStmt)
          }
          assert(e.errorClass.get == "UNRESOLVED_COLUMN.WITH_SUGGESTION")
          assert(e.getMessage.contains("A column, variable, or function parameter with name " +
            "`active` cannot be resolved"))
        }

        sql(s"DROP TABLE $tableNameAsString")
      }
    }
  }

  test("merge into schema evolution type widening from short to int") {
    Seq((true, true), (false, true), (true, false)).foreach {
      case (withSchemaEvolution, schemaEvolutionEnabled) =>
        withTable(tableNameAsString) {
          withTempView("source") {
            createAndInitTable("pk INT NOT NULL, salary SMALLINT, dep STRING",
              """{ "pk": 1, "salary": 100, "dep": "hr" }
                |{ "pk": 2, "salary": 200, "dep": "finance" }
                |{ "pk": 3, "salary": 300, "dep": "engineering" }
                |""".stripMargin)

            if (!schemaEvolutionEnabled) {
              sql(
                s"""ALTER TABLE $tableNameAsString SET TBLPROPERTIES
                   | ('auto-schema-evolution' = 'false')""".stripMargin)
            }

            // Source data with int salary values that would exceed short range
            val sourceRows = Seq(
              (1, 50000, "hr"),
              (4, 40000, "sales"),
              (5, 500, "marketing"))
            sourceRows.toDF("pk", "salary", "dep").createOrReplaceTempView("source")

            val schemaEvolutionClause = if (withSchemaEvolution) "WITH SCHEMA EVOLUTION" else ""
            val mergeStmt =
              s"""MERGE $schemaEvolutionClause
                 |INTO $tableNameAsString t
                 |USING source s
                 |ON t.pk = s.pk
                 |WHEN MATCHED THEN
                 | UPDATE SET salary = s.salary
                 |WHEN NOT MATCHED THEN
                 | INSERT (pk, salary, dep) VALUES (s.pk, s.salary, s.dep)
                 |""".stripMargin

            if (withSchemaEvolution && schemaEvolutionEnabled) {
              // Schema evolution should allow type widening from SMALLINT to INT for salary column
              sql(mergeStmt)
              checkAnswer(
                sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
                Seq(
                  Row(1, 50000, "hr"),
                  Row(2, 200, "finance"),
                  Row(3, 300, "engineering"),
                  Row(4, 40000, "sales"),
                  Row(5, 500, "marketing")))
              val tableSchema = sql(s"SELECT * FROM $tableNameAsString").schema
              val salaryField = tableSchema.find(_.name == "salary").get
              assert(salaryField.dataType == IntegerType)
            } else {
              val exception = intercept[Exception] {
                sql(mergeStmt)
              }
              assert(exception.getMessage.contains(
                "Fail to assign a value of \"INT\" type to the \"SMALLINT\" " +
                  "type column or variable `salary` due to an overflow"))
            }
          }
        }
    }
  }

  test("merge into schema evolution type widening nested struct from int to long") {
    Seq(true, false).foreach { withSchemaEvolution =>
      withTable(tableNameAsString) {
        withTempView("source") {
          // Create table with nested struct containing int field
          createAndInitTable(
            s"""pk INT NOT NULL,
               |employee STRUCT<salary: INT, details: STRUCT<bonus: INT, years: INT>>,
               |dep STRING""".stripMargin,
            """{ "pk": 1, "employee": { "salary": 50000, "details":
              |{ "bonus": 5000, "years": 2 } }, "dep": "hr" }""".stripMargin.replace("\n", "")
              + "\n" +
              """{ "pk": 2, "employee": { "salary": 60000, "details":
                |{ "bonus": 6000, "years": 3 } }, "dep": "finance" }"""
                .stripMargin.replace("\n", "")
          )

          // Source data with long values that exceed int range for nested fields
          val sourceTableSchema = StructType(Seq(
            StructField("pk", IntegerType),
            StructField("employee", StructType(Seq(
              StructField("salary", IntegerType),
              StructField("details", StructType(Seq(
                StructField("bonus", LongType), // Changed from INT to LONG
                StructField("years", IntegerType)
              )))
            ))),
            StructField("dep", StringType)
          ))

          val data = Seq(
            Row(1, Row(75000, Row(3000000000L, 5)), "hr"),
            Row(3, Row(80000, Row(4000000000L, 1)), "engineering")
          )

          spark.createDataFrame(spark.sparkContext.parallelize(data), sourceTableSchema)
            .createOrReplaceTempView("source")

          val schemaEvolutionClause = if (withSchemaEvolution) "WITH SCHEMA EVOLUTION" else ""
          val mergeStmt =
            s"""MERGE $schemaEvolutionClause
               |INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN MATCHED THEN
               | UPDATE SET *
               |WHEN NOT MATCHED THEN
               | INSERT (pk, employee, dep) VALUES (s.pk, s.employee, s.dep)
               |""".stripMargin

          if (withSchemaEvolution) {
            // Schema evolution should allow type widening from INT to LONG for nested bonus field
            sql(mergeStmt)
            checkAnswer(
              sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
              Seq(
                Row(1, Row(75000, Row(3000000000L, 5)), "hr"),
                Row(2, Row(60000, Row(6000, 3)), "finance"),
                Row(3, Row(80000, Row(4000000000L, 1)), "engineering")
              ))

            val tableSchema = sql(s"SELECT * FROM $tableNameAsString").schema
            val employeeField = tableSchema.find(_.name == "employee").get.dataType
              .asInstanceOf[StructType]
            val detailsField = employeeField.find(_.name == "details").get.dataType
              .asInstanceOf[StructType]
            val bonusField = detailsField.find(_.name == "bonus").get
            assert(bonusField.dataType == LongType)
          } else {
            val exception = intercept[Exception] {
              sql(mergeStmt)
            }
            assert(exception.getMessage.contains("Fail to assign a value of \"BIGINT\" type " +
              "to the \"INT\" type column or variable `employee`.`details`.`bonus`" +
              " due to an overflow"))
          }
        }
      }
    }
  }

  test("merge into schema evolution type widening in array from int to long") {
    Seq(true, false).foreach { withSchemaEvolution =>
      withTable(tableNameAsString) {
        withTempView("source") {
          // Create table with array of int values
          createAndInitTable(
            s"""pk INT NOT NULL,
               |scores ARRAY<INT>,
               |dep STRING""".stripMargin,
            """{ "pk": 1, "scores": [1000, 2000, 3000], "dep": "hr" }
              |{ "pk": 2, "scores": [4000, 5000, 6000], "dep": "finance" }
              |""".stripMargin)

          // Source data with array of long values that exceed int range
          val sourceTableSchema = StructType(Seq(
            StructField("pk", IntegerType),
            StructField("scores", ArrayType(LongType)), // Changed from INT to LONG
            StructField("dep", StringType)
          ))

          val data = Seq(
            Row(1, Array(3000000000L, 4000000000L), "hr"),
            Row(3, Array(5000000000L, 6000000000L), "engineering")
          )

          spark.createDataFrame(spark.sparkContext.parallelize(data), sourceTableSchema)
            .createOrReplaceTempView("source")

          val schemaEvolutionClause = if (withSchemaEvolution) "WITH SCHEMA EVOLUTION" else ""
          val mergeStmt =
            s"""MERGE $schemaEvolutionClause
               |INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN MATCHED THEN
               | UPDATE SET *
               |WHEN NOT MATCHED THEN
               | INSERT (pk, scores, dep) VALUES (s.pk, s.scores, s.dep)
               |""".stripMargin

          if (withSchemaEvolution) {
            // Schema evolution should allow type widening from ARRAY<INT> to ARRAY<LONG>
            sql(mergeStmt)
            checkAnswer(
              sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
              Seq(
                Row(1, Array(3000000000L, 4000000000L), "hr"),
                Row(2, Array(4000, 5000, 6000), "finance"),
                Row(3, Array(5000000000L, 6000000000L), "engineering")
              ))

            val tableSchema = sql(s"SELECT * FROM $tableNameAsString").schema
            val scoresField = tableSchema.find(_.name == "scores").get
            val arrayElementType = scoresField.dataType.asInstanceOf[ArrayType].elementType
            assert(arrayElementType == LongType)
          } else {
            val exception = intercept[Exception] {
              sql(mergeStmt)
            }
            assert(exception.getMessage.contains("Fail to assign a value of \"BIGINT\" type " +
              "to the \"INT\" type column or variable `scores`.`element`" +
              " due to an overflow"))
          }
        }
      }
    }
  }

  test("merge into schema evolution type widening in map from int to long") {
    Seq(true, false).foreach { withSchemaEvolution =>
      withTable(tableNameAsString) {
        withTempView("source") {
          // Create table with map of string to int values
          createAndInitTable(
            s"""pk INT NOT NULL,
               |metrics MAP<STRING, INT>,
               |dep STRING""".stripMargin,
            """{ "pk": 1, "metrics": {"revenue": 100000, "profit": 50000}, "dep": "hr" }
              |{ "pk": 2, "metrics": {"revenue": 200000, "profit": 80000}, "dep": "finance" }
              |""".stripMargin)

          // Source data with map of string to long values that exceed int range
          val sourceTableSchema = StructType(Seq(
            StructField("pk", IntegerType),
            StructField("metrics", MapType(StringType, LongType)),
            StructField("dep", StringType)
          ))

          val data = Seq(
            Row(1, Map("revenue" -> 3000000000L, "profit" -> 1500000000L), "hr"),
            Row(3, Map("revenue" -> 4000000000L, "profit" -> 2000000000L), "engineering")
          )

          spark.createDataFrame(spark.sparkContext.parallelize(data), sourceTableSchema)
            .createOrReplaceTempView("source")

          val schemaEvolutionClause = if (withSchemaEvolution) "WITH SCHEMA EVOLUTION" else ""
          val mergeStmt =
            s"""MERGE $schemaEvolutionClause
               |INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN MATCHED THEN
               | UPDATE SET *
               |WHEN NOT MATCHED THEN
               | INSERT (pk, metrics, dep) VALUES (s.pk, s.metrics, s.dep)
               |""".stripMargin

          if (withSchemaEvolution) {
            // Schema evolution should allow type widening from MAP<_, INT> to MAP<_, LONG>
            sql(mergeStmt)
            checkAnswer(
              sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
              Seq(
                Row(1, Map("revenue" -> 3000000000L, "profit" -> 1500000000L), "hr"),
                Row(2, Map("revenue" -> 200000L, "profit" -> 80000L), "finance"),
                Row(3, Map("revenue" -> 4000000000L, "profit" -> 2000000000L), "engineering")
              ))

            val tableSchema = sql(s"SELECT * FROM $tableNameAsString").schema
            val metricsField = tableSchema.find(_.name == "metrics").get
            val mapValueType = metricsField.dataType.asInstanceOf[MapType].valueType
            assert(mapValueType == LongType)
          } else {
            val exception = intercept[Exception] {
              sql(mergeStmt)
            }
            assert(exception.getMessage.contains("Fail to assign a value of \"BIGINT\" type " +
              "to the \"INT\" type column or variable `metrics`.`value`" +
              " due to an overflow"))
          }
        }
      }
    }
  }

  test("merge into schema evolution type widening two types and adding two columns") {
    Seq(true, false).foreach { withSchemaEvolution =>
      withTable(tableNameAsString) {
        withTempView("source") {
          createAndInitTable(
            s"""pk INT NOT NULL,
               |score INT,
               |rating SHORT,
               |dep STRING""".stripMargin,
            """{ "pk": 1, "score": 100, "rating": 45, "dep": "premium" }
              |{ "pk": 2, "score": 85, "rating": 38, "dep": "standard" }
              |""".stripMargin)

          val sourceTableSchema = StructType(Seq(
            StructField("pk", IntegerType),
            StructField("score", LongType), // Widened from INT to LONG
            StructField("rating", IntegerType), // Widened from SHORT to INT
            StructField("dep", StringType),
            StructField("priority", StringType), // New column 1
            StructField("region", StringType) // New column 2
          ))

          val data = Seq(
            Row(1, 5000000000L, 485, "premium", "high", "west"),
            Row(3, 7500000000L, 495, "enterprise", "critical", "east")
          )

          spark.createDataFrame(spark.sparkContext.parallelize(data), sourceTableSchema)
            .createOrReplaceTempView("source")

          val schemaEvolutionClause = if (withSchemaEvolution) "WITH SCHEMA EVOLUTION" else ""
          val mergeStmt =
            s"""MERGE $schemaEvolutionClause
               |INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN MATCHED THEN
               | UPDATE SET *
               |WHEN NOT MATCHED THEN
               | INSERT *
               |""".stripMargin

          if (withSchemaEvolution) {
            sql(mergeStmt)
            checkAnswer(
              sql(s"SELECT * FROM $tableNameAsString ORDER BY pk"),
              Seq(
                Row(1, 5000000000L, 485, "premium", "high", "west"),
                Row(2, 85L, 38, "standard", null, null),
                Row(3, 7500000000L, 495, "enterprise", "critical", "east")
              ))

            val tableSchema = sql(s"SELECT * FROM $tableNameAsString").schema
            val scoreField = tableSchema.find(_.name == "score").get
            val ratingField = tableSchema.find(_.name == "rating").get
            val priorityField = tableSchema.find(_.name == "priority")
            val regionField = tableSchema.find(_.name == "region")

            // Verify type widening
            assert(scoreField.dataType == LongType)
            assert(ratingField.dataType == IntegerType)

            // Verify new columns added
            assert(priorityField.isDefined)
            assert(regionField.isDefined)
            assert(priorityField.get.dataType == StringType)
            assert(regionField.get.dataType == StringType)
          } else {
            val exception = intercept[Exception] {
              sql(mergeStmt)
            }
            assert(exception.getMessage.contains("Fail to assign a value of \"BIGINT\" type " +
              "to the \"INT\" type column or variable `score` due to an overflow."))
          }
        }
      }
    }
  }

  test("merge into schema evolution type promotion from int to struct not allowed") {
    Seq(true, false).foreach { withSchemaEvolution =>
      withTable(tableNameAsString) {
        withTempView("source") {
          createAndInitTable(
            s"""pk INT NOT NULL,
               |data INT,
               |dep STRING""".stripMargin,
            """{ "pk": 1, "data": 100, "dep": "test" }
              |{ "pk": 2, "data": 200, "dep": "sample" }
              |""".stripMargin)

          val sourceTableSchema = StructType(Seq(
            StructField("pk", IntegerType),
            StructField("data", StructType(Seq(
              StructField("value", IntegerType),
              StructField("timestamp", LongType)
            ))),
            StructField("dep", StringType)
          ))

          val data = Seq(
            Row(1, Row(150, 1634567890L), "test"),
            Row(3, Row(300, 1634567900L), "new")
          )

          spark.createDataFrame(spark.sparkContext.parallelize(data), sourceTableSchema)
            .createOrReplaceTempView("source")

          val schemaEvolutionClause = if (withSchemaEvolution) "WITH SCHEMA EVOLUTION" else ""
          val mergeStmt =
            s"""MERGE $schemaEvolutionClause
               |INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN MATCHED THEN
               | UPDATE SET *
               |WHEN NOT MATCHED THEN
               | INSERT *
               |""".stripMargin

          // Even with schema evolution, int to struct promotion should not be allowed
          val exception = intercept[Exception] {
            sql(mergeStmt)
          }

          if (withSchemaEvolution) {
            assert(exception.getMessage.contains("Failed to merge incompatible schemas"))
          } else {
            assert(exception.getMessage.contains(
              """Cannot write incompatible data for the table ``""".stripMargin))
          }
        }
      }
    }
  }

  test("merge into schema evolution add column with nested field and set explicit columns") {
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

  test("merge into schema evolution add column with nested struct and set all columns") {
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
             | UPDATE SET *
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin

        if (withSchemaEvolution) {
          sql(mergeStmt)
          checkAnswer(
            sql(s"SELECT * FROM $tableNameAsString"),
            Seq(Row(1, Row(10, Row(Seq(3, 4), Map("c" -> "d"), false)), "sales"),
              Row(2, Row(20, Row(Seq(4, 5), Map("e" -> "f"), true)), "engineering")))
        } else {
          val exception = intercept[org.apache.spark.sql.AnalysisException] {
            sql(mergeStmt)
          }
          assert(exception.errorClass.get == "INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS")
          assert(exception.getMessage.contains(
            "Cannot write extra fields `c3` to the struct `s`.`c2`"))
        }
      }
      sql(s"DROP TABLE IF EXISTS $tableNameAsString")
    }
  }

  test("merge into schema evolution replace column with nested struct and set explicit columns") {
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
              // removed column 'a'
              StructField("m", MapType(StringType, StringType)),
              StructField("c3", BooleanType) // new column
            )))
          ))),
          StructField("dep", StringType)
        ))
        val data = Seq(
          Row(1, Row(10, Row(Map("c" -> "d"), false)), "sales"),
          Row(2, Row(20, Row(Map("e" -> "f"), true)), "engineering")
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
             |     'c2', named_struct('a', array(-2), 'm', map('g', 'h'), 'c3', true)), src.dep)
             |""".stripMargin

        if (withSchemaEvolution) {
          sql(mergeStmt)
          checkAnswer(
            sql(s"SELECT * FROM $tableNameAsString"),
            Seq(Row(1, Row(-1, Row(Seq(-1), Map("k" -> "v"), false)), "hr"),
              Row(2, Row(20, Row(Seq(-2), Map("g" -> "h"), true)), "engineering")))
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

  // currently the source struct needs to be fully compatible with target struct
  // i.e. cannot remove a nested field
  test("merge into schema evolution replace column with nested field and set all columns") {
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
              // removed column 'a'
              StructField("m", MapType(StringType, StringType)),
              StructField("c3", BooleanType) // new column
            )))
          ))),
          StructField("dep", StringType)
        ))
        val data = Seq(
          Row(1, Row(10, Row(Map("c" -> "d"), false)), "sales"),
          Row(2, Row(20, Row(Map("e" -> "f"), true)), "engineering")
        )
        spark.createDataFrame(spark.sparkContext.parallelize(data), sourceTableSchema)
          .createOrReplaceTempView("source")

        val schemaEvolutionClause = if (withSchemaEvolution) "WITH SCHEMA EVOLUTION" else ""
        val exception = intercept[org.apache.spark.sql.AnalysisException] {
          sql(
            s"""MERGE $schemaEvolutionClause
               |INTO $tableNameAsString t
               |USING source src
               |ON t.pk = src.pk
               |WHEN MATCHED THEN
               | UPDATE SET *
               |WHEN NOT MATCHED THEN
               | INSERT *
               |""".stripMargin)
        }

        assert(exception.errorClass.get == "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA")
        assert(exception.getMessage.contains("Cannot find data for the output column `s`.`c2`.`a`"))
      }
      sql(s"DROP TABLE IF EXISTS $tableNameAsString")
    }
  }

  test("merge into schema evolution add column for struct in array and set all columns") {
    Seq(true, false).foreach { withSchemaEvolution =>
      withTempView("source") {
        createAndInitTable(
          s"""pk INT NOT NULL,
             |a ARRAY<STRUCT<c1: INT, c2: STRING>>,
             |dep STRING""".stripMargin,
          """{ "pk": 0, "a": [ { "c1": 1, "c2": "a" }, { "c1": 2, "c2": "b" } ], "dep": "sales"},
             { "pk": 1, "a": [ { "c1": 1, "c2": "a" }, { "c1": 2, "c2": "b" } ], "dep": "hr" }"""
            .stripMargin)

        val sourceTableSchema = StructType(Seq(
          StructField("pk", IntegerType, nullable = false),
          StructField("a", ArrayType(
            StructType(Seq(
              StructField("c1", IntegerType),
              StructField("c2", StringType),
              StructField("c3", BooleanType))))), // new column
          StructField("dep", StringType)))
        val data = Seq(
          Row(1, Array(Row(10, "c", true), Row(20, "d", false)), "hr"),
          Row(2, Array(Row(30, "d", false), Row(40, "e", true)), "engineering")
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
             | UPDATE SET *
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin

        if (withSchemaEvolution) {
          sql(mergeStmt)
          checkAnswer(
            sql(s"SELECT * FROM $tableNameAsString"),
            Seq(Row(0, Array(Row(1, "a", null), Row(2, "b", null)), "sales"),
              Row(1, Array(Row(10, "c", true), Row(20, "d", false)), "hr"),
              Row(2, Array(Row(30, "d", false), Row(40, "e", true)), "engineering")))
        } else {
          val exception = intercept[org.apache.spark.sql.AnalysisException] {
            sql(mergeStmt)
          }
          assert(exception.errorClass.get == "INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS")
          assert(exception.getMessage.contains(
            "Cannot write extra fields `c3` to the struct `a`.`element`"))
        }
      }
      sql(s"DROP TABLE IF EXISTS $tableNameAsString")
    }
  }

  test("merge into schema evolution add column for struct in map and set all columns") {
    Seq(true, false).foreach { withSchemaEvolution =>
      withTempView("source") {
        val schema =
          StructType(Seq(
            StructField("pk", IntegerType, nullable = false),
            StructField("m", MapType(
              StructType(Seq(StructField("c1", IntegerType))),
              StructType(Seq(StructField("c2", StringType))))),
            StructField("dep", StringType)))
        createTable(CatalogV2Util.structTypeToV2Columns(schema))

        val data = Seq(
          Row(0, Map(Row(10) -> Row("c")), "hr"),
          Row(1, Map(Row(20) -> Row("d")), "sales"))
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
          .writeTo(tableNameAsString).append()

        val sourceTableSchema = StructType(Seq(
          StructField("pk", IntegerType),
          StructField("m", MapType(
            StructType(Seq(StructField("c1", IntegerType), StructField("c3", BooleanType))),
            StructType(Seq(StructField("c2", StringType), StructField("c4", BooleanType))))),
          StructField("dep", StringType)))
        val sourceData = Seq(
          Row(1, Map(Row(10, true) -> Row("y", false)), "sales"),
          Row(2, Map(Row(20, false) -> Row("z", true)), "engineering")
        )
        spark.createDataFrame(spark.sparkContext.parallelize(sourceData), sourceTableSchema)
          .createOrReplaceTempView("source")

        val schemaEvolutionClause = if (withSchemaEvolution) "WITH SCHEMA EVOLUTION" else ""
        val mergeStmt =
          s"""MERGE $schemaEvolutionClause
             |INTO $tableNameAsString t
             |USING source src
             |ON t.pk = src.pk
             |WHEN MATCHED THEN
             | UPDATE SET *
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin

        if (withSchemaEvolution) {
          sql(mergeStmt)
          checkAnswer(
            sql(s"SELECT * FROM $tableNameAsString"),
            Seq(Row(0, Map(Row(10, null) -> Row("c", null)), "hr"),
              Row(1, Map(Row(10, true) -> Row("y", false)), "sales"),
              Row(2, Map(Row(20, false) -> Row("z", true)), "engineering")))
        } else {
          val exception = intercept[org.apache.spark.sql.AnalysisException] {
            sql(mergeStmt)
          }
          assert(exception.errorClass.get == "INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS")
          assert(exception.getMessage.contains(
            "Cannot write extra fields `c3` to the struct `m`.`key`"))
        }
      }
      sql(s"DROP TABLE IF EXISTS $tableNameAsString")
    }
  }

  test("merge into empty table with NOT MATCHED clause schema evolution") {
    Seq(true, false) foreach { withSchemaEvolution =>
      withTempView("source") {
        createTable("pk INT NOT NULL, salary INT, dep STRING")

        val sourceRows = Seq(
          (1, 100, "hr", true),
          (2, 200, "finance", false),
          (3, 300, "hr", true))
        sourceRows.toDF("pk", "salary", "dep", "active").createOrReplaceTempView("source")

        val schemaEvolutionClause = if (withSchemaEvolution) "WITH SCHEMA EVOLUTION" else ""

        sql(
          s"""MERGE $schemaEvolutionClause
             |INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED THEN
             | INSERT *
             |""".stripMargin)

        if (withSchemaEvolution) {
          checkAnswer(
            sql(s"SELECT * FROM $tableNameAsString"),
            Seq(
              Row(1, 100, "hr", true),
              Row(2, 200, "finance", false),
              Row(3, 300, "hr", true)))
        } else {
          checkAnswer(
            sql(s"SELECT * FROM $tableNameAsString"),
            Seq(
              Row(1, 100, "hr"),
              Row(2, 200, "finance"),
              Row(3, 300, "hr")))
        }
        sql("DROP TABLE IF EXISTS " + tableNameAsString)
      }
    }
  }

  private def findMergeExec(query: String): MergeRowsExec = {
    val plan = executeAndKeepPlan {
      sql(query)
    }
    collectFirst(plan) {
      case m: MergeRowsExec => m
    } match {
      case Some(m) => m
      case None =>
        fail("MergeRowsExec not found in the plan")
    }
  }

  private def getMergeSummary(): MergeSummary = {
    val table = catalog.loadTable(ident)
    table.asInstanceOf[InMemoryTable].commits.last.writeSummary.get
      .asInstanceOf[MergeSummary]
  }

  private def assertNoLeftBroadcastOrReplication(query: String): Unit = {
    val plan = executeAndKeepPlan {
      sql(query)
    }
    assertNoLeftBroadcastOrReplication(plan)
  }

  private def assertNoLeftBroadcastOrReplication(plan: SparkPlan): Unit = {
    val joins = plan.collect {
      case j: BroadcastHashJoinExec if j.buildSide == BuildLeft => j
      case j: BroadcastNestedLoopJoinExec if j.buildSide == BuildLeft => j
      case j: CartesianProductExec => j
    }
    assert(joins.isEmpty, "left side must not be broadcasted or replicated")
  }

  private def assertCardinalityError(query: String): Unit = {
    val e = intercept[SparkRuntimeException] {
      sql(query)
    }
    assert(e.getMessage.contains("ON search condition of the MERGE statement"))
  }

  private def assertMetric(
      mergeExec: MergeRowsExec,
      metricName: String,
      expected: Long): Unit = {
    mergeExec.metrics.get(metricName) match {
      case Some(metric) =>
        assert(metric.value == expected,
          s"Expected $metricName to be $expected, but got ${metric.value}")
      case None => fail(s"$metricName metric not found")
    }
  }
}
