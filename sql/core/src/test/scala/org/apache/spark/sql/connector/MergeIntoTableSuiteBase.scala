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
import org.apache.spark.sql.connector.catalog.{Column, ColumnDefaultValue}
import org.apache.spark.sql.connector.expressions.LiteralValue
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, CartesianProductExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType}

abstract class MergeIntoTableSuiteBase extends RowLevelOperationSuiteBase {

  import testImplicits._

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
}
