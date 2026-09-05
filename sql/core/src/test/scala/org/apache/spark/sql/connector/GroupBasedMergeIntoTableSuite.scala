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

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Exists
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

class GroupBasedMergeIntoTableNoCodegenSuite extends GroupBasedMergeIntoTableSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
}

class GroupBasedMergeIntoTableSuite extends MergeIntoTableSuiteBase {

  import testImplicits._

  test("merge handles metadata columns correctly") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |{ "pk": 4, "salary": 400, "dep": "hr" }
          |{ "pk": 5, "salary": 500, "dep": "hr" }
          |{ "pk": 7, "salary": 700, "dep": "hr" }
          |""".stripMargin)

      val sourceDF = Seq(3, 4, 5, 6, 7).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED AND t.pk != 7 THEN
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
          Row(6, 0, "new"), // insert
          Row(7, 700, "hr"))) // unchanged

      checkLastWriteInfo(
        expectedRowSchema = table.schema,
        expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))

      checkLastWriteLog(
        writeWithMetadataLogEntry(metadata = Row("software", 0), data = Row(2, 200, "software")),
        writeWithMetadataLogEntry(metadata = Row("hr", null), data = Row(3, 301, "hr")),
        writeWithMetadataLogEntry(metadata = Row("hr", null), data = Row(4, 401, "hr")),
        writeWithMetadataLogEntry(metadata = Row("hr", null), data = Row(5, 501, "hr")),
        writeLogEntry(data = Row(6, 0, "new")),
        writeWithMetadataLogEntry(metadata = Row("hr", 4), data = Row(7, 700, "hr")))
    }
  }

  test("merge runtime filtering is disabled with NOT MATCHED BY SOURCE clauses") {
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

      executeAndCheckScans(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET t.salary = t.salary + 1
           |WHEN NOT MATCHED THEN
           | INSERT (pk, salary, dep) VALUES (s.pk, 0, 'hr')
           |WHEN NOT MATCHED BY SOURCE THEN
           | DELETE
           |""".stripMargin,
        primaryScanSchema = "pk INT, salary INT, dep STRING, _partition STRING, index INT",
        groupFilterScanSchema = None)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 101, "hr"), // update
          Row(2, 201, "hr"), // update
          Row(3, 301, "hr"), // update
          Row(6, 0, "hr"))) // insert

      checkReplacedPartitions(Seq("hr", "software"))
    }
  }

  test("merge runtime group filtering (DPP enabled)") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true") {
      checkMergeRuntimeGroupFiltering()
    }
  }

  test("merge runtime group filtering (DPP disabled)") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false") {
      checkMergeRuntimeGroupFiltering()
    }
  }

  test("merge runtime group filtering (AQE enabled)") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      checkMergeRuntimeGroupFiltering()
    }
  }

  test("merge runtime group filtering (AQE disabled)") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      checkMergeRuntimeGroupFiltering()
    }
  }

  private def checkMergeRuntimeGroupFiltering(): Unit = {
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

      executeAndCheckScans(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET t.salary = t.salary + 1
           |WHEN NOT MATCHED THEN
           | INSERT (pk, salary, dep) VALUES (s.pk, 0, 'hr')
           |""".stripMargin,
        primaryScanSchema = "pk INT, salary INT, dep STRING, _partition STRING, index INT",
        groupFilterScanSchema = Some("pk INT, dep STRING"))

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 101, "hr"), // update
          Row(2, 201, "hr"), // update
          Row(3, 301, "hr"), // update
          Row(4, 400, "software"), // unchanged
          Row(5, 500, "software"), // unchanged
          Row(6, 0, "hr"))) // insert

      checkReplacedPartitions(Seq("hr"))
    }
  }

  test("merge pushes conditional NOT MATCHED BY SOURCE predicates to target") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "finance" }
          |""".stripMargin)

      val sourceDF = Seq(1, 3, 6).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      // every NOT MATCHED BY SOURCE clause is conditional, so the ON condition and that clause's
      // condition can be pushed to the target table as a disjunction; a target row satisfying
      // neither is only carried over, so its group doesn't have to be read at all
      val (cond, groupFilterCond) = executeAndKeepConditions {
        sql(
          s"""MERGE INTO $tableNameAsString t
             |USING source s
             |ON t.pk = s.pk AND t.dep IN ('hr', 'finance')
             |WHEN MATCHED THEN
             | UPDATE SET t.salary = t.salary + 1
             |WHEN NOT MATCHED THEN
             | INSERT (pk, salary, dep) VALUES (s.pk, 0, 'hr')
             |WHEN NOT MATCHED BY SOURCE AND t.dep IN ('hr', 'finance') THEN
             | DELETE
             |""".stripMargin)
      }

      assert(cond.sql == "(t.dep IN ('hr', 'finance'))", s"unexpected pushable condition: $cond")
      assert(groupFilterCond.isEmpty, "runtime group filtering must stay disabled")

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 101, "hr"), // update
          Row(2, 200, "software"), // unchanged, never read
          Row(3, 301, "finance"), // update
          Row(6, 0, "hr"))) // insert

      // "software" can match neither the ON condition nor the NOT MATCHED BY SOURCE condition,
      // so the pushed predicate keeps that group out of the scan and out of the rewrite
      checkReplacedPartitions(Seq("hr", "finance"))
    }
  }

  test("merge with unmatched ON condition pushes NOT MATCHED BY SOURCE predicates to target") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "finance" }
          |""".stripMargin)

      val sourceDF = Seq(5, 6).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      // this mirrors a "replace where" command: the ON condition never matches, so all source
      // rows are inserted and target rows are deleted only where the NOT MATCHED BY SOURCE
      // condition holds, which is exactly the predicate that ends up pushed to the target
      val (cond, groupFilterCond) = executeAndKeepConditions {
        sql(
          s"""MERGE INTO $tableNameAsString t
             |USING source s
             |ON 1 = 0
             |WHEN NOT MATCHED THEN
             | INSERT (pk, salary, dep) VALUES (s.pk, 0, 'new')
             |WHEN NOT MATCHED BY SOURCE AND t.dep IN ('hr', 'finance') THEN
             | DELETE
             |""".stripMargin)
      }

      assert(cond.sql == "(t.dep IN ('hr', 'finance'))", s"unexpected pushable condition: $cond")
      assert(groupFilterCond.isEmpty, "runtime group filtering must stay disabled")

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(2, 200, "software"), // unchanged, never read
          Row(5, 0, "new"), // insert
          Row(6, 0, "new"))) // insert

      checkReplacedPartitions(Seq("hr", "finance"))
    }
  }

  test("merge does not double plan table (group filter enabled)") {
    withSQLConf(SQLConf.RUNTIME_ROW_LEVEL_OPERATION_GROUP_FILTER_ENABLED.key -> "true") {
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        sql(
          s"""CREATE TEMP VIEW source AS
             |SELECT pk, salary FROM $tableNameAsString WHERE salary > 150
             |""".stripMargin)

        val (_, groupFilterCond) = executeAndKeepConditions {
          sql(
            s"""MERGE INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN MATCHED THEN
               | UPDATE SET t.salary = s.salary + 1
               |WHEN NOT MATCHED THEN
               | INSERT (pk, salary, dep) VALUES (s.pk, s.salary, 'new')
               |""".stripMargin)
        }

        groupFilterCond match {
          case Some(p: Exists) => assertNoScanPlanning(p.plan)
          case _ => fail(s"unexpected group filter: $groupFilterCond")
        }

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(Row(1, 100, "hr"), Row(2, 201, "software"), Row(3, 301, "hr")))

        checkReplacedPartitions(Seq("software", "hr"))
      }
    }
  }

  test("merge does not double plan table (group filter disabled)") {
    withSQLConf(SQLConf.RUNTIME_ROW_LEVEL_OPERATION_GROUP_FILTER_ENABLED.key -> "false") {
      withTempView("source") {
        createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
          """{ "pk": 1, "salary": 100, "dep": "hr" }
            |{ "pk": 2, "salary": 200, "dep": "software" }
            |{ "pk": 3, "salary": 300, "dep": "hr" }
            |""".stripMargin)

        sql(
          s"""CREATE TEMP VIEW source AS
             |SELECT pk, salary FROM $tableNameAsString WHERE salary > 150
             |""".stripMargin)

        val (_, groupFilterCond) = executeAndKeepConditions {
          sql(
            s"""MERGE INTO $tableNameAsString t
               |USING source s
               |ON t.pk = s.pk
               |WHEN MATCHED THEN
               | UPDATE SET t.salary = s.salary + 1
               |WHEN NOT MATCHED THEN
               | INSERT (pk, salary, dep) VALUES (s.pk, s.salary, 'new')
               |""".stripMargin)
        }

        assert(groupFilterCond.isEmpty, "group filter must be disabled")

        checkAnswer(
          sql(s"SELECT * FROM $tableNameAsString"),
          Seq(Row(1, 100, "hr"), Row(2, 201, "software"), Row(3, 301, "hr")))

        checkReplacedPartitions(Seq("hr", "software"))
      }
    }
  }
}
