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

/**
 * Tests for SPARK-57685: MERGE INTO with non-deterministic expressions in action
 * assignments should pass analysis and execute correctly on DSv2 row-level tables.
 *
 * MergeRows now implements SupportsNonDeterministicExpression, allowing non-deterministic
 * expressions like uuid() and rand() in MERGE action assignments.
 */
class MergeNonDeterministicReproSuite extends MergeIntoTableSuiteBase {

  import testImplicits._

  // Group-based (copy-on-write) path: uses MergeRows -> ReplaceData
  test("SPARK-57685: group-based MERGE with non-deterministic UPDATE SET succeeds") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }""")

      Seq(1).toDF("pk").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET dep = CAST(uuid() AS STRING)
           |""".stripMargin)

      val result = sql(s"SELECT pk, dep FROM $tableNameAsString WHERE pk = 1")
      val rows = result.collect()
      assert(rows.length == 1, "Expected exactly 1 row after UPDATE")
      // uuid() produces a non-null 36-char string
      val dep = rows(0).getString(1)
      assert(dep != null && dep.length == 36,
        s"Expected uuid() to produce a 36-char string, got: $dep")
    }
  }

  test("SPARK-57685: group-based MERGE with non-deterministic INSERT VALUES succeeds") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }""")

      Seq(1, 99).toDF("pk").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET salary = salary + 1
           |WHEN NOT MATCHED THEN
           | INSERT (pk, salary, dep) VALUES (s.pk, CAST(rand() * 1000 AS INT), 'new')
           |""".stripMargin)

      // Original row updated
      val updated = sql(
        s"SELECT salary FROM $tableNameAsString WHERE pk = 1").collect()
      assert(updated.length == 1 && updated(0).getInt(0) == 101)

      // Inserted row has non-null salary from rand()
      val inserted = sql(
        s"SELECT pk, salary, dep FROM $tableNameAsString WHERE pk = 99").collect()
      assert(inserted.length == 1, "Expected 1 inserted row")
      assert(inserted(0).getInt(1) >= 0 && inserted(0).getInt(1) < 1000,
        s"Expected rand()-derived salary in [0, 1000), got: ${inserted(0).getInt(1)}")
      assert(inserted(0).getString(2) == "new")
    }
  }
}

class MergeNonDeterministicDeltaReproSuite extends MergeIntoTableSuiteBase {

  import testImplicits._

  override protected def deltaMerge = true

  override protected lazy val extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("supports-deltas", "true")
    props
  }

  // Delta-based (merge-on-read) path: uses MergeRows -> WriteDelta
  test("SPARK-57685: delta-based MERGE with non-deterministic UPDATE SET succeeds") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }""")

      Seq(1).toDF("pk").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET dep = CAST(uuid() AS STRING)
           |""".stripMargin)

      val result = sql(s"SELECT pk, dep FROM $tableNameAsString WHERE pk = 1")
      val rows = result.collect()
      assert(rows.length == 1, "Expected exactly 1 row after UPDATE")
      val dep = rows(0).getString(1)
      assert(dep != null && dep.length == 36,
        s"Expected uuid() to produce a 36-char string, got: $dep")
    }
  }

  test("SPARK-57685: delta-based MERGE with non-deterministic INSERT VALUES succeeds") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }""")

      Seq(1, 99).toDF("pk").createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET salary = salary + 1
           |WHEN NOT MATCHED THEN
           | INSERT (pk, salary, dep) VALUES (s.pk, CAST(rand() * 1000 AS INT), 'new')
           |""".stripMargin)

      // Original row updated
      val updated = sql(
        s"SELECT salary FROM $tableNameAsString WHERE pk = 1").collect()
      assert(updated.length == 1 && updated(0).getInt(0) == 101)

      // Inserted row has non-null salary from rand()
      val inserted = sql(
        s"SELECT pk, salary, dep FROM $tableNameAsString WHERE pk = 99").collect()
      assert(inserted.length == 1, "Expected 1 inserted row")
      assert(inserted(0).getInt(1) >= 0 && inserted(0).getInt(1) < 1000,
        s"Expected rand()-derived salary in [0, 1000), got: ${inserted(0).getInt(1)}")
      assert(inserted(0).getString(2) == "new")
    }
  }
}
