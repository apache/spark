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

abstract class DeltaBasedMergeIntoTableSuiteBase extends MergeIntoTableSuiteBase {

  import testImplicits._

  test("merge into schema pruning with WHEN MATCHED clause (update)") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, country STRING, dep STRING",
        """{ "pk": 1, "salary": 100, "country": "uk", "dep": "hr" }
          |{ "pk": 2, "salary": 200, "country": "us", "dep": "corrupted" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, 100, "france", "software"),
        (2, 200, "india", "finance"),
        (3, 300, "china", "software"))
      sourceRows.toDF("pk", "salary", "country", "dep").createOrReplaceTempView("source")

      executeAndCheckScan(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED AND t.salary = 200 THEN
           | UPDATE SET *
           |""".stripMargin,
        // `pk` is used in the SEARCH condition
        // `salary` is used in the UPDATE condition
        // `_partition` is used in the requested write distribution
        expectedScanSchema = "pk INT, salary INT, _partition STRING")

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 100, "uk", "hr"), // unchanged
          Row(2, 200, "india", "finance"))) // update
    }
  }

  test("merge into schema pruning with WHEN MATCHED clause (delete)") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, country STRING, dep STRING",
        """{ "pk": 1, "salary": 100, "country": "uk", "dep": "hr" }
          |{ "pk": 2, "salary": 200, "country": "us", "dep": "corrupted" }
          |""".stripMargin)

      Seq(1, 2, 3).toDF("pk").createOrReplaceTempView("source")

      executeAndCheckScan(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED AND t.salary = 200 THEN
           | DELETE
           |""".stripMargin,
        // `pk` is used in the SEARCH condition
        // `salary` is used in the DELETE condition
        // `_partition` is used in the requested write distribution
        expectedScanSchema = "pk INT, salary INT, _partition STRING")

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(Row(1, 100, "uk", "hr"))) // unchanged
    }
  }

  test("merge into schema pruning with WHEN NOT MATCHED clause") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, country STRING, dep STRING",
        """{ "pk": 1, "salary": 100, "country": "uk", "dep": "hr" }
          |{ "pk": 2, "salary": 200, "country": "us", "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (1, 100, "france", "software"),
        (2, 200, "india", "finance"),
        (3, 300, "china", "software"))
      sourceRows.toDF("pk", "salary", "country", "dep").createOrReplaceTempView("source")

      executeAndCheckScan(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN NOT MATCHED THEN
           | INSERT *
           |""".stripMargin,
        // `pk` is used in the SEARCH condition
        expectedScanSchema = "pk INT")

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 100, "uk", "hr"), // unchanged
          Row(2, 200, "us", "software"), // unchanged
          Row(3, 300, "china", "software"))) // insert
    }
  }

  test("merge into schema pruning with WHEN NOT MATCHED BY SOURCE clause (update)") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, country STRING, dep STRING",
        """{ "pk": 1, "salary": 100, "country": "uk", "dep": "hr" }
          |{ "pk": 2, "salary": 200, "country": "us", "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (2, 200, "india", "finance"),
        (3, 300, "china", "software"))
      sourceRows.toDF("pk", "salary", "country", "dep").createOrReplaceTempView("source")

      executeAndCheckScan(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN NOT MATCHED BY SOURCE AND salary = 100 THEN
           | UPDATE SET country = 'invalid', dep = 'invalid'
           |""".stripMargin,
        // `pk` is used in the SEARCH condition
        // `salary` is used in the UPDATE condition
        // `_partition` is used in the requested write distribution
        expectedScanSchema = "pk INT, salary INT, _partition STRING")

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 100, "invalid", "invalid"), // update
          Row(2, 200, "us", "software"))) // unchanged
    }
  }

  test("merge into schema pruning with WHEN NOT MATCHED BY SOURCE clause (delete)") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, country STRING, dep STRING",
        """{ "pk": 1, "salary": 100, "country": "uk", "dep": "hr" }
          |{ "pk": 2, "salary": 200, "country": "us", "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (2, 200, "india", "finance"),
        (3, 300, "china", "software"))
      sourceRows.toDF("pk", "salary", "country", "dep").createOrReplaceTempView("source")

      executeAndCheckScan(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN NOT MATCHED BY SOURCE AND salary = 100 THEN
           | DELETE
           |""".stripMargin,
        // `pk` is used in the SEARCH condition
        // `salary` is used in the UPDATE condition
        // `_partition` is used in the requested write distribution
        expectedScanSchema = "pk INT, salary INT, _partition STRING")

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(Row(2, 200, "us", "software"))) // unchanged
    }
  }

  test("merge into schema pruning with clauses") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, country STRING, dep STRING",
        """{ "pk": 1, "salary": 100, "country": "uk", "dep": "hr" }
          |{ "pk": 2, "salary": 200, "country": "us", "dep": "software" }
          |""".stripMargin)

      val sourceRows = Seq(
        (2, 200, "india", "finance"),
        (3, 300, "china", "software"))
      sourceRows.toDF("pk", "salary", "country", "dep").createOrReplaceTempView("source")

      executeAndCheckScan(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED THEN
           | UPDATE SET *
           |WHEN NOT MATCHED THEN
           | INSERT *
           |WHEN NOT MATCHED BY SOURCE AND salary = 100 THEN
           | DELETE
           |""".stripMargin,
        // `pk` is used in the SEARCH condition
        // `salary` is used in the DELETE condition
        // `_partition` is used in the requested write distribution
        expectedScanSchema = "pk INT, salary INT, _partition STRING")

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(2, 200, "india", "finance"), // update
          Row(3, 300, "china", "software"))) // insert
    }
  }

  test("SPARK-51513: Fix RewriteMergeIntoTable rule produces unresolved plan") {
    val sourceTableName: String = "cat.ns1.test_source"
    withTable(sourceTableName) {
      createTable("pk INT NOT NULL, salary INT, dep STRING")
      sql(s"CREATE TABLE $sourceTableName (PK INT NOT NULL, SALARY INT, DEP VARCHAR(10))")
      sql(s"INSERT INTO $sourceTableName values " +
        s"(1, 100, 'hr1'), (2, 200, 'hr2'), (3, 300, 'hr3')")
      sql(s"""MERGE INTO $tableNameAsString t
             |USING $sourceTableName s
             |ON t.pk = s.pk
             |WHEN NOT MATCHED AND s.pk >= 2 THEN
             | INSERT *
             |""".stripMargin)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(Row(2, 200, "hr2"), Row(3, 300, "hr3")))
    }
  }
}
