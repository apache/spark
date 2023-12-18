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
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType

abstract class DeltaBasedMergeIntoDFWriterV2SuiteBase extends MergeIntoDFWriterV2SuiteBase {

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

      val expectedScanSchema = "pk INT, salary INT, _partition STRING"
      val tableNameAsString = "cat.ns1.test_table"

      val executedPlan = executeAndKeepPlan {
        spark.table("source")
          .mergeInto(tableNameAsString, $"source.pk"  === col(tableNameAsString + ".pk"))
          .whenMatched(col(tableNameAsString + ".salary") === 200)
          .updateAll()
          .merge()
      }

      checkScan(executedPlan, expectedScanSchema)
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

      val expectedScanSchema = "pk INT, salary INT, _partition STRING"
      val executedPlan = executeAndKeepPlan {
        spark.table("source")
          .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
          .whenMatched(col(tableNameAsString + ".salary") === 200)
          .delete()
          .merge()
      }

      checkScan(executedPlan, expectedScanSchema)
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

      val expectedScanSchema = "pk INT"
      val executedPlan = executeAndKeepPlan {
        spark.table("source")
          .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
          .whenNotMatched()
          .insertAll()
          .merge()
      }

      checkScan(executedPlan, expectedScanSchema)
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

      val expectedScanSchema = "pk INT, salary INT, _partition STRING"
      val executedPlan = executeAndKeepPlan {
        spark.table("source")
          .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
          .whenNotMatchedBySource($"salary" === 100)
          .update(Map(
            "country" -> lit("invalid"),
            "dep" -> lit("invalid")
          ))
          .merge()
      }

      checkScan(executedPlan, expectedScanSchema)
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

      val expectedScanSchema = "pk INT, salary INT, _partition STRING"
      val executedPlan = executeAndKeepPlan {
        spark.table("source")
          .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
          .whenNotMatchedBySource($"salary" === 100)
          .delete()
          .merge()
      }

      checkScan(executedPlan, expectedScanSchema)
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

      val expectedScanSchema = "pk INT, salary INT, _partition STRING"
      val executedPlan = executeAndKeepPlan {
        spark.table("source")
          .mergeInto(tableNameAsString, $"source.pk" === col(tableNameAsString + ".pk"))
          .whenMatched()
          .updateAll()
          .whenNotMatched()
          .insertAll()
          .whenNotMatchedBySource($"salary" === 100)
          .delete()
          .merge()
      }

      checkScan(executedPlan, expectedScanSchema)
      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(2, 200, "india", "finance"), // update
          Row(3, 300, "china", "software"))) // insert
    }
  }

  protected def checkScan(
      executedPlan: SparkPlan,
      expectedScanSchema: String): Unit = {
    val scan = collect(executedPlan) {
      case s: BatchScanExec => s
    }.head
    assert(DataTypeUtils.sameType(scan.schema, StructType.fromDDL(expectedScanSchema)))
  }
}
