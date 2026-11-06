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

class GroupBasedNoMetadataMergeIntoTableSuite extends MergeIntoTableSuiteBase {

  import testImplicits._

  override protected def extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("no-metadata", "true")
    props
  }

  test("SPARK-60673: merge reads non-required metadata columns without writing them") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |""".stripMargin)

      val sourceDF = Seq(1, 2, 5).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk
           |WHEN MATCHED AND t._partition = 'hr' THEN
           | UPDATE SET t.salary = t.index + 10
           |WHEN NOT MATCHED THEN
           | INSERT (pk, salary, dep) VALUES (s.pk, 0, 'hr')
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, 10, "hr"), // update
          Row(2, 200, "software"), // unchanged
          Row(3, 300, "hr"), // unchanged
          Row(5, 0, "hr"))) // insert

      checkLastWriteInfo(expectedRowSchema = table.schema)
      checkLastWriteLog(
        writeLogEntry(data = Row(1, 10, "hr")),
        writeLogEntry(data = Row(2, 200, "software")),
        writeLogEntry(data = Row(3, 300, "hr")),
        writeLogEntry(data = Row(5, 0, "hr")))
    }
  }

  test("SPARK-60673: merge reads non-required metadata columns only used in conditions") {
    withTempView("source") {
      createAndInitTable("pk INT NOT NULL, salary INT, dep STRING",
        """{ "pk": 1, "salary": 100, "dep": "hr" }
          |{ "pk": 2, "salary": 200, "dep": "software" }
          |{ "pk": 3, "salary": 300, "dep": "hr" }
          |""".stripMargin)

      val sourceDF = Seq(1, 2).toDF("pk")
      sourceDF.createOrReplaceTempView("source")

      // `_partition` is only in the ON condition and `index` is only in the condition of
      // the NOT MATCHED BY SOURCE clause, which also disables the runtime group filter
      sql(
        s"""MERGE INTO $tableNameAsString t
           |USING source s
           |ON t.pk = s.pk AND t._partition = 'hr'
           |WHEN MATCHED THEN
           | UPDATE SET t.salary = -1
           |WHEN NOT MATCHED BY SOURCE AND t.index = 1 THEN
           | DELETE
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableNameAsString"),
        Seq(
          Row(1, -1, "hr"), // update
          Row(2, 200, "software"))) // unchanged

      checkLastWriteInfo(expectedRowSchema = table.schema)
      checkLastWriteLog(
        writeLogEntry(data = Row(1, -1, "hr")),
        writeLogEntry(data = Row(2, 200, "software")))
    }
  }
}
