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
import org.apache.spark.sql.types.StructType

class DeltaBasedNoMetadataMergeIntoTableSuite extends MergeIntoTableSuiteBase {

  import testImplicits._

  override protected def deltaMerge = true

  override protected def extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("supports-deltas", "true")
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

      checkLastWriteInfo(
        expectedRowSchema = StructType(table.schema.map {
          case attr if attr.name == "salary" => attr.copy(nullable = false) // input is not nullable
          case attr => attr
        }),
        expectedRowIdSchema = Some(StructType(Array(PK_FIELD))))

      checkLastWriteLog(
        updateWriteLogEntry(id = 1, metadata = null, data = Row(1, 10, "hr")),
        insertWriteLogEntry(data = Row(5, 0, "hr")))
    }
  }
}
