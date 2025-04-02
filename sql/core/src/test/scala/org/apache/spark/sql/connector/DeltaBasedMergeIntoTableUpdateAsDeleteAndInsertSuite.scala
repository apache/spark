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

class DeltaBasedMergeIntoTableUpdateAsDeleteAndInsertSuite
  extends DeltaBasedMergeIntoTableSuiteBase {

  import testImplicits._

  override protected lazy val extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("supports-deltas", "true")
    props.put("split-updates", "true")
    props
  }

  test("merge handles metadata columns correctly") {
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
          Row(6, 0, "new"))) // insert

      checkLastWriteInfo(
        expectedRowSchema = table.schema,
        expectedRowIdSchema = Some(StructType(Array(PK_FIELD))),
        expectedMetadataSchema = Some(StructType(Array(PARTITION_FIELD, INDEX_FIELD_NULLABLE))))

      checkLastWriteLog(
        deleteWriteLogEntry(id = 1, metadata = Row("hr", null)),
        deleteWriteLogEntry(id = 3, metadata = Row("hr", null)),
        reinsertWriteLogEntry(metadata = Row("hr", null), data = Row(3, 301, "hr")),
        deleteWriteLogEntry(id = 4, metadata = Row("hr", null)),
        reinsertWriteLogEntry(metadata = Row("hr", null), data = Row(4, 401, "hr")),
        deleteWriteLogEntry(id = 5, metadata = Row("hr", null)),
        reinsertWriteLogEntry(metadata = Row("hr", null), data = Row(5, 501, "hr")),
        insertWriteLogEntry(data = Row(6, 0, "new")))
    }
  }
}
