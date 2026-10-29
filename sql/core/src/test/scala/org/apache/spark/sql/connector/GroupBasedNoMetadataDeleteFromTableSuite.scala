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

class GroupBasedNoMetadataDeleteFromTableSuite extends DeleteFromTableSuiteBase {

  override protected def extraTableProps: java.util.Map[String, String] = {
    val props = new java.util.HashMap[String, String]()
    props.put("no-metadata", "true")
    props
  }

  test("SPARK-60673: delete reads non-required metadata columns without writing them") {
    createAndInitTable("pk INT NOT NULL, id INT, dep STRING",
      """{ "pk": 1, "id": 1, "dep": "hr" }
        |{ "pk": 2, "id": 2, "dep": "hr" }
        |{ "pk": 3, "id": 1, "dep": "software" }
        |""".stripMargin)

    executeAndCheckScans(
      s"DELETE FROM $tableNameAsString WHERE _partition = 'hr' AND id < 2",
      primaryScanSchema = "pk INT, id INT, dep STRING, _partition STRING",
      groupFilterScanSchema = Some("id INT, dep STRING, _partition STRING"))

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Row(2, 2, "hr") :: Row(3, 1, "software") :: Nil)

    checkReplacedPartitions(Seq("hr"))
    checkLastWriteInfo(expectedRowSchema = table.schema)
    checkLastWriteLog(writeLogEntry(data = Row(2, 2, "hr")))
  }
}
