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
import org.apache.spark.sql.functions._

class UpdateTableAPISuite extends RowLevelOperationSuiteBase {

  import testImplicits._

  test("Basic Update") {
    createAndInitTable("pk INT, salary INT, dep STRING",
      """{ "pk": 1, "salary": 300, "dep": 'hr' }
        |{ "pk": 2, "salary": 150, "dep": 'software' }
        |{ "pk": 3, "salary": 120, "dep": 'hr' }
        |""".stripMargin)

    spark.catalog.updateTable(
        tableNameAsString, Map("salary" -> lit(-1)), $"pk" >= 2)

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, 300, "hr"),
        Row(2, -1, "software"),
        Row(3, -1, "hr")))
  }

  test("Update without where clause") {
    createAndInitTable("pk INT, salary INT, dep STRING",
      """{ "pk": 1, "salary": 300, "dep": 'hr' }
        |{ "pk": 2, "salary": 150, "dep": 'software' }
        |{ "pk": 3, "salary": 120, "dep": 'hr' }
        |""".stripMargin)

    spark.catalog.updateTable(tableNameAsString,
      Map("dep" -> lit("software")))

    checkAnswer(
      sql(s"SELECT * FROM $tableNameAsString"),
      Seq(
        Row(1, 300, "software"),
        Row(2, 150, "software"),
        Row(3, 120, "software")))
  }
}
