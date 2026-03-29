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

package org.apache.spark.sql.hive.execution.command

import org.apache.spark.sql.execution.command.v1

/**
 * The class contains tests for the `ALTER TABLE ... CLUSTER BY` command to check V1 Hive external
 * table catalog.
 */
class AlterTableClusterBySuite extends v1.AlterTableClusterBySuiteBase with CommandSuiteBase {
  // Hive doesn't support nested column names with space and dot.
  override protected val nestedColumnSchema: String =
    "col1 INT, col2 STRUCT<col3 INT, col4 INT>"
  override protected val nestedClusteringColumns: Seq[String] =
    Seq("col2.col3")
  override protected val nestedClusteringColumnsNew: Seq[String] =
    Seq("col2.col4")

  // Hive catalog doesn't support column names with commas.
  override def excluded: Seq[String] = Seq(
    s"$command using Hive V1 catalog V1 command: test clustering columns with comma",
    s"$command using Hive V1 catalog V2 command: test clustering columns with comma")

  override def commandVersion: String = super[AlterTableClusterBySuiteBase].commandVersion
}
