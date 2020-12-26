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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{QueryTest, Row}

trait AlterTableRenamePartitionSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "ALTER TABLE .. RENAME PARTITION"

  protected def createSinglePartTable(t: String): Unit = {
    sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
    sql(s"INSERT INTO $t PARTITION (id = 1) SELECT 'abc'")
  }

  test("rename without explicitly specifying database") {
    withNamespaceAndTable("ns", "tbl") { t =>
      createSinglePartTable(t)
      checkPartitions(t, Map("id" -> "1"))

      sql(s"ALTER TABLE $t PARTITION (id = 1) RENAME TO PARTITION (id = 2)")
      checkPartitions(t, Map("id" -> "2"))
      checkAnswer(sql(s"SELECT id, data FROM $t"), Row(2, "abc"))
    }
  }
}
