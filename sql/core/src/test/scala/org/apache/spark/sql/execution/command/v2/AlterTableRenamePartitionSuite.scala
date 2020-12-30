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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.command

/**
 * The class contains tests for the `ALTER TABLE .. RENAME PARTITION` command
 * to check V2 table catalogs.
 */
class AlterTableRenamePartitionSuite
  extends command.AlterTableRenamePartitionSuiteBase
  with CommandSuiteBase {

  test("with location") {
    withNamespaceAndTable("ns", "tbl") { t =>
      createSinglePartTable(t)
      val loc = "location1"
      sql(s"ALTER TABLE $t ADD PARTITION (id = 2) LOCATION '$loc'")
      sql(s"INSERT INTO $t PARTITION (id = 2) SELECT 'def'")
      checkPartitions(t, Map("id" -> "1"), Map("id" -> "2"))
      checkLocation(t, Map("id" -> "2"), loc)

      sql(s"ALTER TABLE $t PARTITION (id = 2) RENAME TO PARTITION (id = 3)")
      checkPartitions(t, Map("id" -> "1"), Map("id" -> "3"))
      // `InMemoryPartitionTableCatalog` should keep the original location
      checkLocation(t, Map("id" -> "3"), loc)
      checkAnswer(sql(s"SELECT id, data FROM $t WHERE id = 3"), Row(3, "def"))
    }
  }
}
