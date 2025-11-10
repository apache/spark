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

package org.apache.spark.sql.execution.datasources.v2

import java.util.Locale

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connector.read.SupportsPushDownJoin.ColumnWithAlias

class DSV2JoinPushDownAliasGenerationSuite extends SparkFunSuite {

  private def assertAliases(
    leftInput: Array[String],
    rightInput: Array[String],
    expectedLeft: Array[ColumnWithAlias],
    expectedRight: Array[ColumnWithAlias]
  ): Unit = {
    val (actualLeft, actualRight) = V2ScanRelationPushDown
      .generateColumnAliasesForDuplicatedName(leftInput, rightInput)

    val uniqName: ColumnWithAlias => String = col => {
      if (col.alias() == null) col.colName() else col.alias().toLowerCase(Locale.ROOT)
    }
    // Ensure no duplicate column names after ignoring capitalization
    assert((actualLeft ++ actualRight).map(uniqName).distinct.length
      == actualLeft.length + actualRight.length)

    assert(
      actualLeft === expectedLeft,
      s"""Left side aliases mismatch.
         |Expected: ${expectedLeft.map(_.alias()).mkString(", ")}
         |Actual: ${actualLeft.map(_.alias()).mkString(", ")}""".stripMargin
    )

    assert(
      actualRight === expectedRight,
      s"""Right side aliases mismatch.
         |Expected: ${expectedRight.map(_.alias()).mkString(", ")}
         |Actual: ${actualRight.map(_.alias()).mkString(", ")}""".stripMargin
    )
  }

  test("Basic case with no duplicate column names") {
    assertAliases(
      leftInput = Array("id", "name"),
      rightInput = Array("email", "phone"),
      expectedLeft = Array(
        new ColumnWithAlias("id", null),
        new ColumnWithAlias("name", null)
      ),
      expectedRight = Array(
        new ColumnWithAlias("email", null),
        new ColumnWithAlias("phone", null)
      )
    )
  }

  test("Extreme duplication scenarios") {
    assertAliases(
      leftInput = Array("id", "id", "id"),
      rightInput = Array("id", "id"),
      expectedLeft = Array(
        new ColumnWithAlias("id", null),
        new ColumnWithAlias("id", "id_1"),
        new ColumnWithAlias("id", "id_2")
      ),
      expectedRight = Array(
        new ColumnWithAlias("id", "id_3"),
        new ColumnWithAlias("id", "id_4")
      )
    )
  }

  test("Exact duplicate column names") {
    assertAliases(
      leftInput = Array("id", "name"),
      rightInput = Array("id", "name"),
      expectedLeft = Array(
        new ColumnWithAlias("id", null),
        new ColumnWithAlias("name", null)
      ),
      expectedRight = Array(
        new ColumnWithAlias("id", "id_1"),
        new ColumnWithAlias("name", "name_1")
      )
    )
  }

  test("Columns with numeric suffixes (id vs id_1)") {
    assertAliases(
      leftInput = Array("id", "id_1", "name"),
      rightInput = Array("id", "name", "value"),
      expectedLeft = Array(
        new ColumnWithAlias("id", null),
        new ColumnWithAlias("id_1", null),
        new ColumnWithAlias("name", null)
      ),
      expectedRight = Array(
        new ColumnWithAlias("id", "id_2"),
        new ColumnWithAlias("name", "name_1"),
        new ColumnWithAlias("value", null)
      )
    )
  }

  test("Case-sensitive conflicts (ID vs id)") {
    assertAliases(
      leftInput = Array("ID", "Name"),
      rightInput = Array("id", "name"),
      expectedLeft = Array(
        new ColumnWithAlias("ID", null),
        new ColumnWithAlias("Name", null)
      ),
      expectedRight = Array(
        new ColumnWithAlias("id", "id_1"),
        new ColumnWithAlias("name", "name_1")
      )
    )
  }

  test("Mixed case and numeric suffixes") {
    assertAliases(
      leftInput = Array("UserID", "user_id", "user_id_1"),
      rightInput = Array("userId", "USER_ID", "user_id_2"),
      expectedLeft = Array(
        new ColumnWithAlias("UserID", null),
        new ColumnWithAlias("user_id", null),
        new ColumnWithAlias("user_id_1", null)
      ),
      expectedRight = Array(
        new ColumnWithAlias("userId", "userId_1"),
        new ColumnWithAlias("USER_ID", "USER_ID_3"),
        new ColumnWithAlias("user_id_2", null)
      )
    )
  }
}
