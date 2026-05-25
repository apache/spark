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

import java.util
import java.util.Locale

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.analysis.{BranchAlreadyExistsException, BranchNotFoundException, InvalidFastForwardException}
import org.apache.spark.sql.connector.catalog.{Column, Identifier, InMemoryCatalog, SupportsBranching, Table, TableCapability}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
import org.apache.spark.sql.types.StructType

class SupportsBranchingSuite extends QueryTest with DatasourceV2SQLBase {

  private def branchingTable(tableName: String): SupportsBranching = {
    val cat = catalog("testcat").asInstanceOf[InMemoryCatalog]
    cat.loadTable(Identifier.of(Array("ns"), tableName)).asInstanceOf[SupportsBranching]
  }

  private def withBranchingTable(tableName: String)(f: String => Unit): Unit = {
    val fqName = s"testcat.ns.$tableName"
    sql(s"CREATE TABLE $fqName (id bigint, data string) USING foo")
    try {
      f(fqName)
    } finally {
      sql(s"DROP TABLE IF EXISTS $fqName")
    }
  }

  test("CREATE BRANCH on an InMemoryTable creates a branch") {
    withBranchingTable("t") { fq =>
      sql(s"ALTER TABLE $fq CREATE BRANCH dev")
      val t = branchingTable("t")
      val branches = t.listBranches()
      assert(branches.length == 1)
      assert(branches(0).name == "dev")
    }
  }

  test("CREATE BRANCH with explicit snapshot id") {
    withBranchingTable("t") { fq =>
      sql(s"ALTER TABLE $fq CREATE BRANCH staging VERSION AS OF 7")
      val branch = branchingTable("t").listBranches().head
      assert(branch.name == "staging")
      assert(branch.snapshotId.isPresent && branch.snapshotId.getAsLong == 7L)
    }
  }

  test("CREATE BRANCH IF NOT EXISTS is idempotent") {
    withBranchingTable("t") { fq =>
      sql(s"ALTER TABLE $fq CREATE BRANCH dev")
      sql(s"ALTER TABLE $fq CREATE BRANCH IF NOT EXISTS dev")
      assert(branchingTable("t").listBranches().length == 1)
    }
  }

  test("CREATE BRANCH without IF NOT EXISTS fails on duplicate") {
    withBranchingTable("t") { fq =>
      sql(s"ALTER TABLE $fq CREATE BRANCH dev")
      val ex = intercept[BranchAlreadyExistsException] {
        sql(s"ALTER TABLE $fq CREATE BRANCH dev")
      }
      assert(ex.getMessage.contains("dev"))
      assert(ex.getCondition == "BRANCH_ALREADY_EXISTS")
    }
  }

  test("CREATE OR REPLACE BRANCH overwrites an existing branch") {
    withBranchingTable("t") { fq =>
      sql(s"ALTER TABLE $fq CREATE BRANCH dev VERSION AS OF 1")
      sql(s"ALTER TABLE $fq CREATE OR REPLACE BRANCH dev VERSION AS OF 9")
      val branch = branchingTable("t").listBranches().head
      assert(branch.snapshotId.getAsLong == 9L)
    }
  }

  test("DROP BRANCH removes a branch") {
    withBranchingTable("t") { fq =>
      sql(s"ALTER TABLE $fq CREATE BRANCH dev")
      sql(s"ALTER TABLE $fq DROP BRANCH dev")
      assert(branchingTable("t").listBranches().isEmpty)
    }
  }

  test("DROP BRANCH IF EXISTS does not fail on missing branch") {
    withBranchingTable("t") { fq =>
      sql(s"ALTER TABLE $fq DROP BRANCH IF EXISTS missing")
    }
  }

  test("DROP BRANCH fails on missing branch when IF EXISTS not specified") {
    withBranchingTable("t") { fq =>
      val ex = intercept[BranchNotFoundException] {
        sql(s"ALTER TABLE $fq DROP BRANCH missing")
      }
      assert(ex.getMessage.contains("missing"))
      assert(ex.getCondition == "BRANCH_NOT_FOUND")
    }
  }

  test("FASTFORWARD BRANCH updates branch to target's snapshot") {
    withBranchingTable("t") { fq =>
      sql(s"ALTER TABLE $fq CREATE BRANCH dev VERSION AS OF 1")
      sql(s"ALTER TABLE $fq CREATE BRANCH main VERSION AS OF 5")
      sql(s"ALTER TABLE $fq FASTFORWARD BRANCH dev TO main")
      val dev = branchingTable("t").listBranches().find(_.name == "dev").get
      assert(dev.snapshotId.getAsLong == 5L)
    }
  }

  test("FASTFORWARD BRANCH fails when target is behind") {
    withBranchingTable("t") { fq =>
      sql(s"ALTER TABLE $fq CREATE BRANCH dev VERSION AS OF 5")
      sql(s"ALTER TABLE $fq CREATE BRANCH main VERSION AS OF 1")
      val ex = intercept[InvalidFastForwardException] {
        sql(s"ALTER TABLE $fq FASTFORWARD BRANCH dev TO main")
      }
      assert(ex.getMessage.contains("behind"))
      assert(ex.getCondition == "INVALID_FASTFORWARD")
    }
  }

  test("FASTFORWARD BRANCH fails when the source branch is missing") {
    withBranchingTable("t") { fq =>
      sql(s"ALTER TABLE $fq CREATE BRANCH main VERSION AS OF 1")
      val ex = intercept[BranchNotFoundException] {
        sql(s"ALTER TABLE $fq FASTFORWARD BRANCH missing TO main")
      }
      assert(ex.getMessage.contains("missing"))
      assert(ex.getCondition == "BRANCH_NOT_FOUND")
    }
  }

  test("FASTFORWARD BRANCH fails when the target branch is missing") {
    withBranchingTable("t") { fq =>
      sql(s"ALTER TABLE $fq CREATE BRANCH dev VERSION AS OF 1")
      val ex = intercept[BranchNotFoundException] {
        sql(s"ALTER TABLE $fq FASTFORWARD BRANCH dev TO missing")
      }
      assert(ex.getMessage.contains("missing"))
      assert(ex.getCondition == "BRANCH_NOT_FOUND")
    }
  }

  test("SHOW BRANCHES lists branches with snapshot ids") {
    withBranchingTable("t") { fq =>
      sql(s"ALTER TABLE $fq CREATE BRANCH dev VERSION AS OF 1")
      sql(s"ALTER TABLE $fq CREATE BRANCH main VERSION AS OF 2")
      val rows = sql(s"SHOW BRANCHES IN $fq").collect()
      val byName = rows.map(r => r.getString(0) -> r.getLong(1)).toMap
      assert(byName == Map("dev" -> 1L, "main" -> 2L))
    }
  }

  test("SHOW BRANCHES surfaces NULL snapshot_id when the branch has no snapshot") {
    withBranchingTable("t") { fq =>
      // A branch created on a table with no commits has no snapshot to point at.
      sql(s"ALTER TABLE $fq CREATE BRANCH empty")
      val rows = sql(s"SHOW BRANCHES IN $fq").collect()
      assert(rows.length == 1)
      assert(rows(0).getString(0) == "empty")
      assert(rows(0).isNullAt(1))
    }
  }

  test("asBranchable rejects tables that do not implement SupportsBranching") {
    val plainTable = new Table {
      override def name(): String = "plain"
      override def columns(): Array[Column] = Array.empty
      override def schema(): StructType = new StructType()
      override def capabilities(): util.Set[TableCapability] = util.Set.of()
    }
    val ex = intercept[AnalysisException] {
      plainTable.asBranchable
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("branching"))
  }
}
