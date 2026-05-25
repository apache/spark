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

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{BranchAlreadyExistsException, BranchNotFoundException, InvalidFastForwardException}
import org.apache.spark.sql.connector.catalog.{Column, Identifier, InMemoryCatalog, SupportsBranching, Table, TableCapability}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
import org.apache.spark.sql.internal.SQLConf
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

  // ------------------------------------------------------------------
  // SELECT and INSERT targeting a branch
  // ------------------------------------------------------------------

  test("SELECT FOR BRANCH reads from the branch's data") {
    withBranchingTable("t") { fq =>
      sql(s"INSERT INTO $fq VALUES (1L, 'main')")
      sql(s"ALTER TABLE $fq CREATE BRANCH dev")
      sql(s"INSERT INTO $fq FOR BRANCH 'dev' VALUES (2L, 'dev')")
      // Reads without FOR BRANCH go to the main table.
      checkAnswer(sql(s"SELECT data FROM $fq"), Seq(Row("main")))
      // Reads with FOR BRANCH go to the branch.
      checkAnswer(sql(s"SELECT data FROM $fq FOR BRANCH 'dev'"), Seq(Row("dev")))
    }
  }

  test("SELECT VERSION AS OF BRANCH is equivalent to FOR BRANCH") {
    withBranchingTable("t") { fq =>
      sql(s"ALTER TABLE $fq CREATE BRANCH dev")
      sql(s"INSERT INTO $fq FOR BRANCH 'dev' VALUES (1L, 'a'), (2L, 'b')")
      checkAnswer(
        sql(s"SELECT data FROM $fq VERSION AS OF BRANCH 'dev' ORDER BY id"),
        Seq(Row("a"), Row("b")))
    }
  }

  test("INSERT FOR BRANCH writes to the branch only") {
    withBranchingTable("t") { fq =>
      sql(s"ALTER TABLE $fq CREATE BRANCH dev")
      sql(s"INSERT INTO $fq VALUES (1L, 'main')")
      sql(s"INSERT INTO $fq FOR BRANCH 'dev' VALUES (1L, 'dev')")
      // Main table only sees main row.
      checkAnswer(sql(s"SELECT data FROM $fq"), Seq(Row("main")))
      checkAnswer(
        sql(s"SELECT data FROM $fq FOR BRANCH 'dev'"),
        Seq(Row("dev")))
    }
  }

  test("INSERT OVERWRITE FOR BRANCH overwrites branch data") {
    withBranchingTable("t") { fq =>
      sql(s"ALTER TABLE $fq CREATE BRANCH dev")
      sql(s"INSERT INTO $fq FOR BRANCH 'dev' VALUES (1L, 'a')")
      sql(s"INSERT OVERWRITE $fq FOR BRANCH 'dev' VALUES (2L, 'b')")
      checkAnswer(sql(s"SELECT data FROM $fq FOR BRANCH 'dev'"), Seq(Row("b")))
    }
  }

  test("spark.sql.defaultBranch routes reads and writes to the named branch") {
    withBranchingTable("t") { fq =>
      sql(s"ALTER TABLE $fq CREATE BRANCH dev")
      withSQLConf(SQLConf.DEFAULT_BRANCH.key -> "dev") {
        sql(s"INSERT INTO $fq VALUES (1L, 'x')")
        checkAnswer(sql(s"SELECT data FROM $fq"), Seq(Row("x")))
      }
      // Without the conf, the main table has no rows.
      checkAnswer(sql(s"SELECT data FROM $fq"), Nil)
      // The branch should hold the row that was written under the conf.
      checkAnswer(sql(s"SELECT data FROM $fq FOR BRANCH 'dev'"), Seq(Row("x")))
    }
  }

  test("explicit FOR BRANCH overrides spark.sql.defaultBranch") {
    withBranchingTable("t") { fq =>
      sql(s"ALTER TABLE $fq CREATE BRANCH dev")
      sql(s"ALTER TABLE $fq CREATE BRANCH staging")
      withSQLConf(SQLConf.DEFAULT_BRANCH.key -> "dev") {
        sql(s"INSERT INTO $fq FOR BRANCH 'staging' VALUES (1L, 'staging')")
      }
      checkAnswer(sql(s"SELECT data FROM $fq FOR BRANCH 'staging'"), Seq(Row("staging")))
      checkAnswer(sql(s"SELECT data FROM $fq FOR BRANCH 'dev'"), Nil)
    }
  }

  test("spark.sql.defaultBranch is silently ignored for non-branching tables") {
    // The session catalog's default fallback (InMemoryTableSessionCatalog) uses InMemoryTable
    // which is branching-capable, so create a temp view instead -- views aren't branching.
    withSQLConf(SQLConf.DEFAULT_BRANCH.key -> "dev") {
      sql("CREATE OR REPLACE TEMPORARY VIEW v AS SELECT 1 AS x")
      checkAnswer(sql("SELECT x FROM v"), Seq(Row(1)))
    }
  }

  test("explicit FOR BRANCH on a non-branching table fails") {
    withSQLConf(SQLConf.DEFAULT_BRANCH.key -> "") {
      sql("CREATE OR REPLACE TEMPORARY VIEW v AS SELECT 1 AS x")
      val ex = intercept[AnalysisException] {
        sql("SELECT x FROM v FOR BRANCH 'dev'")
      }
      // Temp views don't support time travel at all -- accept either error.
      val msg = ex.getMessage.toLowerCase(Locale.ROOT)
      assert(msg.contains("time travel") || msg.contains("branching"))
    }
  }
}
