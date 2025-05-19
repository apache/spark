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

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedIdentifier, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.{
  CreateFlowCommand,
  InsertIntoStatement
}
import org.apache.spark.sql.connector.catalog.TableWritePrivilege
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.command.v1.CommandSuiteBase

/**
 * The class contains tests for the `CREATE FLOW ...` command
 */
class CreateFlowCommandSuite extends CommandSuiteBase with AnalysisTest {
  protected lazy val parser = new SparkSqlParser()

  test("comment is correctly parsed") {
    Seq(
      ("CREATE FLOW f AS INSERT INTO a SELECT * FROM b", None),
      ("CREATE FLOW F COMMENT 'comment' AS INSERT INTO a SELECT * FROM b", Some("comment"))
    ).foreach { case (query, commentOpt) =>
      val plan = parser.parsePlan(query)
      val cmd = plan.asInstanceOf[CreateFlowCommand]
      assert(cmd.comment == commentOpt)
    }
  }

  test("multipart flow names are correctly parsed") {
    Seq(
      ("CREATE FLOW f AS INSERT INTO a SELECT * FROM b", Seq("f")),
      ("CREATE FLOW c.f AS INSERT INTO a SELECT * FROM b", Seq("c", "f")),
      ("CREATE FLOW c.d.f AS INSERT INTO a SELECT * FROM b", Seq("c", "d", "f"))
    ).foreach { case (query, nameParts) =>
      val plan = parser.parsePlan(query)
      val cmd = plan.asInstanceOf[CreateFlowCommand]
      assert(cmd.name.asInstanceOf[UnresolvedIdentifier].nameParts == nameParts)
    }
  }

  test("INSERT subquery is correctly parsed") {
    val plan = parser.parsePlan("CREATE FLOW f AS INSERT OVERWRITE a PARTITION (col1) BY NAME " +
      "SELECT col1, col2 FROM b")
    val cmd = plan.asInstanceOf[CreateFlowCommand]
    assert(cmd.right == InsertIntoStatement(
      table = UnresolvedRelation(Seq("a"))
        .requireWritePrivileges(Seq(TableWritePrivilege.INSERT, TableWritePrivilege.DELETE)),
      partitionSpec = Map("col1" -> None),
      userSpecifiedCols = Seq.empty,
      query = parser.parsePlan("SELECT col1, col2 FROM b"),
      overwrite = true,
      ifPartitionNotExists = false,
      byName = true
    ))
  }
}
