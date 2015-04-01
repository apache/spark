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

package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Command
import org.scalatest.FunSuite

private[sql] case class TestCommand(cmd: String) extends Command

private[sql] class SuperLongKeywordTestParser extends AbstractSparkSQLParser {
  protected val EXECUTE   = Keyword("THISISASUPERLONGKEYWORDTEST")

  override protected lazy val start: Parser[LogicalPlan] = set

  private lazy val set: Parser[LogicalPlan] =
    EXECUTE ~> ident ^^ {
      case fileName => TestCommand(fileName)
    }
}

private[sql] class CaseInsensitiveTestParser extends AbstractSparkSQLParser {
  protected val EXECUTE   = Keyword("EXECUTE")

  override protected lazy val start: Parser[LogicalPlan] = set

  private lazy val set: Parser[LogicalPlan] =
    EXECUTE ~> ident ^^ {
      case fileName => TestCommand(fileName)
    }
}

class SqlParserSuite extends FunSuite {

  test("test long keyword") {
    val parser = new SuperLongKeywordTestParser
    assert(TestCommand("NotRealCommand") === parser("ThisIsASuperLongKeyWordTest NotRealCommand"))
  }

  test("test case insensitive") {
    val parser = new CaseInsensitiveTestParser
    assert(TestCommand("NotRealCommand") === parser("EXECUTE NotRealCommand"))
    assert(TestCommand("NotRealCommand") === parser("execute NotRealCommand"))
    assert(TestCommand("NotRealCommand") === parser("exEcute NotRealCommand"))
  }
}
