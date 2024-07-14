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

package org.apache.spark.sql.scripting

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Project}
import org.apache.spark.sql.catalyst.trees.Origin

/**
 * Unit tests for execution nodes from SqlScriptingExecutionNode.scala.
 * Execution nodes are constructed manually and iterated through.
 * It is then checked if the leaf statements have been iterated in the expected order.
 */
class SqlScriptingExecutionNodeSuite extends SparkFunSuite {
  // Helpers
  case class TestLeafStatement(testVal: String) extends LeafStatementExec {
    override def reset(): Unit = ()
  }

  case class TestNestedStatementIterator(statements: Seq[CompoundStatementExec])
    extends CompoundNestedStatementIteratorExec(statements)

  case class TestBody(statements: Seq[CompoundStatementExec]) extends CompoundBodyExec(statements)

  case class TestSparkStatementWithPlan(testVal: String)
    extends SingleStatementExec(
      parsedPlan = Project(Seq(Alias(Literal(testVal), "condition")()), OneRowRelation()),
      Origin(startIndex = Some(0), stopIndex = Some(testVal.length)),
      isInternal = false)

  // Tests
  test("test body - single statement") {
    val iter = TestNestedStatementIterator(Seq(TestLeafStatement("one"))).getTreeIterator
    val statements = iter.map {
      case TestLeafStatement(v) => v
      case _ => fail("Unexpected statement type")
    }.toList

    assert(statements === List("one"))
  }

  test("test body - no nesting") {
    val iter = TestNestedStatementIterator(
      Seq(
        TestLeafStatement("one"),
        TestLeafStatement("two"),
        TestLeafStatement("three")))
      .getTreeIterator
    val statements = iter.map {
      case TestLeafStatement(v) => v
      case _ => fail("Unexpected statement type")
    }.toList

    assert(statements === Seq("one", "two", "three"))
  }

  test("test body - nesting") {
    val iter = TestNestedStatementIterator(
      Seq(
        TestNestedStatementIterator(Seq(TestLeafStatement("one"), TestLeafStatement("two"))),
        TestLeafStatement("three"),
        TestNestedStatementIterator(Seq(TestLeafStatement("four"), TestLeafStatement("five")))))
      .getTreeIterator
    val statements = iter.map {
      case TestLeafStatement(v) => v
      case _ => fail("Unexpected statement type")
    }.toList

    assert(statements === Seq("one", "two", "three", "four", "five"))
  }
}
