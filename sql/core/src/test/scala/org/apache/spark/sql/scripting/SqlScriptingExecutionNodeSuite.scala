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

  case class TestSingleStatement(testVal: String)
    extends SingleStatementExec(
      parsedPlan = Project(Seq(Alias(Literal(testVal), "condition")()), OneRowRelation()),
      Origin(startIndex = Some(0), stopIndex = Some(testVal.length)),
      isInternal = false)

  case class TestIfElse(
      conditions: Seq[TestSingleStatement],
      conditionalBodies: Seq[TestBody],
      unconditionalBody: Option[TestBody],
      booleanEvaluator: StatementBooleanEvaluator)
    extends IfElseStatementExec(
      conditions, conditionalBodies, unconditionalBody, booleanEvaluator)

  // Repeat retValue reps times, then return !retValue once.
  case class RepEval(retValue: Boolean, reps: Int) extends StatementBooleanEvaluator {
    private var callCount: Int = 0;
    override def eval(statement: LeafStatementExec): Boolean = {
      callCount += 1
      if (callCount % (reps + 1) == 0) !retValue else retValue
    }
  }

  private def extractStatementValue(statement: CompoundStatementExec): String =
    statement match {
      case TestLeafStatement(testVal) => testVal
      case TestSingleStatement(testVal) => testVal
      case _ => fail("Unexpected statement type")
    }

  // Tests
  test("test body - single statement") {
    val iter = TestNestedStatementIterator(Seq(TestLeafStatement("one"))).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("one"))
  }

  test("test body - no nesting") {
    val iter = TestNestedStatementIterator(
      Seq(
        TestLeafStatement("one"),
        TestLeafStatement("two"),
        TestLeafStatement("three")))
      .getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("one", "two", "three"))
  }

  test("test body - nesting") {
    val iter = TestNestedStatementIterator(
      Seq(
        TestNestedStatementIterator(Seq(TestLeafStatement("one"), TestLeafStatement("two"))),
        TestLeafStatement("three"),
        TestNestedStatementIterator(Seq(TestLeafStatement("four"), TestLeafStatement("five")))))
      .getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("one", "two", "three", "four", "five"))
  }

  test("if else - enter body of the IF clause") {
    val iter = TestNestedStatementIterator(Seq(
      TestIfElse(
        conditions = Seq(
          TestSingleStatement("con1")
        ),
        conditionalBodies = Seq(
          TestBody(Seq(TestLeafStatement("body1")))
        ),
        unconditionalBody = Some(TestBody(Seq(TestLeafStatement("body2")))),
        booleanEvaluator = RepEval(retValue = false, reps = 0)
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "body1"))
  }

  test("if else - enter body of the ELSE clause") {
    val iter = TestNestedStatementIterator(Seq(
      TestIfElse(
        conditions = Seq(
          TestSingleStatement("con1")
        ),
        conditionalBodies = Seq(
          TestBody(Seq(TestLeafStatement("body1")))
        ),
        unconditionalBody = Some(TestBody(Seq(TestLeafStatement("body2")))),
        booleanEvaluator = RepEval(retValue = false, reps = 1)
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "body2"))
  }

  test("if else if - enter body of the IF clause") {
    val iter = TestNestedStatementIterator(Seq(
      TestIfElse(
        conditions = Seq(
          TestSingleStatement("con1"),
          TestSingleStatement("con2")
        ),
        conditionalBodies = Seq(
          TestBody(Seq(TestLeafStatement("body1"))),
          TestBody(Seq(TestLeafStatement("body2")))
        ),
        unconditionalBody = Some(TestBody(Seq(TestLeafStatement("body3")))),
        booleanEvaluator = RepEval(retValue = false, reps = 0)
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "body1"))
  }

  test("if else if - enter body of the ELSE IF clause") {
    val iter = TestNestedStatementIterator(Seq(
      TestIfElse(
        conditions = Seq(
          TestSingleStatement("con1"),
          TestSingleStatement("con2")
        ),
        conditionalBodies = Seq(
          TestBody(Seq(TestLeafStatement("body1"))),
          TestBody(Seq(TestLeafStatement("body2")))
        ),
        unconditionalBody = Some(TestBody(Seq(TestLeafStatement("body3")))),
        booleanEvaluator = RepEval(retValue = false, reps = 1)
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2", "body2"))
  }

  test("if else if - enter body of the second ELSE IF clause") {
    val iter = TestNestedStatementIterator(Seq(
      TestIfElse(
        conditions = Seq(
          TestSingleStatement("con1"),
          TestSingleStatement("con2"),
          TestSingleStatement("con3")
        ),
        conditionalBodies = Seq(
          TestBody(Seq(TestLeafStatement("body1"))),
          TestBody(Seq(TestLeafStatement("body2"))),
          TestBody(Seq(TestLeafStatement("body3")))
        ),
        unconditionalBody = Some(TestBody(Seq(TestLeafStatement("body4")))),
        booleanEvaluator = RepEval(retValue = false, reps = 2)
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2", "con3", "body3"))
  }

  test("if else if - enter body of the ELSE clause") {
    val iter = TestNestedStatementIterator(Seq(
      TestIfElse(
        conditions = Seq(
          TestSingleStatement("con1"),
          TestSingleStatement("con2")
        ),
        conditionalBodies = Seq(
          TestBody(Seq(TestLeafStatement("body1"))),
          TestBody(Seq(TestLeafStatement("body2")))
        ),
        unconditionalBody = Some(TestBody(Seq(TestLeafStatement("body3")))),
        booleanEvaluator = RepEval(retValue = false, reps = 2)
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2", "body3"))
  }

  test("if else if - without else (successful check)") {
    val iter = TestNestedStatementIterator(Seq(
      TestIfElse(
        conditions = Seq(
          TestSingleStatement("con1"),
          TestSingleStatement("con2")
        ),
        conditionalBodies = Seq(
          TestBody(Seq(TestLeafStatement("body1"))),
          TestBody(Seq(TestLeafStatement("body2")))
        ),
        unconditionalBody = None,
        booleanEvaluator = RepEval(retValue = false, reps = 1)
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2", "body2"))
  }

  test("if else if - without else (unsuccessful checks)") {
    val iter = TestNestedStatementIterator(Seq(
      TestIfElse(
        conditions = Seq(
          TestSingleStatement("con1"),
          TestSingleStatement("con2")
        ),
        conditionalBodies = Seq(
          TestBody(Seq(TestLeafStatement("body1"))),
          TestBody(Seq(TestLeafStatement("body2")))
        ),
        unconditionalBody = None,
        booleanEvaluator = RepEval(retValue = false, reps = 2)
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2"))
  }
}
