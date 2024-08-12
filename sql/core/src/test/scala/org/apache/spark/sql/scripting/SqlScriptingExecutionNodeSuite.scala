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

import scala.collection.mutable

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Project}
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Unit tests for execution nodes from SqlScriptingExecutionNode.scala.
 * Execution nodes are constructed manually and iterated through.
 * It is then checked if the leaf statements have been iterated in the expected order.
 */
class SqlScriptingExecutionNodeSuite extends SparkFunSuite with SharedSparkSession {
  // Helpers
  case class TestLeafStatement(testVal: String) extends LeafStatementExec {
    override def reset(): Unit = ()

    override def execute(session: SparkSession): Unit = ()
  }

  case class TestBody(statements: Seq[CompoundStatementExec])
    extends CompoundBodyExec(statements, null, None, mutable.HashMap())

  case class TestSparkStatementWithPlan(testVal: String)
  case class TestIfElseCondition(condVal: Boolean, description: String)
    extends SingleStatementExec(
      parsedPlan = Project(Seq(Alias(Literal(condVal), description)()), OneRowRelation()),
      Origin(startIndex = Some(0), stopIndex = Some(description.length)),
      isInternal = false)

  private def extractStatementValue(statement: CompoundStatementExec): String =
    statement match {
      case TestLeafStatement(testVal) => testVal
      case TestIfElseCondition(_, description) => description
      case _ => fail("Unexpected statement type")
    }

  // Tests
  test("test body - single statement") {
    val iter = TestBody(Seq(TestLeafStatement("one"))).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("one"))
  }

  test("test body - no nesting") {
    val iter = TestBody(
      Seq(
        TestLeafStatement("one"),
        TestLeafStatement("two"),
        TestLeafStatement("three")))
      .getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("one", "two", "three"))
  }

  test("test body - nesting") {
    val iter = TestBody(
      Seq(
        TestBody(Seq(TestLeafStatement("one"), TestLeafStatement("two"))),
        TestLeafStatement("three"),
        TestBody(Seq(TestLeafStatement("four"), TestLeafStatement("five")))))
      .getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("one", "two", "three", "four", "five"))
  }

  test("if else - enter body of the IF clause") {
    val iter = TestBody(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = true, description = "con1")
        ),
        conditionalBodies = Seq(
          TestBody(Seq(TestLeafStatement("body1")))
        ),
        elseBody = Some(TestBody(Seq(TestLeafStatement("body2")))),
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "body1"))
  }

  test("if else - enter body of the ELSE clause") {
    val iter = TestBody(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1")
        ),
        conditionalBodies = Seq(
          TestBody(Seq(TestLeafStatement("body1")))
        ),
        elseBody = Some(TestBody(Seq(TestLeafStatement("body2")))),
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "body2"))
  }

  test("if else if - enter body of the IF clause") {
    val iter = TestBody(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = true, description = "con1"),
          TestIfElseCondition(condVal = false, description = "con2")
        ),
        conditionalBodies = Seq(
          TestBody(Seq(TestLeafStatement("body1"))),
          TestBody(Seq(TestLeafStatement("body2")))
        ),
        elseBody = Some(TestBody(Seq(TestLeafStatement("body3")))),
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "body1"))
  }

  test("if else if - enter body of the ELSE IF clause") {
    val iter = TestBody(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1"),
          TestIfElseCondition(condVal = true, description = "con2")
        ),
        conditionalBodies = Seq(
          TestBody(Seq(TestLeafStatement("body1"))),
          TestBody(Seq(TestLeafStatement("body2")))
        ),
        elseBody = Some(TestBody(Seq(TestLeafStatement("body3")))),
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2", "body2"))
  }

  test("if else if - enter body of the second ELSE IF clause") {
    val iter = TestBody(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1"),
          TestIfElseCondition(condVal = false, description = "con2"),
          TestIfElseCondition(condVal = true, description = "con3")
        ),
        conditionalBodies = Seq(
          TestBody(Seq(TestLeafStatement("body1"))),
          TestBody(Seq(TestLeafStatement("body2"))),
          TestBody(Seq(TestLeafStatement("body3")))
        ),
        elseBody = Some(TestBody(Seq(TestLeafStatement("body4")))),
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2", "con3", "body3"))
  }

  test("if else if - enter body of the ELSE clause") {
    val iter = TestBody(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1"),
          TestIfElseCondition(condVal = false, description = "con2")
        ),
        conditionalBodies = Seq(
          TestBody(Seq(TestLeafStatement("body1"))),
          TestBody(Seq(TestLeafStatement("body2")))
        ),
        elseBody = Some(TestBody(Seq(TestLeafStatement("body3")))),
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2", "body3"))
  }

  test("if else if - without else (successful check)") {
    val iter = TestBody(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1"),
          TestIfElseCondition(condVal = true, description = "con2")
        ),
        conditionalBodies = Seq(
          TestBody(Seq(TestLeafStatement("body1"))),
          TestBody(Seq(TestLeafStatement("body2")))
        ),
        elseBody = None,
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2", "body2"))
  }

  test("if else if - without else (unsuccessful checks)") {
    val iter = TestBody(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1"),
          TestIfElseCondition(condVal = false, description = "con2")
        ),
        conditionalBodies = Seq(
          TestBody(Seq(TestLeafStatement("body1"))),
          TestBody(Seq(TestLeafStatement("body2")))
        ),
        elseBody = None,
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2"))
  }
}
