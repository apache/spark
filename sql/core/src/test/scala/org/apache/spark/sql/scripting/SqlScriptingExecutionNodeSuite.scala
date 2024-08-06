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
  }

  case class TestIfElseCondition(condVal: Boolean, description: String)
    extends SingleStatementExec(
      parsedPlan = Project(Seq(Alias(Literal(condVal), description)()), OneRowRelation()),
      Origin(startIndex = Some(0), stopIndex = Some(description.length)),
      isInternal = false)

  case class TestWhileCondition(description: String)
    extends SingleStatementExec(
      parsedPlan = Project(Seq(Alias(Literal(true), description)()), OneRowRelation()),
      Origin(startIndex = Some(0), stopIndex = Some(description.length)),
      isInternal = false)

  case class TestWhile(
      condition: SingleStatementExec,
      body: CompoundBodyExec,
      reps: Int)
    extends WhileStatementExec(condition, body, spark) {

    private var callCount: Int = 0

    override def evaluateBooleanCondition(
      session: SparkSession,
      statement: LeafStatementExec): Boolean = {
        if (callCount < reps) {
          callCount += 1
          true
        } else {
          callCount = 0
          false
        }
    }
  }

  private def extractStatementValue(statement: CompoundStatementExec): String =
    statement match {
      case TestLeafStatement(testVal) => testVal
      case TestIfElseCondition(_, description) => description
      case TestWhileCondition(description) => description
      case _ => fail("Unexpected statement type")
    }

  // Tests
  test("test body - single statement") {
    val iter = new CompoundBodyExec(Seq(TestLeafStatement("one"))).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("one"))
  }

  test("test body - no nesting") {
    val iter = new CompoundBodyExec(
      Seq(
        TestLeafStatement("one"),
        TestLeafStatement("two"),
        TestLeafStatement("three")))
      .getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("one", "two", "three"))
  }

  test("test body - nesting") {
    val iter = new CompoundBodyExec(
      Seq(
        new CompoundBodyExec(Seq(TestLeafStatement("one"), TestLeafStatement("two"))),
        TestLeafStatement("three"),
        new CompoundBodyExec(Seq(TestLeafStatement("four"), TestLeafStatement("five")))))
      .getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("one", "two", "three", "four", "five"))
  }

  test("if else - enter body of the IF clause") {
    val iter = new CompoundBodyExec(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = true, description = "con1")
        ),
        conditionalBodies = Seq(
          new CompoundBodyExec(Seq(TestLeafStatement("body1")))
        ),
        elseBody = Some(new CompoundBodyExec(Seq(TestLeafStatement("body2")))),
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "body1"))
  }

  test("if else - enter body of the ELSE clause") {
    val iter = new CompoundBodyExec(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1")
        ),
        conditionalBodies = Seq(
          new CompoundBodyExec(Seq(TestLeafStatement("body1")))
        ),
        elseBody = Some(new CompoundBodyExec(Seq(TestLeafStatement("body2")))),
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "body2"))
  }

  test("if else if - enter body of the IF clause") {
    val iter = new CompoundBodyExec(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = true, description = "con1"),
          TestIfElseCondition(condVal = false, description = "con2")
        ),
        conditionalBodies = Seq(
          new CompoundBodyExec(Seq(TestLeafStatement("body1"))),
          new CompoundBodyExec(Seq(TestLeafStatement("body2")))
        ),
        elseBody = Some(new CompoundBodyExec(Seq(TestLeafStatement("body3")))),
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "body1"))
  }

  test("if else if - enter body of the ELSE IF clause") {
    val iter = new CompoundBodyExec(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1"),
          TestIfElseCondition(condVal = true, description = "con2")
        ),
        conditionalBodies = Seq(
          new CompoundBodyExec(Seq(TestLeafStatement("body1"))),
          new CompoundBodyExec(Seq(TestLeafStatement("body2")))
        ),
        elseBody = Some(new CompoundBodyExec(Seq(TestLeafStatement("body3")))),
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2", "body2"))
  }

  test("if else if - enter body of the second ELSE IF clause") {
    val iter = new CompoundBodyExec(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1"),
          TestIfElseCondition(condVal = false, description = "con2"),
          TestIfElseCondition(condVal = true, description = "con3")
        ),
        conditionalBodies = Seq(
          new CompoundBodyExec(Seq(TestLeafStatement("body1"))),
          new CompoundBodyExec(Seq(TestLeafStatement("body2"))),
          new CompoundBodyExec(Seq(TestLeafStatement("body3")))
        ),
        elseBody = Some(new CompoundBodyExec(Seq(TestLeafStatement("body4")))),
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2", "con3", "body3"))
  }

  test("if else if - enter body of the ELSE clause") {
    val iter = new CompoundBodyExec(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1"),
          TestIfElseCondition(condVal = false, description = "con2")
        ),
        conditionalBodies = Seq(
          new CompoundBodyExec(Seq(TestLeafStatement("body1"))),
          new CompoundBodyExec(Seq(TestLeafStatement("body2")))
        ),
        elseBody = Some(new CompoundBodyExec(Seq(TestLeafStatement("body3")))),
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2", "body3"))
  }

  test("if else if - without else (successful check)") {
    val iter = new CompoundBodyExec(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1"),
          TestIfElseCondition(condVal = true, description = "con2")
        ),
        conditionalBodies = Seq(
          new CompoundBodyExec(Seq(TestLeafStatement("body1"))),
          new CompoundBodyExec(Seq(TestLeafStatement("body2")))
        ),
        elseBody = None,
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2", "body2"))
  }

  test("if else if - without else (unsuccessful checks)") {
    val iter = new CompoundBodyExec(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1"),
          TestIfElseCondition(condVal = false, description = "con2")
        ),
        conditionalBodies = Seq(
          new CompoundBodyExec(Seq(TestLeafStatement("body1"))),
          new CompoundBodyExec(Seq(TestLeafStatement("body2")))
        ),
        elseBody = None,
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2"))
  }

  test("while - doesn't enter body") {
    val iter = new CompoundBodyExec(Seq(
      TestWhile(
        condition = TestWhileCondition("con1"),
        body = new CompoundBodyExec(Seq(TestLeafStatement("body1"))),
        reps = 0
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1"))
  }

  test("while - enters body once") {
    val iter = new CompoundBodyExec(Seq(
      TestWhile(
        condition = TestWhileCondition("con1"),
        body = new CompoundBodyExec(Seq(TestLeafStatement("body1"))),
        reps = 1
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "body1", "con1"))
  }

  test("while - enters body with multiple statements multiple times") {
    val iter = new CompoundBodyExec(Seq(
      TestWhile(
        condition = TestWhileCondition("con1"),
        body = new CompoundBodyExec(Seq(
          TestLeafStatement("statement1"),
          TestLeafStatement("statement2"))),
        reps = 2
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "statement1", "statement2",
                              "con1", "statement1", "statement2", "con1"))
  }

  test("nested while - 2 times outer 2 times inner") {
    val iter = new CompoundBodyExec(Seq(
      TestWhile(
        condition = TestWhileCondition("con1"),
        body = new CompoundBodyExec(Seq(
          TestWhile(
            condition = TestWhileCondition("con2"),
            body = new CompoundBodyExec(Seq(TestLeafStatement("body1"))),
            reps = 2
          )
        )),
        reps = 2
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2", "body1",
                                      "con2", "body1", "con2",
                              "con1", "con2", "body1",
                                      "con2", "body1", "con2", "con1"))
  }

}
