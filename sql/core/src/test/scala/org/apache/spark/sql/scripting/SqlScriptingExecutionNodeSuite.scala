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
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{DropVariable, LeafNode, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * Unit tests for execution nodes from SqlScriptingExecutionNode.scala.
 * Execution nodes are constructed manually and iterated through.
 * It is then checked if the leaf statements have been iterated in the expected order.
 */
class SqlScriptingExecutionNodeSuite extends SparkFunSuite with SharedSparkSession {
  // Helpers
  case class TestCompoundBody(
      statements: Seq[CompoundStatementExec],
      label: Option[String] = None,
      isScope: Boolean = false,
      context: SqlScriptingExecutionContext = null)
    extends CompoundBodyExec(statements, label, isScope, context) {

    // No-op to remove unnecessary logic for these tests.
    override def enterScope(): Unit = ()

    // No-op to remove unnecessary logic for these tests.
    override def exitScope(): Unit = ()
  }

  case class TestForStatement(
      query: SingleStatementExec,
      variableName: Option[String],
      body: CompoundBodyExec,
      override val label: Option[String],
      session: SparkSession,
      context: SqlScriptingExecutionContext = null)
    extends ForStatementExec(
      query,
      variableName,
      body,
      label,
      session,
      context)

  case class TestLeafStatement(testVal: String) extends LeafStatementExec {
    override def reset(): Unit = ()
  }

  case class TestIfElseCondition(condVal: Boolean, description: String)
    extends SingleStatementExec(
      parsedPlan = Project(Seq(Alias(Literal(condVal), description)()), OneRowRelation()),
      Origin(startIndex = Some(0), stopIndex = Some(description.length)),
      Map.empty,
      isInternal = false,
      null
    )

  case class DummyLogicalPlan() extends LeafNode {
    override def output: Seq[Attribute] = Seq.empty
  }

  case class TestLoopCondition(
      condVal: Boolean, reps: Int, description: String)
    extends SingleStatementExec(
      parsedPlan = DummyLogicalPlan(),
      Origin(startIndex = Some(0), stopIndex = Some(description.length)),
      Map.empty,
      isInternal = false,
      null
    )

  class LoopBooleanConditionEvaluator(condition: TestLoopCondition) {
    private var callCount: Int = 0

    def evaluateLoopBooleanCondition(): Boolean = {
      if (callCount < condition.reps) {
        callCount += 1
        condition.condVal
      } else {
        callCount = 0
        !condition.condVal
      }
    }
  }

  case class TestWhile(
      condition: TestLoopCondition,
      body: TestCompoundBody,
      label: Option[String] = None)
    extends WhileStatementExec(condition, body, label, spark) {

    private val evaluator = new LoopBooleanConditionEvaluator(condition)

    override def evaluateBooleanCondition(
        session: SparkSession,
        statement: LeafStatementExec): Boolean = evaluator.evaluateLoopBooleanCondition()
  }

  case class TestRepeat(
      condition: TestLoopCondition,
      body: TestCompoundBody,
      label: Option[String] = None)
    extends RepeatStatementExec(condition, body, label, spark) {

    private val evaluator = new LoopBooleanConditionEvaluator(condition)

    override def evaluateBooleanCondition(
      session: SparkSession,
      statement: LeafStatementExec): Boolean = evaluator.evaluateLoopBooleanCondition()
  }

  case class MockQuery(numberOfRows: Int, columnName: String, description: String)
      extends SingleStatementExec(
        DummyLogicalPlan(),
        Origin(startIndex = Some(0), stopIndex = Some(description.length)),
        Map.empty,
        isInternal = false,
        null) {
    override def buildDataFrame(session: SparkSession): DataFrame = {
      val data = Seq.range(0, numberOfRows).map(Row(_))
      val schema = List(StructField(columnName, IntegerType))

      spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        StructType(schema)
      )
    }
  }

  private def extractStatementValue(statement: CompoundStatementExec): String =
    statement match {
      case TestLeafStatement(testVal) => testVal
      case TestIfElseCondition(_, description) => description
      case TestLoopCondition(_, _, description) => description
      case loopStmt: LoopStatementExec => loopStmt.label.get
      case leaveStmt: LeaveStatementExec => leaveStmt.label
      case iterateStmt: IterateStatementExec => iterateStmt.label
      case forStmt: TestForStatement => forStmt.label.get
      case dropStmt: SingleStatementExec if dropStmt.parsedPlan.isInstanceOf[DropVariable]
        => "DropVariable"
      case _ => fail("Unexpected statement type")
    }

  // Tests
  test("test body - single statement") {
    val iter = TestCompoundBody(Seq(TestLeafStatement("one"))).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("one"))
  }

  test("test body - no nesting") {
    val iter = TestCompoundBody(
      Seq(
        TestLeafStatement("one"),
        TestLeafStatement("two"),
        TestLeafStatement("three")))
      .getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("one", "two", "three"))
  }

  test("test body - nesting") {
    val iter = TestCompoundBody(
      Seq(
        TestCompoundBody(Seq(TestLeafStatement("one"), TestLeafStatement("two"))),
        TestLeafStatement("three"),
        TestCompoundBody(Seq(TestLeafStatement("four"), TestLeafStatement("five")))))
      .getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("one", "two", "three", "four", "five"))
  }

  test("if else - enter body of the IF clause") {
    val iter = TestCompoundBody(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = true, description = "con1")
        ),
        conditionalBodies = Seq(
          TestCompoundBody(Seq(TestLeafStatement("body1")))
        ),
        elseBody = Some(TestCompoundBody(Seq(TestLeafStatement("body2")))),
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "body1"))
  }

  test("if else - enter body of the ELSE clause") {
    val iter = TestCompoundBody(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1")
        ),
        conditionalBodies = Seq(
          TestCompoundBody(Seq(TestLeafStatement("body1")))
        ),
        elseBody = Some(TestCompoundBody(Seq(TestLeafStatement("body2")))),
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "body2"))
  }

  test("if else if - enter body of the IF clause") {
    val iter = TestCompoundBody(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = true, description = "con1"),
          TestIfElseCondition(condVal = false, description = "con2")
        ),
        conditionalBodies = Seq(
          TestCompoundBody(Seq(TestLeafStatement("body1"))),
          TestCompoundBody(Seq(TestLeafStatement("body2")))
        ),
        elseBody = Some(TestCompoundBody(Seq(TestLeafStatement("body3")))),
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "body1"))
  }

  test("if else if - enter body of the ELSE IF clause") {
    val iter = TestCompoundBody(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1"),
          TestIfElseCondition(condVal = true, description = "con2")
        ),
        conditionalBodies = Seq(
          TestCompoundBody(Seq(TestLeafStatement("body1"))),
          TestCompoundBody(Seq(TestLeafStatement("body2")))
        ),
        elseBody = Some(TestCompoundBody(Seq(TestLeafStatement("body3")))),
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2", "body2"))
  }

  test("if else if - enter body of the second ELSE IF clause") {
    val iter = TestCompoundBody(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1"),
          TestIfElseCondition(condVal = false, description = "con2"),
          TestIfElseCondition(condVal = true, description = "con3")
        ),
        conditionalBodies = Seq(
          TestCompoundBody(Seq(TestLeafStatement("body1"))),
          TestCompoundBody(Seq(TestLeafStatement("body2"))),
          TestCompoundBody(Seq(TestLeafStatement("body3")))
        ),
        elseBody = Some(TestCompoundBody(Seq(TestLeafStatement("body4")))),
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2", "con3", "body3"))
  }

  test("if else if - enter body of the ELSE clause") {
    val iter = TestCompoundBody(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1"),
          TestIfElseCondition(condVal = false, description = "con2")
        ),
        conditionalBodies = Seq(
          TestCompoundBody(Seq(TestLeafStatement("body1"))),
          TestCompoundBody(Seq(TestLeafStatement("body2")))
        ),
        elseBody = Some(TestCompoundBody(Seq(TestLeafStatement("body3")))),
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2", "body3"))
  }

  test("if else if - without else (successful check)") {
    val iter = TestCompoundBody(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1"),
          TestIfElseCondition(condVal = true, description = "con2")
        ),
        conditionalBodies = Seq(
          TestCompoundBody(Seq(TestLeafStatement("body1"))),
          TestCompoundBody(Seq(TestLeafStatement("body2")))
        ),
        elseBody = None,
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2", "body2"))
  }

  test("if else if - without else (unsuccessful checks)") {
    val iter = TestCompoundBody(Seq(
      new IfElseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1"),
          TestIfElseCondition(condVal = false, description = "con2")
        ),
        conditionalBodies = Seq(
          TestCompoundBody(Seq(TestLeafStatement("body1"))),
          TestCompoundBody(Seq(TestLeafStatement("body2")))
        ),
        elseBody = None,
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2"))
  }

  test("while - doesn't enter body") {
    val iter = TestCompoundBody(Seq(
      TestWhile(
        condition = TestLoopCondition(condVal = true, reps = 0, description = "con1"),
        body = TestCompoundBody(Seq(TestLeafStatement("body1")))
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1"))
  }

  test("while - enters body once") {
    val iter = TestCompoundBody(Seq(
      TestWhile(
        condition = TestLoopCondition(condVal = true, reps = 1, description = "con1"),
        body = TestCompoundBody(Seq(TestLeafStatement("body1")))
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "body1", "con1"))
  }

  test("while - enters body with multiple statements multiple times") {
    val iter = TestCompoundBody(Seq(
      TestWhile(
        condition = TestLoopCondition(condVal = true, reps = 2, description = "con1"),
        body = TestCompoundBody(Seq(
          TestLeafStatement("statement1"),
          TestLeafStatement("statement2")))
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "statement1", "statement2",
                              "con1", "statement1", "statement2", "con1"))
  }

  test("nested while - 2 times outer 2 times inner") {
    val iter = TestCompoundBody(Seq(
      TestWhile(
        condition = TestLoopCondition(condVal = true, reps = 2, description = "con1"),
        body = TestCompoundBody(Seq(
          TestWhile(
            condition = TestLoopCondition(condVal = true, reps = 2, description = "con2"),
            body = TestCompoundBody(Seq(TestLeafStatement("body1")))
          ))
        )
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2", "body1",
                                      "con2", "body1", "con2",
                              "con1", "con2", "body1",
                                      "con2", "body1", "con2", "con1"))
  }

  test("repeat - true condition") {
    val iter = TestCompoundBody(Seq(
      TestRepeat(
        condition = TestLoopCondition(condVal = false, reps = 0, description = "con1"),
        body = TestCompoundBody(Seq(TestLeafStatement("body1")))
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("body1", "con1"))
  }

  test("repeat - condition false once") {
    val iter = TestCompoundBody(Seq(
      TestRepeat(
        condition = TestLoopCondition(condVal = false, reps = 1, description = "con1"),
        body = TestCompoundBody(Seq(TestLeafStatement("body1")))
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("body1", "con1", "body1", "con1"))
  }

  test("repeat - enters body with multiple statements multiple times") {
    val iter = TestCompoundBody(Seq(
      TestRepeat(
        condition = TestLoopCondition(condVal = false, reps = 2, description = "con1"),
        body = TestCompoundBody(Seq(
          TestLeafStatement("statement1"),
          TestLeafStatement("statement2")))
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("statement1", "statement2", "con1", "statement1", "statement2",
      "con1", "statement1", "statement2", "con1"))
  }

  test("nested repeat") {
    val iter = TestCompoundBody(Seq(
      TestRepeat(
        condition = TestLoopCondition(condVal = false, reps = 2, description = "con1"),
        body = TestCompoundBody(Seq(
          TestRepeat(
            condition = TestLoopCondition(condVal = false, reps = 2, description = "con2"),
            body = TestCompoundBody(Seq(TestLeafStatement("body1")))
          ))
        )
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("body1", "con2", "body1",
      "con2", "body1", "con2",
      "con1", "body1", "con2",
      "body1", "con2", "body1",
      "con2", "con1", "body1",
      "con2", "body1", "con2",
      "body1", "con2", "con1"))
  }

  test("leave compound block") {
    val iter = TestCompoundBody(
      statements = Seq(
        TestLeafStatement("one"),
        new LeaveStatementExec("lbl")
      ),
      label = Some("lbl")
    ).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("one", "lbl"))
  }

  test("leave while loop") {
    val iter = TestCompoundBody(
      statements = Seq(
        TestWhile(
          condition = TestLoopCondition(condVal = true, reps = 2, description = "con1"),
          body = TestCompoundBody(Seq(
            TestLeafStatement("body1"),
            new LeaveStatementExec("lbl"))
          ),
          label = Some("lbl")
        )
      )
    ).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "body1", "lbl"))
  }

  test("leave repeat loop") {
    val iter = TestCompoundBody(
      statements = Seq(
        TestRepeat(
          condition = TestLoopCondition(condVal = false, reps = 2, description = "con1"),
          body = TestCompoundBody(Seq(
            TestLeafStatement("body1"),
            new LeaveStatementExec("lbl"))
          ),
          label = Some("lbl")
        )
      )
    ).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("body1", "lbl"))
  }

  test("iterate while loop") {
    val iter = TestCompoundBody(
      statements = Seq(
        TestWhile(
          condition = TestLoopCondition(condVal = true, reps = 2, description = "con1"),
          body = TestCompoundBody(Seq(
            TestLeafStatement("body1"),
            new IterateStatementExec("lbl"),
            TestLeafStatement("body2"))
          ),
          label = Some("lbl")
        )
      )
    ).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "body1", "lbl", "con1", "body1", "lbl", "con1"))
  }

  test("iterate repeat loop") {
    val iter = TestCompoundBody(
      statements = Seq(
        TestRepeat(
          condition = TestLoopCondition(condVal = false, reps = 2, description = "con1"),
          body = TestCompoundBody(Seq(
            TestLeafStatement("body1"),
            new IterateStatementExec("lbl"),
            TestLeafStatement("body2"))
          ),
          label = Some("lbl")
        )
      )
    ).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(
      statements === Seq("body1", "lbl", "con1", "body1", "lbl", "con1", "body1", "lbl", "con1"))
  }

  test("leave outer loop from nested while loop") {
    val iter = TestCompoundBody(
      statements = Seq(
        TestWhile(
          condition = TestLoopCondition(condVal = true, reps = 2, description = "con1"),
          body = TestCompoundBody(Seq(
            TestWhile(
              condition = TestLoopCondition(condVal = true, reps = 2, description = "con2"),
              body = TestCompoundBody(Seq(
                TestLeafStatement("body1"),
                new LeaveStatementExec("lbl"))
              ),
              label = Some("lbl2")
            )
          )),
          label = Some("lbl")
        )
      )
    ).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2", "body1", "lbl"))
  }

  test("leave outer loop from nested repeat loop") {
    val iter = TestCompoundBody(
      statements = Seq(
        TestRepeat(
          condition = TestLoopCondition(condVal = false, reps = 2, description = "con1"),
          body = TestCompoundBody(Seq(
            TestRepeat(
              condition = TestLoopCondition(condVal = false, reps = 2, description = "con2"),
              body = TestCompoundBody(Seq(
                TestLeafStatement("body1"),
                new LeaveStatementExec("lbl"))
              ),
              label = Some("lbl2")
            )
          )),
          label = Some("lbl")
        )
      )
    ).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("body1", "lbl"))
  }

  test("iterate outer loop from nested while loop") {
    val iter = TestCompoundBody(
      statements = Seq(
        TestWhile(
          condition = TestLoopCondition(condVal = true, reps = 2, description = "con1"),
          body = TestCompoundBody(Seq(
            TestWhile(
              condition = TestLoopCondition(condVal = true, reps = 2, description = "con2"),
              body = TestCompoundBody(Seq(
                TestLeafStatement("body1"),
                new IterateStatementExec("lbl"),
                TestLeafStatement("body2"))
              ),
              label = Some("lbl2")
            )
          )),
          label = Some("lbl")
        )
      )
    ).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq(
      "con1", "con2", "body1", "lbl",
      "con1", "con2", "body1", "lbl",
      "con1"))
  }

  test("iterate outer loop from nested repeat loop") {
    val iter = TestCompoundBody(
      statements = Seq(
        TestRepeat(
          condition = TestLoopCondition(condVal = false, reps = 2, description = "con1"),
          body = TestCompoundBody(Seq(
            TestRepeat(
              condition = TestLoopCondition(condVal = false, reps = 2, description = "con2"),
              body = TestCompoundBody(Seq(
                TestLeafStatement("body1"),
                new IterateStatementExec("lbl"),
                TestLeafStatement("body2"))
              ),
              label = Some("lbl2")
            )
          )),
          label = Some("lbl")
        )
      )
    ).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq(
      "body1", "lbl", "con1",
      "body1", "lbl", "con1",
      "body1", "lbl", "con1"))
  }

  test("searched case - enter first WHEN clause") {
    val iter = TestCompoundBody(Seq(
      new CaseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = true, description = "con1"),
          TestIfElseCondition(condVal = false, description = "con2")
        ),
        conditionalBodies = Seq(
          TestCompoundBody(Seq(TestLeafStatement("body1"))),
          TestCompoundBody(Seq(TestLeafStatement("body2")))
        ),
        elseBody = Some(TestCompoundBody(Seq(TestLeafStatement("body3")))),
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "body1"))
  }

  test("searched case - enter body of the ELSE clause") {
    val iter = TestCompoundBody(Seq(
      new CaseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1")
        ),
        conditionalBodies = Seq(
          TestCompoundBody(Seq(TestLeafStatement("body1")))
        ),
        elseBody = Some(TestCompoundBody(Seq(TestLeafStatement("body2")))),
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "body2"))
  }

  test("searched case - enter second WHEN clause") {
    val iter = TestCompoundBody(Seq(
      new CaseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1"),
          TestIfElseCondition(condVal = true, description = "con2")
        ),
        conditionalBodies = Seq(
          TestCompoundBody(Seq(TestLeafStatement("body1"))),
          TestCompoundBody(Seq(TestLeafStatement("body2")))
        ),
        elseBody = Some(TestCompoundBody(Seq(TestLeafStatement("body3")))),
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2", "body2"))
  }

  test("searched case - without else (successful check)") {
    val iter = TestCompoundBody(Seq(
      new CaseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1"),
          TestIfElseCondition(condVal = true, description = "con2")
        ),
        conditionalBodies = Seq(
          TestCompoundBody(Seq(TestLeafStatement("body1"))),
          TestCompoundBody(Seq(TestLeafStatement("body2")))
        ),
        elseBody = None,
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2", "body2"))
  }

  test("searched case - without else (unsuccessful checks)") {
    val iter = TestCompoundBody(Seq(
      new CaseStatementExec(
        conditions = Seq(
          TestIfElseCondition(condVal = false, description = "con1"),
          TestIfElseCondition(condVal = false, description = "con2")
        ),
        conditionalBodies = Seq(
          TestCompoundBody(Seq(TestLeafStatement("body1"))),
          TestCompoundBody(Seq(TestLeafStatement("body2")))
        ),
        elseBody = None,
        session = spark
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("con1", "con2"))
  }

  test("loop statement with leave") {
    val iter = TestCompoundBody(
      statements = Seq(
        new LoopStatementExec(
          body = TestCompoundBody(Seq(
            TestLeafStatement("body1"),
            new LeaveStatementExec("lbl"))
          ),
          label = Some("lbl")
        )
      )
    ).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("body1", "lbl"))
  }

  test("for statement - enters body once") {
    val iter = TestCompoundBody(Seq(
      TestForStatement(
        query = MockQuery(1, "intCol", "query1"),
        variableName = Some("x"),
        label = Some("for1"),
        session = spark,
        body = TestCompoundBody(Seq(TestLeafStatement("body")))
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq(
      "body",
      "DropVariable", // drop for query var intCol
      "DropVariable" // drop for loop var x
    ))
  }

  test("for statement - enters body with multiple statements multiple times") {
    val iter = TestCompoundBody(Seq(
      TestForStatement(
        query = MockQuery(2, "intCol", "query1"),
        variableName = Some("x"),
        label = Some("for1"),
        session = spark,
        body = TestCompoundBody(
          Seq(TestLeafStatement("statement1"), TestLeafStatement("statement2"))
        )
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq(
      "statement1",
      "statement2",
      "statement1",
      "statement2",
      "DropVariable", // drop for query var intCol
      "DropVariable" // drop for loop var x
    ))
  }

  test("for statement - empty result") {
    val iter = TestCompoundBody(Seq(
      TestForStatement(
        query = MockQuery(0, "intCol", "query1"),
        variableName = Some("x"),
        label = Some("for1"),
        session = spark,
        body = TestCompoundBody(Seq(TestLeafStatement("body1")))
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq.empty[String])
  }

  test("for statement - nested") {
    val iter = TestCompoundBody(Seq(
      TestForStatement(
        query = MockQuery(2, "intCol", "query1"),
        variableName = Some("x"),
        label = Some("for1"),
        session = spark,
        body = TestCompoundBody(Seq(
          TestForStatement(
            query = MockQuery(2, "intCol1", "query2"),
            variableName = Some("y"),
            label = Some("for2"),
            session = spark,
            body = TestCompoundBody(Seq(TestLeafStatement("body")))
          )
        ))
      )),
      label = Some("lbl")
    ).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq(
      "body",
      "body",
      "DropVariable", // drop for query var intCol1
      "DropVariable", // drop for loop var y
      "body",
      "body",
      "DropVariable", // drop for query var intCol1
      "DropVariable", // drop for loop var y
      "DropVariable", // drop for query var intCol
      "DropVariable" // drop for loop var x
    ))
  }

  test("for statement no variable - enters body once") {
    val iter = TestCompoundBody(Seq(
      TestForStatement(
        query = MockQuery(1, "intCol", "query1"),
        variableName = None,
        label = Some("for1"),
        session = spark,
        body = TestCompoundBody(Seq(TestLeafStatement("body")))
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq(
      "body",
      "DropVariable" // drop for query var intCol
    ))
  }

  test("for statement no variable - enters body with multiple statements multiple times") {
    val iter = TestCompoundBody(Seq(
      TestForStatement(
        query = MockQuery(2, "intCol", "query1"),
        variableName = None,
        label = Some("for1"),
        session = spark,
        body = TestCompoundBody(Seq(
          TestLeafStatement("statement1"),
          TestLeafStatement("statement2")))
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq(
      "statement1", "statement2", "statement1", "statement2",
      "DropVariable" // drop for query var intCol
    ))
  }

  test("for statement no variable - empty result") {
    val iter = TestCompoundBody(Seq(
      TestForStatement(
        query = MockQuery(0, "intCol", "query1"),
        variableName = None,
        label = Some("for1"),
        session = spark,
        body = TestCompoundBody(Seq(TestLeafStatement("body1")))
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq.empty[String])
  }

  test("for statement no variable - nested") {
    val iter = TestCompoundBody(Seq(
      TestForStatement(
        query = MockQuery(2, "intCol", "query1"),
        variableName = None,
        label = Some("for1"),
        session = spark,
        body = TestCompoundBody(Seq(
          TestForStatement(
            query = MockQuery(2, "intCol1", "query2"),
            variableName = None,
            label = Some("for2"),
            session = spark,
            body = TestCompoundBody(Seq(TestLeafStatement("body")))
          )
        ))
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq(
      "body", "body",
      "DropVariable", // drop for query var intCol1
      "body", "body",
      "DropVariable", // drop for query var intCol1
      "DropVariable" // drop for query var intCol
    ))
  }

  test("for statement - iterate") {
    val iter = TestCompoundBody(Seq(
      TestForStatement(
        query = MockQuery(2, "intCol", "query1"),
        variableName = Some("x"),
        label = Some("lbl1"),
        session = spark,
        body = TestCompoundBody(Seq(
          TestLeafStatement("statement1"),
          new IterateStatementExec("lbl1"),
          TestLeafStatement("statement2")))
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq(
      "statement1",
      "lbl1",
      "statement1",
      "lbl1",
      "DropVariable", // drop for query var intCol
      "DropVariable" // drop for loop var x
    ))
  }

  test("for statement - leave") {
    val iter = TestCompoundBody(Seq(
      TestForStatement(
        query = MockQuery(2, "intCol", "query1"),
        variableName = Some("x"),
        label = Some("lbl1"),
        session = spark,
        body = TestCompoundBody(Seq(
          TestLeafStatement("statement1"),
          new LeaveStatementExec("lbl1"),
          TestLeafStatement("statement2")))
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("statement1", "lbl1"))
  }

  test("for statement - nested - iterate outer loop") {
    val iter = TestCompoundBody(Seq(
      TestForStatement(
        query = MockQuery(2, "intCol", "query1"),
        variableName = Some("x"),
        label = Some("lbl1"),
        session = spark,
        body = TestCompoundBody(Seq(
          TestLeafStatement("outer_body"),
          TestForStatement(
            query = MockQuery(2, "intCol1", "query2"),
            variableName = Some("y"),
            label = Some("lbl2"),
            session = spark,
            body = TestCompoundBody(Seq(
              TestLeafStatement("body1"),
              new IterateStatementExec("lbl1"),
              TestLeafStatement("body2")))
          )
        ))
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq(
      "outer_body",
      "body1",
      "lbl1",
      "outer_body",
      "body1",
      "lbl1",
      "DropVariable", // drop for query var intCol
      "DropVariable" // drop for loop var x
    ))
  }

  test("for statement - nested - leave outer loop") {
    val iter = TestCompoundBody(Seq(
      TestForStatement(
        query = MockQuery(2, "intCol", "query1"),
        variableName = Some("x"),
        label = Some("lbl1"),
        session = spark,
        body = TestCompoundBody(Seq(
          TestForStatement(
            query = MockQuery(2, "intCol", "query2"),
            variableName = Some("y"),
            label = Some("lbl2"),
            session = spark,
            body = TestCompoundBody(Seq(
              TestLeafStatement("body1"),
              new LeaveStatementExec("lbl1"),
              TestLeafStatement("body2")))
          )
        ))
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("body1", "lbl1"))
  }

  test("for statement no variable - iterate") {
    val iter = TestCompoundBody(Seq(
      TestForStatement(
        query = MockQuery(2, "intCol", "query1"),
        variableName = None,
        label = Some("lbl1"),
        session = spark,
        body = TestCompoundBody(Seq(
          TestLeafStatement("statement1"),
          new IterateStatementExec("lbl1"),
          TestLeafStatement("statement2")))
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq(
      "statement1", "lbl1", "statement1", "lbl1",
      "DropVariable" // drop for query var intCol
    ))
  }

  test("for statement no variable - leave") {
    val iter = TestCompoundBody(Seq(
      TestForStatement(
        query = MockQuery(2, "intCol", "query1"),
        variableName = None,
        label = Some("lbl1"),
        session = spark,
        body = TestCompoundBody(Seq(
          TestLeafStatement("statement1"),
          new LeaveStatementExec("lbl1"),
          TestLeafStatement("statement2")))
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("statement1", "lbl1"))
  }

  test("for statement no variable - nested - iterate outer loop") {
    val iter = TestCompoundBody(Seq(
      TestForStatement(
        query = MockQuery(2, "intCol", "query1"),
        variableName = None,
        label = Some("lbl1"),
        session = spark,
        body = TestCompoundBody(Seq(
          TestLeafStatement("outer_body"),
          TestForStatement(
            query = MockQuery(2, "intCol1", "query2"),
            variableName = None,
            label = Some("lbl2"),
            session = spark,
            body = TestCompoundBody(Seq(
              TestLeafStatement("body1"),
              new IterateStatementExec("lbl1"),
              TestLeafStatement("body2")))
          )
        ))
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq(
      "outer_body", "body1", "lbl1", "outer_body", "body1", "lbl1",
      "DropVariable" // drop for query var intCol
    ))
  }

  test("for statement no variable - nested - leave outer loop") {
    val iter = TestCompoundBody(Seq(
      TestForStatement(
        query = MockQuery(2, "intCol", "query1"),
        variableName = None,
        label = Some("lbl1"),
        session = spark,
        body = TestCompoundBody(Seq(
          TestForStatement(
            query = MockQuery(2, "intCol1", "query2"),
            variableName = None,
            label = Some("lbl2"),
            session = spark,
            body = TestCompoundBody(Seq(
              TestLeafStatement("body1"),
              new LeaveStatementExec("lbl1"),
              TestLeafStatement("body2")))
          )
        ))
      )
    )).getTreeIterator
    val statements = iter.map(extractStatementValue).toSeq
    assert(statements === Seq("body1", "lbl1"))
  }
}
