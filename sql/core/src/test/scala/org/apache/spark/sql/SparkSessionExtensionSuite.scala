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
package org.apache.spark.sql

import org.apache.spark.{SparkFunSuite, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, AttributeReference, BindReferences, Expression, ExpressionInfo, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, ProjectExec, SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Test cases for the [[SparkSessionExtensions]].
 */
class SparkSessionExtensionSuite extends SparkFunSuite {
  type ExtensionsBuilder = SparkSessionExtensions => Unit
  private def create(builder: ExtensionsBuilder): Seq[ExtensionsBuilder] = Seq(builder)

  private def stop(spark: SparkSession): Unit = {
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def withSession(builders: Seq[ExtensionsBuilder])(f: SparkSession => Unit): Unit = {
    val builder = SparkSession.builder().master("local[1]")
    builders.foreach(builder.withExtensions)
    val spark = builder.getOrCreate()
    try f(spark) finally {
      stop(spark)
    }
  }

  test("inject analyzer rule") {
    withSession(Seq(_.injectResolutionRule(MyRule))) { session =>
      assert(session.sessionState.analyzer.extendedResolutionRules.contains(MyRule(session)))
    }
  }

  test("inject post hoc resolution analyzer rule") {
    withSession(Seq(_.injectPostHocResolutionRule(MyRule))) { session =>
      assert(session.sessionState.analyzer.postHocResolutionRules.contains(MyRule(session)))
    }
  }

  test("inject check analysis rule") {
    withSession(Seq(_.injectCheckRule(MyCheckRule))) { session =>
      assert(session.sessionState.analyzer.extendedCheckRules.contains(MyCheckRule(session)))
    }
  }

  test("inject optimizer rule") {
    withSession(Seq(_.injectOptimizerRule(MyRule))) { session =>
      assert(session.sessionState.optimizer.batches.flatMap(_.rules).contains(MyRule(session)))
    }
  }

  test("inject spark planner strategy") {
    withSession(Seq(_.injectPlannerStrategy(MySparkStrategy))) { session =>
      assert(session.sessionState.planner.strategies.contains(MySparkStrategy(session)))
    }
  }

  test("inject parser") {
    val extension = create { extensions =>
      extensions.injectParser((_: SparkSession, _: ParserInterface) => CatalystSqlParser)
    }
    withSession(extension) { session =>
      assert(session.sessionState.sqlParser === CatalystSqlParser)
    }
  }

  test("inject multiple rules") {
    withSession(Seq(_.injectOptimizerRule(MyRule),
        _.injectPlannerStrategy(MySparkStrategy))) { session =>
      assert(session.sessionState.optimizer.batches.flatMap(_.rules).contains(MyRule(session)))
      assert(session.sessionState.planner.strategies.contains(MySparkStrategy(session)))
    }
  }

  test("inject stacked parsers") {
    val extension = create { extensions =>
      extensions.injectParser((_: SparkSession, _: ParserInterface) => CatalystSqlParser)
      extensions.injectParser(MyParser)
      extensions.injectParser(MyParser)
    }
    withSession(extension) { session =>
      val parser = MyParser(session, MyParser(session, CatalystSqlParser))
      assert(session.sessionState.sqlParser === parser)
    }
  }

  test("inject function") {
    val extensions = create { extensions =>
      extensions.injectFunction(MyExtensions.myFunction)
    }
    withSession(extensions) { session =>
      assert(session.sessionState.functionRegistry
        .lookupFunction(MyExtensions.myFunction._1).isDefined)
    }
  }

  test("inject columnar") {
    val extensions = create { extensions =>
      extensions.injectColumnar(session => MyColumarRule(PreRuleReplaceAdd(), MyPostRule()))
    }
    withSession(extensions) { session =>
      assert(session.sessionState.columnarRules.contains(
        MyColumarRule(PreRuleReplaceAdd(), MyPostRule())))
      import session.sqlContext.implicits._
      // repartitioning avoids having the add operation pushed up into the LocalTableScan
      val data = Seq((100L), (200L), (300L)).toDF("vals").repartition(1)
      val result = data.selectExpr("vals + 1").collect()
      assert(result(0).getLong(0) == 102L)
      assert(result(1).getLong(0) == 202L)
      assert(result(2).getLong(0) == 302L)
    }
  }

  test("use custom class for extensions") {
    val session = SparkSession.builder()
      .master("local[1]")
      .config("spark.sql.extensions", classOf[MyExtensions].getCanonicalName)
      .getOrCreate()
    try {
      assert(session.sessionState.planner.strategies.contains(MySparkStrategy(session)))
      assert(session.sessionState.analyzer.extendedResolutionRules.contains(MyRule(session)))
      assert(session.sessionState.analyzer.postHocResolutionRules.contains(MyRule(session)))
      assert(session.sessionState.analyzer.extendedCheckRules.contains(MyCheckRule(session)))
      assert(session.sessionState.optimizer.batches.flatMap(_.rules).contains(MyRule(session)))
      assert(session.sessionState.sqlParser.isInstanceOf[MyParser])
      assert(session.sessionState.functionRegistry
        .lookupFunction(MyExtensions.myFunction._1).isDefined)
      assert(session.sessionState.columnarRules.contains(
        MyColumarRule(PreRuleReplaceAdd(), MyPostRule())))
    } finally {
      stop(session)
    }
  }

  test("use multiple custom class for extensions in the specified order") {
    val session = SparkSession.builder()
      .master("local[1]")
      .config("spark.sql.extensions", Seq(
        classOf[MyExtensions2].getCanonicalName,
        classOf[MyExtensions].getCanonicalName).mkString(","))
      .getOrCreate()
    try {
      assert(session.sessionState.planner.strategies.containsSlice(
        Seq(MySparkStrategy2(session), MySparkStrategy(session))))
      val orderedRules = Seq(MyRule2(session), MyRule(session))
      val orderedCheckRules = Seq(MyCheckRule2(session), MyCheckRule(session))
      val parser = MyParser(session, CatalystSqlParser)
      assert(session.sessionState.analyzer.extendedResolutionRules.containsSlice(orderedRules))
      assert(session.sessionState.analyzer.postHocResolutionRules.containsSlice(orderedRules))
      assert(session.sessionState.analyzer.extendedCheckRules.containsSlice(orderedCheckRules))
      assert(session.sessionState.optimizer.batches.flatMap(_.rules).filter(orderedRules.contains)
        .containsSlice(orderedRules ++ orderedRules)) // The optimizer rules are duplicated
      assert(session.sessionState.sqlParser === parser)
      assert(session.sessionState.functionRegistry
        .lookupFunction(MyExtensions.myFunction._1).isDefined)
      assert(session.sessionState.functionRegistry
        .lookupFunction(MyExtensions2.myFunction._1).isDefined)
    } finally {
      stop(session)
    }
  }

  test("allow an extension to be duplicated") {
    val session = SparkSession.builder()
      .master("local[1]")
      .config("spark.sql.extensions", Seq(
        classOf[MyExtensions].getCanonicalName,
        classOf[MyExtensions].getCanonicalName).mkString(","))
      .getOrCreate()
    try {
      assert(session.sessionState.planner.strategies.count(_ === MySparkStrategy(session)) === 2)
      assert(session.sessionState.analyzer.extendedResolutionRules.count(_ === MyRule(session)) ===
        2)
      assert(session.sessionState.analyzer.postHocResolutionRules.count(_ === MyRule(session)) ===
        2)
      assert(session.sessionState.analyzer.extendedCheckRules.count(_ === MyCheckRule(session)) ===
        2)
      assert(session.sessionState.optimizer.batches.flatMap(_.rules)
        .count(_ === MyRule(session)) === 4) // The optimizer rules are duplicated
      val outerParser = session.sessionState.sqlParser
      assert(outerParser.isInstanceOf[MyParser])
      assert(outerParser.asInstanceOf[MyParser].delegate.isInstanceOf[MyParser])
      assert(session.sessionState.functionRegistry
        .lookupFunction(MyExtensions.myFunction._1).isDefined)
    } finally {
      stop(session)
    }
  }

  test("use the last registered function name when there are duplicates") {
    val session = SparkSession.builder()
      .master("local[1]")
      .config("spark.sql.extensions", Seq(
        classOf[MyExtensions2].getCanonicalName,
        classOf[MyExtensions2Duplicate].getCanonicalName).mkString(","))
      .getOrCreate()
    try {
      val lastRegistered = session.sessionState.functionRegistry
        .lookupFunction(FunctionIdentifier("myFunction2"))
      assert(lastRegistered.isDefined)
      assert(lastRegistered.get !== MyExtensions2.myFunction._2)
      assert(lastRegistered.get === MyExtensions2Duplicate.myFunction._2)
    } finally {
      stop(session)
    }
  }
}

case class MyRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan
}

case class MyCheckRule(spark: SparkSession) extends (LogicalPlan => Unit) {
  override def apply(plan: LogicalPlan): Unit = { }
}

case class MySparkStrategy(spark: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = Seq.empty
}

case class MyParser(spark: SparkSession, delegate: ParserInterface) extends ParserInterface {
  override def parsePlan(sqlText: String): LogicalPlan =
    delegate.parsePlan(sqlText)

  override def parseExpression(sqlText: String): Expression =
    delegate.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    delegate.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    delegate.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType =
    delegate.parseDataType(sqlText)
}

object MyExtensions {

  val myFunction = (FunctionIdentifier("myFunction"),
    new ExpressionInfo("noClass", "myDb", "myFunction", "usage", "extended usage"),
    (_: Seq[Expression]) => Literal(5, IntegerType))
}

case class CloseableColumnBatchIterator(itr: Iterator[ColumnarBatch],
    f: ColumnarBatch => ColumnarBatch) extends Iterator[ColumnarBatch] {
  var cb: ColumnarBatch = null

  private def closeCurrentBatch(): Unit = {
    if (cb != null) {
      cb.close
      cb = null
    }
  }

  TaskContext.get().addTaskCompletionListener[Unit]((tc: TaskContext) => {
    closeCurrentBatch()
  })

  override def hasNext: Boolean = {
    closeCurrentBatch()
    itr.hasNext
  }

  override def next(): ColumnarBatch = {
    closeCurrentBatch()
    cb = f(itr.next())
    cb
  }
}

/**
 * A version of ProjectExec that adds in columnar support.
 */
class ColumnarProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
  extends ProjectExec(projectList, child) {

  override def supportsColumnar: Boolean = true

  // Disable code generation
  override def supportCodegen: Boolean = false

  override def doExecuteColumnar() : RDD[ColumnarBatch] = {
    val boundProjectList: Seq[Any] = BindReferences.bindReferences(projectList, child.output)
    val rdd = child.executeColumnar()
    rdd.mapPartitions((itr) => CloseableColumnBatchIterator(itr,
      (cb) => {
        val newColumns = boundProjectList.map(
          expr => expr.asInstanceOf[Expression].columnarEval(cb).asInstanceOf[ColumnVector]
        ).toArray
        new ColumnarBatch(newColumns, cb.numRows())
      })
    )
  }

  // We have to override equals because subclassing a case class like ProjectExec is not that clean
  // One of the issues is that the generated equals will see ColumnarProjectExec and ProjectExec
  // as being equal and this can result in the withNewChildren method not actually replacing
  // anything
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[ColumnarProjectExec]
  }

  override def hashCode(): Int = super.hashCode()
}

/**
 * A version of add that supports columnar processing for longs.
 */
class ColumnarAdd(left: Expression, right: Expression) extends Add(left, right) {
  override def supportsColumnar(): Boolean = left.supportsColumnar && right.supportsColumnar

  override def columnarEval(batch: ColumnarBatch): Any = {
    var lhs: Any = null
    var rhs: Any = null
    var ret: Any = null
    try {
      lhs = left.columnarEval(batch)
      rhs = right.columnarEval(batch)

      if (lhs == null || rhs == null) {
        ret = null
      } else if (lhs.isInstanceOf[ColumnVector] && rhs.isInstanceOf[ColumnVector]) {
        val l = lhs.asInstanceOf[ColumnVector]
        val r = rhs.asInstanceOf[ColumnVector]
        val result = new OnHeapColumnVector(batch.numRows(), dataType)
        ret = result

        for (i <- 0 until batch.numRows()) {
          result.appendLong(l.getLong(i) + r.getLong(i) + 1) // BUG to show we replaced Add
        }
      } else if (rhs.isInstanceOf[ColumnVector]) {
        val l = lhs.asInstanceOf[Long]
        val r = rhs.asInstanceOf[ColumnVector]
        val result = new OnHeapColumnVector(batch.numRows(), dataType)
        ret = result

        for (i <- 0 until batch.numRows()) {
          result.appendLong(l + r.getLong(i) + 1) // BUG to show we replaced Add
        }
      } else if (lhs.isInstanceOf[ColumnVector]) {
        val l = lhs.asInstanceOf[ColumnVector]
        val r = rhs.asInstanceOf[Long]
        val result = new OnHeapColumnVector(batch.numRows(), dataType)
        ret = result

        for (i <- 0 until batch.numRows()) {
          result.appendLong(l.getLong(i) + r + 1) // BUG to show we replaced Add
        }
      } else {
        ret = nullSafeEval(lhs, rhs)
      }
    } finally {
      if (lhs != null && lhs.isInstanceOf[ColumnVector]) {
        lhs.asInstanceOf[ColumnVector].close()
      }
      if (rhs != null && rhs.isInstanceOf[ColumnVector]) {
        rhs.asInstanceOf[ColumnVector].close()
      }
    }
    ret
  }

  // Again we need to override equals because we are subclassing a case class
  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[ColumnarAdd]
  }

  override def hashCode(): Int = super.hashCode()
}

case class PreRuleReplaceAdd() extends Rule[SparkPlan] {
  def replaceWithColumnarExpression(exp: Expression): Expression = exp match {
    case a: Alias =>
      Alias(replaceWithColumnarExpression(a.child),
        a.name)(a.exprId, a.qualifier, a.explicitMetadata)
    case att: AttributeReference =>
      att // No sub expressions and already supports columnar so just return it
    case lit: Literal =>
      lit // No sub expressions and already supports columnar so just return it
    case add: Add if (add.dataType == LongType) &&
      (add.left.dataType == LongType) &&
      (add.right.dataType == LongType) =>
      // Add only supports Longs for now.
      new ColumnarAdd(replaceWithColumnarExpression(add.left),
        replaceWithColumnarExpression(add.right))
    case exp =>
      logWarning(s"Columnar Processing for expression ${exp.getClass} ${exp} " +
        "is not currently supported.")
      exp
  }

  def replaceWithColumnarPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: ProjectExec =>
      new ColumnarProjectExec(plan.projectList.map((exp) =>
        replaceWithColumnarExpression(exp).asInstanceOf[NamedExpression]),
        replaceWithColumnarPlan(plan.child))
    case p =>
      logWarning(s"Columnar Processing for ${p.getClass} is not currently supported.")
      p.withNewChildren(p.children.map(replaceWithColumnarPlan))
  }

  override def apply(plan: SparkPlan): SparkPlan = replaceWithColumnarPlan(plan)
}

case class MyPostRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan
}

case class MyColumarRule(pre: Rule[SparkPlan], post: Rule[SparkPlan]) extends ColumnarRule {
  override def preColumnarTransitions: Rule[SparkPlan] = pre
  override def postColumnarTransitions: Rule[SparkPlan] = post
}

class MyExtensions extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectPlannerStrategy(MySparkStrategy)
    e.injectResolutionRule(MyRule)
    e.injectPostHocResolutionRule(MyRule)
    e.injectCheckRule(MyCheckRule)
    e.injectOptimizerRule(MyRule)
    e.injectParser(MyParser)
    e.injectFunction(MyExtensions.myFunction)
    e.injectColumnar(session => MyColumarRule(PreRuleReplaceAdd(), MyPostRule()))
  }
}

case class MyRule2(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan
}

case class MyCheckRule2(spark: SparkSession) extends (LogicalPlan => Unit) {
  override def apply(plan: LogicalPlan): Unit = { }
}

case class MySparkStrategy2(spark: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = Seq.empty
}

object MyExtensions2 {

  val myFunction = (FunctionIdentifier("myFunction2"),
    new ExpressionInfo("noClass", "myDb", "myFunction2", "usage", "extended usage"),
    (_: Seq[Expression]) => Literal(5, IntegerType))
}

class MyExtensions2 extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectPlannerStrategy(MySparkStrategy2)
    e.injectResolutionRule(MyRule2)
    e.injectPostHocResolutionRule(MyRule2)
    e.injectCheckRule(MyCheckRule2)
    e.injectOptimizerRule(MyRule2)
    e.injectParser((_: SparkSession, _: ParserInterface) => CatalystSqlParser)
    e.injectFunction(MyExtensions2.myFunction)
  }
}

object MyExtensions2Duplicate {

  val myFunction = (FunctionIdentifier("myFunction2"),
    new ExpressionInfo("noClass", "myDb", "myFunction2", "usage", "extended usage"),
    (_: Seq[Expression]) => Literal(5, IntegerType))
}

class MyExtensions2Duplicate extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectFunction(MyExtensions2Duplicate.myFunction)
  }
}
