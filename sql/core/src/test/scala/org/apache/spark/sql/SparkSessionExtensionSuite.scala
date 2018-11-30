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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.{Order, Rule}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.types.{DataType, IntegerType, StructType}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Test cases for the [[SparkSessionExtensions]].
 */
class SparkSessionExtensionSuite extends SparkFunSuite {
  type ExtensionsBuilder = SparkSessionExtensions => Unit
  private def create(builder: ExtensionsBuilder): ExtensionsBuilder = builder

  private def stop(spark: SparkSession): Unit = {
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def withSession(builder: ExtensionsBuilder)(f: SparkSession => Unit): Unit = {
    val spark = SparkSession.builder().master("local[1]").withExtensions(builder).getOrCreate()
    try f(spark) finally {
      stop(spark)
    }
  }

  private def withSession()(f: SparkSession => Unit): Unit = {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    try f(spark) finally {
      stop(spark)
    }
  }

  test("inject analyzer rule") {
    withSession(_.injectResolutionRule(MyRule)) { session =>
      assert(session.sessionState.analyzer.extendedResolutionRules.contains(MyRule(session)))
    }
  }

  test("inject check analysis rule") {
    withSession(_.injectCheckRule(MyCheckRule)) { session =>
      assert(session.sessionState.analyzer.extendedCheckRules.contains(MyCheckRule(session)))
    }
  }

  test("inject optimizer rule") {
    withSession(_.injectOptimizerRule(MyRule)) { session =>
      assert(session.sessionState.optimizer.batches.flatMap(_.rules).contains(MyRule(session)))
    }
  }

  val RMLITERALGRPEXP = "org.apache.spark.sql.catalyst.optimizer.RemoveLiteralFromGroupExpressions"
  val RMREPGRPEXP = "org.apache.spark.sql.catalyst.optimizer.RemoveRepetitionFromGroupExpressions"

  test("inject optimizer rule in batch order - after") {
    withSession(_.injectOptimizerRuleInOrder(
      MyRule1,
      "Aggregate",
      Order.after,
      RMLITERALGRPEXP)) { session =>
      val rules = Seq(RMLITERALGRPEXP, "org.apache.spark.sql.MyRule1", RMREPGRPEXP)
      val batch = session.sessionState.optimizer.batches.find(_.name.equals("Aggregate"))
      assert(batch.isDefined)
      assert(rules.equals(batch.get.rules.map(_.ruleName)))
    }
  }

  test("inject optimizer rule in batch order - before") {
    withSession(_.injectOptimizerRuleInOrder(
      MyRule1,
      "Aggregate",
      Order.before,
      RMLITERALGRPEXP)) { session =>
      val rules = Seq("org.apache.spark.sql.MyRule1", RMLITERALGRPEXP, RMREPGRPEXP)
      val batch = session.sessionState.optimizer.batches.find(_.name.equals("Aggregate"))
      assert(batch.isDefined)
      assert(rules.equals(batch.get.rules.map(_.ruleName)))
    }
  }

  test("two inject optimizer rule in batch order - after") {
    withSession(e => {
      e.injectOptimizerRuleInOrder(
        MyRule1,
        "Aggregate",
        Order.after,
        RMLITERALGRPEXP)
      e.injectOptimizerRuleInOrder(
        MyRule,
        "Aggregate",
        Order.after,
        RMLITERALGRPEXP)
    }) { session =>
      val rules = Seq(RMLITERALGRPEXP,
        "org.apache.spark.sql.MyRule1", "org.apache.spark.sql.MyRule", RMREPGRPEXP)
      val batch = session.sessionState.optimizer.batches.find(_.name.equals("Aggregate"))
      assert(batch.isDefined)
      assert(rules.equals(batch.get.rules.map(_.ruleName)))
    }
  }

  test("two inject optimizer rule to different batches") {
    withSession(e => {
      e.injectOptimizerRuleInOrder(
        MyRule1,
        "Union",
        Order.before,
        "org.apache.spark.sql.catalyst.optimizer.CombineUnions")
      e.injectOptimizerRuleInOrder(
        MyRule,
        "Aggregate",
        Order.after,
        RMLITERALGRPEXP)
    }) { session =>
      val aggBatch = Seq(RMLITERALGRPEXP, "org.apache.spark.sql.MyRule", RMREPGRPEXP)
      val batch = session.sessionState.optimizer.batches.find(_.name.equals("Aggregate"))
      assert(batch.isDefined)
      assert(aggBatch.equals(batch.get.rules.map(_.ruleName)))
      val unionBatchRules = Seq("org.apache.spark.sql.MyRule1",
        "org.apache.spark.sql.catalyst.optimizer.CombineUnions")
      val unionBatch = session.sessionState.optimizer.batches.find(_.name.equals("Union"))
      assert(unionBatch.isDefined)
      assert(unionBatchRules.equals(unionBatch.get.rules.map(_.ruleName)))
    }
  }

  test("two rules after and two rules before to a batch") {
    withSession(e => {
      e.injectOptimizerRuleInOrder(
        MyRule,
        "Union",
        Order.before,
        "org.apache.spark.sql.catalyst.optimizer.CombineUnions")
      e.injectOptimizerRuleInOrder(
        MyRule1,
        "Union",
        Order.before,
        "org.apache.spark.sql.catalyst.optimizer.CombineUnions")
      e.injectOptimizerRuleInOrder(
        MyRule2,
        "Union",
        Order.after,
        "org.apache.spark.sql.catalyst.optimizer.CombineUnions")
      e.injectOptimizerRuleInOrder(
        MyRule3,
        "Union",
        Order.after,
        "org.apache.spark.sql.catalyst.optimizer.CombineUnions")
    }) { session =>
      val unionBatchRules = Seq("org.apache.spark.sql.MyRule",
        "org.apache.spark.sql.MyRule1",
        "org.apache.spark.sql.catalyst.optimizer.CombineUnions",
        "org.apache.spark.sql.MyRule2",
        "org.apache.spark.sql.MyRule3")
      val unionBatch = session.sessionState.optimizer.batches.find(_.name.equals("Union"))
      assert(unionBatch.isDefined)
      assert(unionBatchRules.equals(unionBatch.get.rules.map(_.ruleName)))
    }
  }

  test("inject optimizer rule in batch order - nonexistent existing rule") {
    withSession(_.injectOptimizerRuleInOrder(
      MyRule1,
      "Aggregate",
      Order.after,
      "org.apache.spark.sql.catalyst.optimizer.XYZ")) { session =>
      val errMsg = intercept[Exception] {
        session.sessionState.optimizer.batches.find(_.name.equals("Aggregate"))
      }.getMessage()
      assert(errMsg.contains("Unable to add the customized optimization rule(s) to " +
        "batch Aggregate: [org.apache.spark.sql.MyRule1]." ))
    }
  }

  test("inject optimizer rule in batch order - 2 nonexistent rule") {
    withSession(e => {
      e.injectOptimizerRuleInOrder(
        MyRule1,
        "Aggregate",
        Order.before,
        "org.apache.spark.sql.catalyst.optimizer.XYZ")
      e.injectOptimizerRuleInOrder(
        MyRule,
        "Aggregate",
        Order.after,
        "org.apache.spark.sql.catalyst.optimizeX.RemoveRepetitionFromGroupExpressions")
    }) { session =>
      val errMsg = intercept[Exception] {
        val batch = session.sessionState.optimizer.batches
      }.getMessage()
      val expected = "Unable to add the customized optimization rule(s) to batch Aggregate: " +
        "[org.apache.spark.sql.MyRule1,org.apache.spark.sql.MyRule]"
      assert(errMsg.contains(expected))
    }
  }

  test("inject optimizer rule in batch order - existent and nonexistent after rule") {
    withSession(e => {
      e.injectOptimizerRuleInOrder(
        MyRule1,
        "Aggregate",
        Order.after,
        "org.apache.spark.sql.catalyst.optimizer.XYZ")
      e.injectOptimizerRuleInOrder(
        MyRule,
        "Aggregate",
        Order.before,
        RMREPGRPEXP)
    }) { session =>
      val errMsg = intercept[Exception] {
        val batch = session.sessionState.optimizer.batches.find(_.name.equals("Aggregate"))
      }.getMessage()
      assert(errMsg.contains("[org.apache.spark.sql.MyRule1]"))
      assert(!errMsg.contains("[org.apache.spark.sql.MyRule]"))
    }
  }

  test("inject optimizer rule in batch order - nonexistent batch") {
    withSession(e => {
      e.injectOptimizerRuleInOrder(
        MyRule1,
        "Aggregate1",
        Order.before,
        "org.apache.spark.sql.catalyst.optimizer.XYZ")
      e.injectOptimizerRuleInOrder(
        MyRule,
        "Aggregate",
        Order.before,
        RMREPGRPEXP)
    }) { session =>
      val errMsg = intercept[Exception] {
        val batch = session.sessionState.optimizer.batches
      }.getMessage()
      val expected = "Unable to add the customized optimization rule:" +
        "[(org.apache.spark.sql.MyRule1 to batch Aggregate1)]"
      assert(errMsg.contains(expected))
    }
  }

  test("inject optimizer batch") {
    val myrules = Seq(MyRule1, MyRule)
    withSession(_.injectOptimizerBatch("MyBatch", 10, "Aggregate", Order.after, myrules))
    { session =>
      val batches = session.sessionState.optimizer.batches.map(_.name)
      val idx = batches.indexOf("MyBatch")
      assert(batches.indexOf("Aggregate") == (idx - 1))
      val mybatch = session.sessionState.optimizer.batches(idx)
      assert(mybatch.strategy.maxIterations == 10)
      assert(mybatch.rules.map(_.ruleName).equals
        (Seq("org.apache.spark.sql.MyRule1", "org.apache.spark.sql.MyRule")))
    }
  }

  test("inject multiple optimizer batch order and disable rule") {
    val myrules = Seq(MyRule1, MyRule)
    val e = create { extensions =>
      extensions.injectOptimizerBatch("MyBatch1", 10, "Aggregate", Order.after, myrules)
      extensions.injectOptimizerBatch("MyBatch2", 20, "Union", Order.before, Seq(MyRule))
      extensions.injectOptimizerBatch("MyBatch3", 20, "Union", Order.after, Seq(MyRule1))
      extensions.injectOptimizerBatch("MyBatch4", 20, "Union", Order.after, Seq(MyRule1))
    }
    withSession(e) { session =>
      val batches = session.sessionState.optimizer.batches
      val batchNames = batches.map(_.name)
      assert(batchNames.filter(_.equals("Union")).length == 1)
      assert(batchNames.indexOf("Aggregate") == (batchNames.indexOf("MyBatch1") - 1))
      val mybatch = batches(batchNames.indexOf("MyBatch1"))
      assert(mybatch.strategy.maxIterations == 10)
      assert(mybatch.rules.map(_.ruleName).equals
        (Seq("org.apache.spark.sql.MyRule1", "org.apache.spark.sql.MyRule")))
      assert(batchNames.indexOf("Union") == (batchNames.indexOf("MyBatch2") + 1))
      val batch2 = batches(batchNames.indexOf("MyBatch2"))
      assert(batch2.strategy.maxIterations == 20)
      assert(batch2.rules.map(_.ruleName).equals(Seq("org.apache.spark.sql.MyRule")))

      val batch3 = batches(batchNames.indexOf("MyBatch3"))
      assert(batchNames.indexOf("MyBatch3") == (batchNames.indexOf("Union") + 1))
      assert(batch3.strategy.maxIterations == 20)
      assert(batch3.rules.map(_.ruleName).equals(Seq("org.apache.spark.sql.MyRule1")))
      assert(batchNames.indexOf("MyBatch4") == (batchNames.indexOf("Union") + 2))

      // Disable a rule in the batch.
      session.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.MyRule")
      val myBatch2 = session.sessionState.optimizer.batches.find(_.name.equals("MyBatch1"))
      myBatch2.get.rules.map(_.ruleName).equals(Seq("org.apache.spark.sql.MyRule1"))
    }
  }

  test("inject optimizer batch - nonexistent batch") {
    val myrules = Seq(MyRule1, MyRule)
    withSession(_.injectOptimizerBatch("MyBatch", 10, "nonexistent", Order.after, myrules))
    { session =>
      val errMsg = intercept[Exception] {
        session.sessionState.optimizer.batches.length
      }.getMessage
      assert(errMsg.contains("Unable to add optimizer batch: [MyBatch]"))
    }
  }

  test("inject optimizer batch - existent and nonexistent after batch") {
    val myrules = Seq(MyRule1, MyRule)
    val e = create { extensions =>
      extensions.injectOptimizerBatch("MyBatch1", 10, "Aggregate", Order.after, myrules)
      extensions.injectOptimizerBatch("MyBatch2", 20, "Union2", Order.after, myrules)
      extensions.injectOptimizerBatch("MyBatch", 10, "nonexistent", Order.after, myrules)
    }
    withSession(e) { session =>
      val errMsg = intercept[Exception] {
        session.sessionState.optimizer.batches
      }.getMessage
      assert(errMsg.contains("Unable to add optimizer batch: [MyBatch2,MyBatch]"))
    }
  }

  test("existing rule name is in the excluded list") {
    val afterRuleName = RMLITERALGRPEXP
    withSession(_.injectOptimizerRuleInOrder(
      MyRule1,
      "Aggregate",
      Order.after,
      afterRuleName)) { session =>
      session.conf.set("spark.sql.optimizer.excludedRules", afterRuleName)
      val errMsg = intercept[Exception] {
        val batch = session.sessionState.optimizer.batches.find(_.name.equals("Aggregate"))
      }.getMessage
      assert(errMsg.contains("Unable to add the customized optimization rule(s) " +
        "to batch Aggregate: [org.apache.spark.sql.MyRule1]"))
    }
  }

  test("exclude a rule") {
    val excludeRule = RMLITERALGRPEXP
    withSession(_.injectOptimizerRuleInOrder(
      MyRule1,
      "Aggregate",
      Order.before,
      RMREPGRPEXP)) { session =>
      session.conf.set("spark.sql.optimizer.excludedRules", excludeRule)
      val rules = Seq("org.apache.spark.sql.MyRule1", RMREPGRPEXP)
      val batch = session.sessionState.optimizer.batches.find(_.name.equals("Aggregate"))
      assert(batch.isDefined)
      assert(rules.equals(batch.get.rules.map(_.ruleName)))
    }
  }

  test("excluded after rule - removes the batch itself") {
    val excludeRules = RMLITERALGRPEXP + "," + RMREPGRPEXP
    withSession(_.injectOptimizerRuleInOrder(
      MyRule1,
      "Aggregate",
      Order.after,
      RMREPGRPEXP)) { session =>
      session.conf.set("spark.sql.optimizer.excludedRules", excludeRules)
      val errMsg = intercept[Exception] {
        val batch = session.sessionState.optimizer.batches.find(_.name.equals("Aggregate"))
      }.getMessage
      assert(errMsg.contains("Unable to add the customized optimization rule(s) to " +
        "batch Aggregate: [org.apache.spark.sql.MyRule1]"))
    }
  }

  test("disable injected rule using the exclude") {
    val excludeRules = "org.apache.spark.sql.MyRule1"
    withSession(_.injectOptimizerRuleInOrder(
      MyRule1,
      "Aggregate",
      Order.before,
      RMLITERALGRPEXP)) { session =>
      session.conf.set("spark.sql.optimizer.excludedRules", excludeRules)
      val rules = Seq(RMLITERALGRPEXP, RMREPGRPEXP)
      val batch = session.sessionState.optimizer.batches.find(_.name.equals("Aggregate"))
      assert(batch.isDefined)
      assert(rules.equals(batch.get.rules.map(_.ruleName)))
      session.conf.set("spark.sql.optimizer.excludedRules", "")
      val newBatch = session.sessionState.optimizer.batches.find(_.name.equals("Aggregate"))
      assert(newBatch.isDefined)
      val newRules = Seq("org.apache.spark.sql.MyRule1", RMLITERALGRPEXP, RMREPGRPEXP)
      assert(newRules.equals(newBatch.get.rules.map(_.ruleName)))
    }
  }

  test("inject spark planner strategy") {
    withSession(_.injectPlannerStrategy(MySparkStrategy)) { session =>
      assert(session.sessionState.planner.strategies.contains(MySparkStrategy(session)))
    }
  }

  test("inject parser") {
    val extension = create { extensions =>
      extensions.injectParser((_, _) => CatalystSqlParser)
    }
    withSession(extension) { session =>
      assert(session.sessionState.sqlParser == CatalystSqlParser)
    }
  }

  test("inject stacked parsers") {
    val extension = create { extensions =>
      extensions.injectParser((_, _) => CatalystSqlParser)
      extensions.injectParser(MyParser)
      extensions.injectParser(MyParser)
    }
    withSession(extension) { session =>
      val parser = MyParser(session, MyParser(session, CatalystSqlParser))
      assert(session.sessionState.sqlParser == parser)
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

  test("use custom class for extensions") {
    val session = SparkSession.builder()
      .master("local[1]")
      .config("spark.sql.extensions", classOf[MyExtensions].getCanonicalName)
      .getOrCreate()
    try {
      assert(session.sessionState.planner.strategies.contains(MySparkStrategy(session)))
      assert(session.sessionState.analyzer.extendedResolutionRules.contains(MyRule(session)))
      assert(session.sessionState.functionRegistry
        .lookupFunction(MyExtensions.myFunction._1).isDefined)
      val batch = session.sessionState.optimizer.batches.find(_.name.equals("Aggregate"))
      assert(batch.isDefined)
      assert(batch.get.rules.find(_.ruleName.equals("org.apache.spark.sql.MyRule1")).isDefined)
      assert(batch.get.rules.find(_.ruleName.equals("org.apache.spark.sql.MyRule")).isDefined)
    } finally {
      stop(session)
    }
  }
}

case class MyRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan
}

case class MyRule1(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan
}

case class MyRule2(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan
}

case class MyRule3(spark: SparkSession) extends Rule[LogicalPlan] {
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

  override def parseTableSchema(sqlText: String): StructType =
    delegate.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType =
    delegate.parseDataType(sqlText)
}

object MyExtensions {

  val myFunction = (FunctionIdentifier("myFunction"),
    new ExpressionInfo("noClass", "myDb", "myFunction", "usage", "extended usage" ),
    (myArgs: Seq[Expression]) => Literal(5, IntegerType))
}

class MyExtensions extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectPlannerStrategy(MySparkStrategy)
    e.injectResolutionRule(MyRule)
    e.injectFunction(MyExtensions.myFunction)
    e.injectOptimizerRuleInOrder(
      MyRule1,
      "Aggregate",
      Order.after,
      "org.apache.spark.sql.catalyst.optimizer.RemoveLiteralFromGroupExpressions")
    e.injectOptimizerRuleInOrder(
      MyRule,
      "Aggregate",
      Order.after,
      "org.apache.spark.sql.catalyst.optimizer.RemoveLiteralFromGroupExpressions")
  }
}
