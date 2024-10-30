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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{SparkSession, SparkSessionExtensionsProvider}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, RangeExec, SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

class AdaptiveRuleContextSuite extends SparkFunSuite with AdaptiveSparkPlanHelper {

  private def stop(spark: SparkSession): Unit = {
    spark.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  private def withSession(
      builders: Seq[SparkSessionExtensionsProvider])(f: SparkSession => Unit): Unit = {
    val builder = SparkSession.builder().master("local[1]")
    builders.foreach(builder.withExtensions)
    val spark = builder.getOrCreate()
    try f(spark) finally {
      stop(spark)
    }
  }

  test("test adaptive rule context") {
    withSession(
      Seq(_.injectRuntimeOptimizerRule(_ => MyRuleContextForRuntimeOptimization),
        _.injectPlannerStrategy(_ => MyRuleContextForPlannerStrategy),
        _.injectQueryPostPlannerStrategyRule(_ => MyRuleContextForPostPlannerStrategyRule),
        _.injectQueryStagePrepRule(_ => MyRuleContextForPreQueryStageRule),
        _.injectQueryStageOptimizerRule(_ => MyRuleContextForQueryStageRule),
        _.injectColumnar(_ => MyRuleContextForColumnarRule))) { spark =>
      val df = spark.range(1, 10, 1, 3).selectExpr("id % 3 as c").groupBy("c").count()
      df.collect()
      assert(collectFirst(df.queryExecution.executedPlan) {
        case s: ShuffleExchangeExec if s.numPartitions == 2 => s
      }.isDefined)
    }
  }

  test("test adaptive rule context with subquery") {
    withSession(
      Seq(_.injectQueryStagePrepRule(_ => MyRuleContextForQueryStageWithSubquery))) { spark =>
      spark.sql("select (select count(*) from range(10)), id from range(10)").collect()
    }
  }
}

object MyRuleContext {
  def checkAndGetRuleContext(): AdaptiveRuleContext = {
    val ruleContextOpt = AdaptiveRuleContext.get()
    assert(ruleContextOpt.isDefined)
    ruleContextOpt.get
  }

  def checkRuleContextForQueryStage(plan: SparkPlan): SparkPlan = {
    val ruleContext = checkAndGetRuleContext()
    assert(!ruleContext.isSubquery)
    val stage = plan.find(_.isInstanceOf[ShuffleQueryStageExec])
    if (stage.isDefined && stage.get.asInstanceOf[ShuffleQueryStageExec].isMaterialized) {
      assert(ruleContext.isFinalStage)
      assert(!ruleContext.configs().get("spark.sql.shuffle.partitions").contains("2"))
    } else {
      assert(!ruleContext.isFinalStage)
      assert(ruleContext.configs().get("spark.sql.shuffle.partitions").contains("2"))
    }
    plan
  }
}

object MyRuleContextForRuntimeOptimization extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    MyRuleContext.checkAndGetRuleContext()
    plan
  }
}

object MyRuleContextForPlannerStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case _: LogicalQueryStage =>
        val ruleContext = MyRuleContext.checkAndGetRuleContext()
        assert(!ruleContext.configs().get("spark.sql.shuffle.partitions").contains("2"))
        Nil
      case _ => Nil
    }
  }
}

object MyRuleContextForPostPlannerStrategyRule extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    val ruleContext = MyRuleContext.checkAndGetRuleContext()
    if (plan.find(_.isInstanceOf[RangeExec]).isDefined) {
      ruleContext.setConfig("spark.sql.shuffle.partitions", "2")
    }
    plan
  }
}

object MyRuleContextForPreQueryStageRule extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    val ruleContext = MyRuleContext.checkAndGetRuleContext()
    assert(!ruleContext.isFinalStage)
    plan
  }
}

object MyRuleContextForQueryStageRule extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    MyRuleContext.checkRuleContextForQueryStage(plan)
  }
}

object MyRuleContextForColumnarRule extends ColumnarRule {
  override def preColumnarTransitions: Rule[SparkPlan] = {
    plan: SparkPlan => {
      if (plan.isInstanceOf[AdaptiveSparkPlanExec]) {
        // skip if we are not inside AQE
        assert(AdaptiveRuleContext.get().isEmpty)
        plan
      } else {
        MyRuleContext.checkRuleContextForQueryStage(plan)
      }
    }
  }

  override def postColumnarTransitions: Rule[SparkPlan] = {
    plan: SparkPlan => {
      if (plan.isInstanceOf[AdaptiveSparkPlanExec]) {
        // skip if we are not inside AQE
        assert(AdaptiveRuleContext.get().isEmpty)
        plan
      } else {
        MyRuleContext.checkRuleContextForQueryStage(plan)
      }
    }
  }
}

object MyRuleContextForQueryStageWithSubquery extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    val ruleContext = MyRuleContext.checkAndGetRuleContext()
    if (plan.exists(_.isInstanceOf[HashAggregateExec])) {
      assert(ruleContext.isSubquery)
      if (plan.exists(_.isInstanceOf[RangeExec])) {
        assert(!ruleContext.isFinalStage)
      } else {
        assert(ruleContext.isFinalStage)
      }
    } else {
      assert(!ruleContext.isSubquery)
    }
    plan
  }
}
