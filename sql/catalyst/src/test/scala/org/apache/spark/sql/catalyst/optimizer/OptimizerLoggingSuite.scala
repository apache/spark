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

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.mutable.ArrayBuffer

import org.apache.log4j.{Appender, AppenderSkeleton, Level, Logger}
import org.apache.log4j.spi.LoggingEvent

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf

class OptimizerLoggingSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Optimizer Batch", FixedPoint(100),
        PushDownPredicate, ColumnPruning, CollapseProject) ::
      Batch("Batch Has No Effect", Once,
        ColumnPruning) :: Nil
  }

  class MockAppender extends AppenderSkeleton {
    val loggingEvents = new ArrayBuffer[LoggingEvent]()

    override def append(loggingEvent: LoggingEvent): Unit = {
      if (loggingEvent.getRenderedMessage().contains("Applying Rule") ||
        loggingEvent.getRenderedMessage().contains("Result of Batch") ||
        loggingEvent.getRenderedMessage().contains("has no effect")) {
        loggingEvents.append(loggingEvent)
      }
    }

    override def close(): Unit = {}
    override def requiresLayout(): Boolean = false
  }

  private def withLogLevelAndAppender(level: Level, appender: Appender)(f: => Unit): Unit = {
    val logger = Logger.getLogger(Optimize.getClass.getName.dropRight(1))
    val restoreLevel = logger.getLevel
    logger.setLevel(level)
    logger.addAppender(appender)
    try f finally {
      logger.setLevel(restoreLevel)
      logger.removeAppender(appender)
    }
  }

  private def verifyLog(expectedLevel: Level, expectedRulesOrBatches: Seq[String]): Unit = {
    val logAppender = new MockAppender()
    withLogAppender(logAppender,
        loggerName = Some(Optimize.getClass.getName.dropRight(1)), level = Some(Level.TRACE)) {
      val input = LocalRelation('a.int, 'b.string, 'c.double)
      val query = input.select('a, 'b).select('a).where('a > 1).analyze
      val expected = input.where('a > 1).select('a).analyze
      comparePlans(Optimize.execute(query), expected)
    }
    val logMessages = logAppender.loggingEvents.map(_.getRenderedMessage)
    assert(expectedRulesOrBatches.forall
    (ruleOrBatch => logMessages.exists(_.contains(ruleOrBatch))))
    assert(logAppender.loggingEvents.forall(_.getLevel == expectedLevel))
  }

  test("test log level") {
    val levels = Seq(
      "TRACE" -> Level.TRACE,
      "trace" -> Level.TRACE,
      "DEBUG" -> Level.DEBUG,
      "debug" -> Level.DEBUG,
      "INFO" -> Level.INFO,
      "info" -> Level.INFO,
      "WARN" -> Level.WARN,
      "warn" -> Level.WARN,
      "ERROR" -> Level.ERROR,
      "error" -> Level.ERROR,
      "deBUG" -> Level.DEBUG)

    levels.foreach { level =>
      withSQLConf(SQLConf.OPTIMIZER_PLAN_CHANGE_LOG_LEVEL.key -> level._1) {
        verifyLog(
          level._2,
          Seq(
            PushDownPredicate.ruleName,
            ColumnPruning.ruleName,
            CollapseProject.ruleName))
      }
    }
  }

  test("test invalid log level conf") {
    val levels = Seq(
      "",
      "*d_",
      "infoo")

    levels.foreach { level =>
      val error = intercept[IllegalArgumentException] {
        withSQLConf(SQLConf.OPTIMIZER_PLAN_CHANGE_LOG_LEVEL.key -> level) {}
      }
      assert(error.getMessage.contains(
        "Invalid value for 'spark.sql.optimizer.planChangeLog.level'."))
    }
  }

  test("test log rules") {
    val rulesSeq = Seq(
      Seq(PushDownPredicate.ruleName,
        ColumnPruning.ruleName,
        CollapseProject.ruleName).reduce(_ + "," + _) ->
        Seq(PushDownPredicate.ruleName,
          ColumnPruning.ruleName,
          CollapseProject.ruleName),
      Seq(PushDownPredicate.ruleName,
        ColumnPruning.ruleName).reduce(_ + "," + _) ->
        Seq(PushDownPredicate.ruleName,
          ColumnPruning.ruleName),
      CollapseProject.ruleName ->
        Seq(CollapseProject.ruleName),
      Seq(ColumnPruning.ruleName,
        "DummyRule").reduce(_ + "," + _) ->
        Seq(ColumnPruning.ruleName),
      "DummyRule" -> Seq(),
      "" -> Seq()
    )

    rulesSeq.foreach { case (rulesConf, expectedRules) =>
      withSQLConf(
        SQLConf.OPTIMIZER_PLAN_CHANGE_LOG_RULES.key -> rulesConf,
        SQLConf.OPTIMIZER_PLAN_CHANGE_LOG_LEVEL.key -> "INFO") {
        verifyLog(Level.INFO, expectedRules)
      }
    }
  }

  test("test log batches which change the plan") {
    withSQLConf(
      SQLConf.OPTIMIZER_PLAN_CHANGE_LOG_BATCHES.key -> "Optimizer Batch",
      SQLConf.OPTIMIZER_PLAN_CHANGE_LOG_LEVEL.key -> "INFO") {
      verifyLog(Level.INFO, Seq("Optimizer Batch"))
    }
  }

  test("test log batches which do not change the plan") {
    withSQLConf(
      SQLConf.OPTIMIZER_PLAN_CHANGE_LOG_BATCHES.key -> "Batch Has No Effect",
      SQLConf.OPTIMIZER_PLAN_CHANGE_LOG_LEVEL.key -> "INFO") {
      verifyLog(Level.INFO, Seq("Batch Has No Effect"))
    }
  }
}
