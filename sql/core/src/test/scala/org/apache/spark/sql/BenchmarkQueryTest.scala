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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeFormatter, CodeGenerator}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.{SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

abstract class BenchmarkQueryTest extends QueryTest with SharedSQLContext with BeforeAndAfterAll {

  // When Utils.isTesting is true, the RuleExecutor will issue an exception when hitting
  // the max iteration of analyzer/optimizer batches.
  assert(Utils.isTesting, s"${IS_TESTING.key} is not set to true")

  /**
   * Drop all the tables
   */
  protected override def afterAll(): Unit = {
    try {
      // For debugging dump some statistics about how much time was spent in various optimizer rules
      logWarning(RuleExecutor.dumpTimeSpent())
      spark.sessionState.catalog.reset()
    } finally {
      super.afterAll()
    }
  }

  override def beforeAll() {
    super.beforeAll()
    RuleExecutor.resetMetrics()
  }

  protected def checkGeneratedCode(plan: SparkPlan): Unit = {
    val codegenSubtrees = new collection.mutable.HashSet[WholeStageCodegenExec]()
    plan foreach {
      case s: WholeStageCodegenExec =>
        codegenSubtrees += s
      case _ =>
    }
    codegenSubtrees.toSeq.foreach { subtree =>
      val code = subtree.doCodeGen()._2
      try {
        // Just check the generated code can be properly compiled
        CodeGenerator.compile(code)
      } catch {
        case e: Exception =>
          val msg =
            s"""
               |failed to compile:
               |Subtree:
               |$subtree
               |Generated code:
               |${CodeFormatter.format(code)}
             """.stripMargin
          throw new Exception(msg, e)
      }
    }
  }
}
