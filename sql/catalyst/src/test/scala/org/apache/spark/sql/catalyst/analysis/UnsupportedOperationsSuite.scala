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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.TestRelations._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class UnsupportedOperationsSuite extends SparkFunSuite {

  testSupported(
    "local relation",
    testRelation,
    forIncremental = false)

  testNonSupported(
    "streaming source",
    testStreamingRelation,
    forIncremental = false,
    Seq("startStream", "streaming", "source"))

  testNonSupported(
    "select on streaming source",
    testStreamingRelation.select($"count(*)"),
    forIncremental = false,
    "startStream" :: "streaming" :: "source" :: Nil)

  testNotSupportedForStreaming(
    "stream-stream join",
    testStreamingRelation.join(testStreamingRelation, condition = Some(Literal(1))),
    "Joining between two streaming" :: Nil)

  testSupportedForStreaming(
    "stream-batch join",
    testStreamingRelation.join(testRelation, condition = Some(Literal(1))))

  testSupportedForStreaming(
    "batch-stream join",
    testRelation.join(testStreamingRelation, condition = Some(Literal(1))))

  testSupportedForStreaming(
    "batch-batch join",
    testRelation.join(testRelation, condition = Some(Literal(1))))


  def testNotSupportedForStreaming(
    name: String,
    plan: LogicalPlan,
    expectedMsgs: Seq[String] = Nil): Unit = {
    testNonSupported(
      name,
      plan,
      forIncremental = true,
      expectedMsgs :+ "streaming" :+ "DataFrame" :+ "Dataset" :+ "not supported")
  }

  def testSupportedForStreaming(
    name: String,
    plan: LogicalPlan): Unit = {
    testSupported(name, plan, forIncremental = true)
  }

  def testNonSupported(
    name: String,
    plan: LogicalPlan,
    forIncremental: Boolean,
    expectedMsgs: Seq[String]): Unit = {
    val testName = if (forIncremental) {
      s"streaming plan - $name"
    } else {
      s"batch plan - $name"
    }

    test(testName) {
      val e = intercept[AnalysisException] {
        UnsupportedOperationChecker.check(plan, forIncremental)
      }

      if (!expectedMsgs.map(_.toLowerCase).forall(e.getMessage.toLowerCase.contains)) {
        fail(
          s"""Exception message should contain the following substrings:
           |
           |  ${expectedMsgs.mkString("\n  ")}
           |
           |Actual exception message:
           |
           |  ${e.getMessage}
         """.stripMargin)
      }
    }
  }

  def testSupported(
      name: String,
      plan: LogicalPlan,
      forIncremental: Boolean): Unit = {
    val testName = if (forIncremental) {
      s"incremental plan - $name"
    } else {
      s"non-incremental plan - $name"
    }

    test(testName) {
      UnsupportedOperationChecker.check(plan, forIncremental) // should not throw exception
    }
  }
}
