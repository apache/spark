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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.logical._

class SubstituteHintsSuite extends AnalysisTest {
  import org.apache.spark.sql.catalyst.analysis.TestRelations._

  val a = testRelation.output(0)
  val b = testRelation2.output(0)

  test("case-sensitive or insensitive parameters") {
    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE"), table("TaBlE")),
      BroadcastHint(testRelation),
      caseSensitive = false)

    checkAnalysis(
      Hint("MAPJOIN", Seq("table"), table("TaBlE")),
      BroadcastHint(testRelation),
      caseSensitive = false)

    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE"), table("TaBlE")),
      BroadcastHint(testRelation))

    checkAnalysis(
      Hint("MAPJOIN", Seq("table"), table("TaBlE")),
      testRelation)
  }

  test("single hint") {
    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE"), table("TaBlE").select(a)),
      BroadcastHint(testRelation).select(a))

    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE"), table("TaBlE").as("t").join(table("TaBlE2").as("u")).select(a)),
      BroadcastHint(testRelation).join(testRelation2).select(a))

    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE2"),
        table("TaBlE").as("t").join(table("TaBlE2").as("u")).select(a)),
      testRelation.join(BroadcastHint(testRelation2)).select(a))
  }

  test("single hint with multiple parameters") {
    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE", "TaBlE"),
        table("TaBlE").as("t").join(table("TaBlE2").as("u")).select(a)),
      BroadcastHint(testRelation).join(testRelation2).select(a))

    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE", "TaBlE2"),
        table("TaBlE").as("t").join(table("TaBlE2").as("u")).select(a)),
      BroadcastHint(testRelation).join(BroadcastHint(testRelation2)).select(a))
  }

  test("duplicated nested hints are transformed into one") {
    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE"),
        Hint("MAPJOIN", Seq("TaBlE"), table("TaBlE").as("t").select('a))
          .join(table("TaBlE2").as("u")).select(a)),
      BroadcastHint(testRelation).select(a).join(testRelation2).select(a))

    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE2"),
        table("TaBlE").as("t").select(a)
          .join(Hint("MAPJOIN", Seq("TaBlE2"), table("TaBlE2").as("u").select(b))).select(a)),
      testRelation.select(a).join(BroadcastHint(testRelation2).select(b)).select(a))
  }

  test("distinct nested two hints are handled separately") {
    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE2"),
        Hint("MAPJOIN", Seq("TaBlE"), table("TaBlE").as("t").select(a))
          .join(table("TaBlE2").as("u")).select(a)),
      BroadcastHint(testRelation).select(a).join(BroadcastHint(testRelation2)).select(a))

    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE"),
        table("TaBlE").as("t")
          .join(Hint("MAPJOIN", Seq("TaBlE2"), table("TaBlE2").as("u").select(b))).select(a)),
      BroadcastHint(testRelation).join(BroadcastHint(testRelation2).select(b)).select(a))
  }

  test("deep self join") {
    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE"),
        table("TaBlE").join(table("TaBlE")).join(table("TaBlE")).join(table("TaBlE")).select(a)),
      BroadcastHint(testRelation).join(testRelation).join(testRelation).join(testRelation)
        .select(a))
  }

  test("subquery should be ignored") {
    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE"),
        table("TaBlE").select(a).as("x").join(table("TaBlE")).select(a)),
      testRelation.select(a).join(BroadcastHint(testRelation)).select(a))

    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE"),
        table("TaBlE").as("t").select(a).as("x")
          .join(table("TaBlE2").as("t2")).select(a)),
      testRelation.select(a).join(testRelation2).select(a))
  }
}
