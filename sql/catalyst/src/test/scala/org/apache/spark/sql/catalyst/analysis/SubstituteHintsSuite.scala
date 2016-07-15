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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._

class SubstituteHintsSuite extends AnalysisTest {
  import org.apache.spark.sql.catalyst.analysis.TestRelations._

  test("case-sensitive or insensitive parameters") {
    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE"), UnresolvedRelation(TableIdentifier("TaBlE"), Some("TbL"))),
      BroadcastHint(testRelation),
      caseSensitive = false)

    checkAnalysis(
      Hint("MAPJOIN", Seq("table"), UnresolvedRelation(TableIdentifier("TaBlE"), Some("TbL"))),
      BroadcastHint(testRelation),
      caseSensitive = false)

    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE"), UnresolvedRelation(TableIdentifier("TaBlE"), Some("TbL"))),
      BroadcastHint(testRelation))

    checkAnalysis(
      Hint("MAPJOIN", Seq("table"), UnresolvedRelation(TableIdentifier("TaBlE"), Some("TbL"))),
      testRelation)
  }

  test("single hint") {
    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE"),
        Project(Seq(UnresolvedAttribute("a")),
          UnresolvedRelation(TableIdentifier("TaBlE"), Some("TbL")))),
      Project(testRelation.output, BroadcastHint(testRelation)))

    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE"),
        Project(Seq(UnresolvedAttribute("t.a")),
          Join(
            UnresolvedRelation(TableIdentifier("TaBlE"), Some("TbL")).as("t"),
            UnresolvedRelation(TableIdentifier("TaBlE2"), Some("TbL")).as("u"),
            Inner,
            None
          ))),
      Project(testRelation.output,
        Join(
          BroadcastHint(testRelation),
          testRelation2,
          Inner,
          None
        )))

    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE2"),
        Project(Seq(UnresolvedAttribute("t.a")),
          Join(
            UnresolvedRelation(TableIdentifier("TaBlE"), Some("TbL")).as("t"),
            UnresolvedRelation(TableIdentifier("TaBlE2"), Some("TbL")).as("u"),
            Inner,
            None
          ))),
      Project(testRelation.output,
        Join(
          testRelation,
          BroadcastHint(testRelation2),
          Inner,
          None
        )))
  }

  test("single hint with multiple parameters") {
    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE", "TaBlE"),
        Project(Seq(UnresolvedAttribute("t.a")),
          Join(
            UnresolvedRelation(TableIdentifier("TaBlE"), Some("TbL")).as("t"),
            UnresolvedRelation(TableIdentifier("TaBlE2"), Some("TbL")).as("u"),
            Inner,
            None
          ))),
      Project(testRelation.output,
        Join(
          BroadcastHint(testRelation),
          testRelation2,
          Inner,
          None
        )))

    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE", "TaBlE2"),
        Project(Seq(UnresolvedAttribute("t.a")),
          Join(
            UnresolvedRelation(TableIdentifier("TaBlE"), Some("TbL")).as("t"),
            UnresolvedRelation(TableIdentifier("TaBlE2"), Some("TbL")).as("u"),
            Inner,
            None
          ))),
      Project(testRelation.output,
        Join(
          BroadcastHint(testRelation),
          BroadcastHint(testRelation2),
          Inner,
          None
        )))
  }

  test("duplicated nested hints are transformed into one") {
    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE"),
        Project(Seq(UnresolvedAttribute("t.a")),
          Join(
            Hint("MAPJOIN", Seq("TaBlE"),
              Project(Seq(UnresolvedAttribute("t.a")),
                UnresolvedRelation(TableIdentifier("TaBlE"), Some("TbL")).as("t"))),
            UnresolvedRelation(TableIdentifier("TaBlE2"), Some("TbL")).as("u"),
            Inner,
            None
          ))),
      Project(testRelation.output,
        Join(
          Project(testRelation.output,
            BroadcastHint(testRelation)),
          testRelation2,
          Inner,
          None
        )))

    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE2"),
        Project(Seq(UnresolvedAttribute("t.a")),
          Join(
            Project(Seq(UnresolvedAttribute("t.a")),
              UnresolvedRelation(TableIdentifier("TaBlE"), Some("TbL")).as("t")),
            Hint("MAPJOIN", Seq("TaBlE2"),
              Project(Seq(UnresolvedAttribute("u.a")),
                UnresolvedRelation(TableIdentifier("TaBlE2"), Some("TbL")).as("u"))),
            Inner,
            None
          ))),
      Project(testRelation.output,
        Join(
          Project(testRelation.output, testRelation),
          Project(Seq(testRelation2.output(0)), BroadcastHint(testRelation2)),
          Inner,
          None
        )))
  }

  test("distinct nested two hints are handled separately") {
    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE2"),
        Project(Seq(UnresolvedAttribute("t.a")),
          Join(
            Hint("MAPJOIN", Seq("TaBlE"),
              Project(Seq(UnresolvedAttribute("t.a")),
                UnresolvedRelation(TableIdentifier("TaBlE"), Some("TbL")).as("t"))),
            UnresolvedRelation(TableIdentifier("TaBlE2"), Some("TbL")).as("u"),
            Inner,
            None
          ))),
      Project(testRelation.output,
        Join(
          Project(testRelation.output,
            BroadcastHint(testRelation)),
          BroadcastHint(testRelation2),
          Inner,
          None
        )))

    checkAnalysis(
      Hint("MAPJOIN", Seq("TaBlE"),
        Project(Seq(UnresolvedAttribute("t.a")),
          Join(
            UnresolvedRelation(TableIdentifier("TaBlE"), Some("TbL")).as("t"),
            Hint("MAPJOIN", Seq("TaBlE2"),
              Project(Seq(UnresolvedAttribute("u.a")),
                UnresolvedRelation(TableIdentifier("TaBlE2"), Some("TbL")).as("u"))),
            Inner,
            None
          ))),
      Project(testRelation.output,
        Join(
          BroadcastHint(testRelation),
          Project(Seq(testRelation2.output(0)), BroadcastHint(testRelation2)),
          Inner,
          None
        )))
  }
}
