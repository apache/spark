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

package org.apache.spark.sql.catalyst.parser

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.plans.logical.{Project, SubqueryAlias}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class StreamRelationParserSuite extends AnalysisTest {
  import CatalystSqlParser._

  test("STREAM parses correctly on table identifier") {
    Seq("SELECT * FROM STREAM(t)", "SELECT * FROM STREAM t").foreach { query =>
      val plan = parsePlan(query)
      comparePlans(
        plan,
        Project(
          projectList = Seq(UnresolvedStar(None)),
          child = UnresolvedRelation(
            multipartIdentifier = Seq("t"),
            isStreaming = true
          )
        )
      )
    }
  }

  test("STREAM with alias is parsed correctly") {
    Seq(
      "SELECT * FROM STREAM(t) AS `a.b.c`",
      "SELECT * FROM STREAM t AS `a.b.c`"
    ).foreach { query =>
      val plan = parsePlan(query)
      comparePlans(
        plan,
        Project(
          projectList = Seq(UnresolvedStar(None)),
          child = SubqueryAlias(
            identifier = AliasIdentifier(
              name = "a.b.c",
              qualifier = Seq.empty
            ),
            child = UnresolvedRelation(
              multipartIdentifier = Seq("t"),
              isStreaming = true
            )
          )
        )
      )
    }
  }

  test("Parse Exceptions: Unsupported STREAM relations") {
    // Sub-queries within STREAM keyword
    interceptParseException(parsePlan)("SELECT * FROM STREAM ( SELECT * FROM t3 )")(None)
    // Temporal Clause within STREAM keyword
    interceptParseException(parsePlan)(
      "SELECT * FROM STREAM ( t1 TIMESTAMP AS OF current_date() )"
    )(None)
  }

  test("STREAM parses WITH options") {
    comparePlans(
      parsePlan("SELECT * FROM STREAM table WITH ('key'='value')"),
      Project(
        projectList = Seq(UnresolvedStar(None)),
        child = UnresolvedRelation(
          multipartIdentifier = Seq("table"),
          options = new CaseInsensitiveStringMap(Map("key" -> "value").asJava),
          isStreaming = true
        )
      )
    )
  }
}
