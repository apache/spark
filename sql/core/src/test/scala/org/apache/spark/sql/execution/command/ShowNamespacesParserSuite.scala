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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedNamespace}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.plans.logical.ShowNamespaces
import org.apache.spark.sql.test.SharedSparkSession

class ShowNamespacesParserSuite extends AnalysisTest with SharedSparkSession {
  private val keywords = Seq("NAMESPACES", "DATABASES", "SCHEMAS")

  test("show namespaces in the current catalog") {
    keywords.foreach { keyword =>
      comparePlans(
        parsePlan(s"SHOW $keyword"),
        ShowNamespaces(UnresolvedNamespace(Seq.empty[String]), None))
    }
  }

  test("show namespaces with a pattern") {
    keywords.foreach { keyword =>
      comparePlans(
        parsePlan(s"SHOW $keyword LIKE 'defau*'"),
        ShowNamespaces(UnresolvedNamespace(Seq.empty[String]), Some("defau*")))
      // LIKE can be omitted.
      comparePlans(
        parsePlan(s"SHOW $keyword 'defau*'"),
        ShowNamespaces(UnresolvedNamespace(Seq.empty[String]), Some("defau*")))
    }
  }

  test("show namespaces in/from a namespace") {
    keywords.foreach { keyword =>
      comparePlans(
        parsePlan(s"SHOW $keyword FROM testcat.ns1.ns2"),
        ShowNamespaces(UnresolvedNamespace(Seq("testcat", "ns1", "ns2")), None))
      comparePlans(
        parsePlan(s"SHOW $keyword IN testcat.ns1.ns2"),
        ShowNamespaces(UnresolvedNamespace(Seq("testcat", "ns1", "ns2")), None))
    }
  }

  test("namespaces by a pattern from another namespace") {
    keywords.foreach { keyword =>
      comparePlans(
        parsePlan(s"SHOW $keyword IN testcat.ns1 LIKE '*pattern*'"),
        ShowNamespaces(UnresolvedNamespace(Seq("testcat", "ns1")), Some("*pattern*")))
    }
  }
}
