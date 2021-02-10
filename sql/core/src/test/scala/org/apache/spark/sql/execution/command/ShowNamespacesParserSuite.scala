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
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.ShowNamespaces
import org.apache.spark.sql.test.SharedSparkSession

class ShowNamespacesParserSuite extends AnalysisTest with SharedSparkSession {
  test("all namespaces") {
    Seq("SHOW NAMESPACES", "SHOW DATABASES").foreach { sqlCmd =>
      comparePlans(
        parsePlan(sqlCmd),
        ShowNamespaces(UnresolvedNamespace(Seq.empty[String]), None))
    }
  }

  test("basic pattern") {
    Seq(
      "SHOW DATABASES LIKE 'defau*'",
      "SHOW NAMESPACES LIKE 'defau*'").foreach { sqlCmd =>
      comparePlans(
        parsePlan(sqlCmd),
        ShowNamespaces(UnresolvedNamespace(Seq.empty[String]), Some("defau*")))
    }
  }

  test("FROM/IN operator is not allowed by SHOW DATABASES") {
    Seq(
      "SHOW DATABASES FROM testcat.ns1.ns2",
      "SHOW DATABASES IN testcat.ns1.ns2").foreach { sqlCmd =>
      val errMsg = intercept[ParseException] {
        parsePlan(sqlCmd)
      }.getMessage
      assert(errMsg.contains("FROM/IN operator is not allowed in SHOW DATABASES"))
    }
  }

  test("show namespaces in/from a namespace") {
    comparePlans(
      parsePlan("SHOW NAMESPACES FROM testcat.ns1.ns2"),
      ShowNamespaces(UnresolvedNamespace(Seq("testcat", "ns1", "ns2")), None))
    comparePlans(
      parsePlan("SHOW NAMESPACES IN testcat.ns1.ns2"),
      ShowNamespaces(UnresolvedNamespace(Seq("testcat", "ns1", "ns2")), None))
  }

  test("namespaces by a pattern from another namespace") {
    comparePlans(
      parsePlan("SHOW NAMESPACES IN testcat.ns1 LIKE '*pattern*'"),
      ShowNamespaces(UnresolvedNamespace(Seq("testcat", "ns1")), Some("*pattern*")))
  }
}
