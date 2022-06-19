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

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedTableOrView}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.plans.logical.DescribeRelation

class DescribeTableParserSuite extends AnalysisTest {
  test("SPARK-17328: Fix NPE with EXPLAIN DESCRIBE TABLE") {
    comparePlans(parsePlan("describe t"),
      DescribeRelation(
        UnresolvedTableOrView(Seq("t"), "DESCRIBE TABLE", true), Map.empty, isExtended = false))
    comparePlans(parsePlan("describe table t"),
      DescribeRelation(
        UnresolvedTableOrView(Seq("t"), "DESCRIBE TABLE", true), Map.empty, isExtended = false))
    comparePlans(parsePlan("describe table extended t"),
      DescribeRelation(
        UnresolvedTableOrView(Seq("t"), "DESCRIBE TABLE", true), Map.empty, isExtended = true))
    comparePlans(parsePlan("describe table formatted t"),
      DescribeRelation(
        UnresolvedTableOrView(Seq("t"), "DESCRIBE TABLE", true), Map.empty, isExtended = true))
  }
}
