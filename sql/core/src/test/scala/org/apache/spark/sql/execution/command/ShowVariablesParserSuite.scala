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

import org.apache.spark.sql.catalyst.analysis.AnalysisTest
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.test.SharedSparkSession

class ShowVariablesParserSuite extends AnalysisTest with SharedSparkSession {

  private lazy val parser = new SparkSqlParser()

  private def parsePlan(sqlText: String): LogicalPlan = {
    parser.parsePlan(sqlText)
  }

  test("show variables") {
    comparePlans(parsePlan("SHOW VARIABLES"), ShowVariablesCommand(None))
    comparePlans(parsePlan("SHOW VARIABLES LIKE '%name%'"), ShowVariablesCommand(Some("%name%")))
  }
}
