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

import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.test.SharedSparkSession

class DataFrameTransposeSuite extends QueryTest with SharedSparkSession {
  // scalastyle:off println
  test("logical plan transformation for transpose") {
    val transposePlan = Transpose(
      firstColumnValues = Seq(Literal("dotNET"), Literal("Java")),
      child = courseSalesDedup.queryExecution.logical
    )

    println(s"Logical plan before analysis:\n$transposePlan")

    val testAnalyzer = new Analyzer(
      new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin))
    val analyzedPlan = testAnalyzer.execute(transposePlan)

    println(s"Logical plan after analysis:\n$analyzedPlan")
  }

  test("transpose scala API") {
    println(s"df: \n${courseSalesDedup.show()}")
    val transposed = courseSalesDedup.transpose(Seq("dotNET", "Java"))
    println(s"transposed df: \n${transposed.show()}")
  }
}
