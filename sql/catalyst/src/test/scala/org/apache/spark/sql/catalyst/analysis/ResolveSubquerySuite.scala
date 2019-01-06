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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{InSubquery, ListQuery}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, Project}

/**
 * Unit tests for [[ResolveSubquery]].
 */
class ResolveSubquerySuite extends AnalysisTest {

  val a = 'a.int
  val b = 'b.int
  val t1 = LocalRelation(a)
  val t2 = LocalRelation(b)

  test("SPARK-17251 Improve `OuterReference` to be `NamedExpression`") {
    val expr = Filter(
      InSubquery(Seq(a), ListQuery(Project(Seq(UnresolvedAttribute("a")), t2))), t1)
    val m = intercept[AnalysisException] {
      SimpleAnalyzer.checkAnalysis(SimpleAnalyzer.ResolveSubquery(expr))
    }.getMessage
    assert(m.contains(
      "Expressions referencing the outer query are not supported outside of WHERE/HAVING clauses"))
  }
}
