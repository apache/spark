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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.EmptyFunctionRegistry
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.ListQuery
import org.apache.spark.sql.catalyst.plans.{LeftSemi, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.internal.SQLConf



class RewriteSubquerySuite extends PlanTest {
  object Optimize extends Optimizer(
    new SessionCatalog(
      new InMemoryCatalog,
      EmptyFunctionRegistry,
      new SQLConf()))


  test("Column pruning after rewriting predicate subquery") {
      val schema1 = LocalRelation('a.int, 'b.int)
      val schema2 = LocalRelation('x.int, 'y.int, 'z.int)

      val relation = LocalRelation.fromExternalRows(schema1.output, Seq(Row(1, 1)))
      val relInSubquery = LocalRelation.fromExternalRows(schema2.output, Seq(Row(1, 1, 1)))

      val query = relation.where('a.in(ListQuery(relInSubquery.select('x)))).select('a)

      val optimized = Optimize.execute(query.analyze)

      val correctAnswer = relation
        .select('a)
        .join(relInSubquery.select('x), LeftSemi, Some('a === 'x))
        .analyze

      comparePlans(optimized, Optimize.execute(correctAnswer))
  }

}
