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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalLimit, LocalRelation}

class PushProjectThroughLimitSuite extends PlanTest {

  test("push redundant alias through limit") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = relation.select('a as 'b).limit(1).select('b as 'c).analyze
    val optimized = PushProjectThroughLimit.apply(query)
    val expected = relation.select('a as 'b).select('b as 'c).limit(1).analyze
    comparePlans(optimized, expected)
  }

  test("push redundant alias through local limit") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = LocalLimit(1, relation.select('a as 'b)).select('b as 'c).analyze
    val optimized = PushProjectThroughLimit.apply(query)
    val expected = LocalLimit(1, relation.select('a as 'b).select('b as 'c)).analyze
    comparePlans(optimized, expected)
  }

  test("push no-op project through local limit") {
    val relation = LocalRelation('a.int, 'b.int)
    val query = LocalLimit(1, relation).select('a, 'b).analyze
    val optimized = PushProjectThroughLimit.apply(query)
    val expected = LocalLimit(1, relation.select('a, 'b)).analyze
    comparePlans(optimized, expected)
  }
}
