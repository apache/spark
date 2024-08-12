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

package org.apache.spark.sql.connector

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.execution.datasources.v2.PushablePredicate
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class PushablePredicateSuite extends QueryTest with SharedSparkSession {

  test("PushablePredicate None returned - flag on") {
    withSQLConf(SQLConf.DATA_SOURCE_DONT_ASSERT_ON_PREDICATE.key -> "true") {
      val pushable = PushablePredicate.unapply(Literal.create("string"))
      assert(!pushable.isDefined)
    }
  }

  test("PushablePredicate success - flag on") {
    withSQLConf(SQLConf.DATA_SOURCE_DONT_ASSERT_ON_PREDICATE.key -> "true") {
      val pushable = PushablePredicate.unapply(Literal.create(true))
      assert(pushable.isDefined)
    }
  }

  test("PushablePredicate success") {
    withSQLConf(SQLConf.DATA_SOURCE_DONT_ASSERT_ON_PREDICATE.key -> "false") {
      val pushable = PushablePredicate.unapply(Literal.create(true))
      assert(pushable.isDefined)
    }
  }

  test("PushablePredicate throws") {
    withSQLConf(SQLConf.DATA_SOURCE_DONT_ASSERT_ON_PREDICATE.key -> "false") {
      intercept[java.lang.AssertionError] {
        val pushable = PushablePredicate.unapply(Literal.create("string"))
      }
    }
  }
}
