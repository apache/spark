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

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.connector.expressions.filter.{AlwaysTrue, Predicate => V2Predicate}
import org.apache.spark.sql.execution.datasources.v2.PushablePredicate
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.BooleanType

class PushablePredicateSuite extends QueryTest with SharedSparkSession {

  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED, true)

  test("simple boolean expression should always return v2 Predicate") {
    Seq(true, false).foreach { createV2Predicate =>
      Seq(true, false).foreach { noAssert =>
        withSQLConf(
          SQLConf.DATA_SOURCE_ALWAYS_CREATE_V2_PREDICATE.key -> createV2Predicate.toString,
          SQLConf.DATA_SOURCE_DONT_ASSERT_ON_PREDICATE.key -> noAssert.toString) {
          val pushable = PushablePredicate.unapply(Literal.create(true))
          assert(pushable.isDefined)
          assert(pushable.get.isInstanceOf[AlwaysTrue])
        }
      }
    }
  }

  test("non-boolean expression") {
    Seq(true, false).foreach { createV2Predicate =>
      Seq(true, false).foreach { noAssert =>
        withSQLConf(
          SQLConf.DATA_SOURCE_ALWAYS_CREATE_V2_PREDICATE.key -> createV2Predicate.toString,
          SQLConf.DATA_SOURCE_DONT_ASSERT_ON_PREDICATE.key -> noAssert.toString) {
          val catalystExpr = Literal.create("string")
          if (noAssert) {
            val pushable = PushablePredicate.unapply(catalystExpr)
            assert(pushable.isEmpty)
          } else {
            intercept[java.lang.AssertionError] {
              PushablePredicate.unapply(catalystExpr)
            }
          }
        }
      }
    }
  }

  test("non-trivial boolean expression") {
    Seq(true, false).foreach { createV2Predicate =>
      Seq(true, false).foreach { noAssert =>
        withSQLConf(
          SQLConf.DATA_SOURCE_ALWAYS_CREATE_V2_PREDICATE.key -> createV2Predicate.toString,
          SQLConf.DATA_SOURCE_DONT_ASSERT_ON_PREDICATE.key -> noAssert.toString) {
          val catalystExpr = Cast(Literal.create("true"), BooleanType)
          if (createV2Predicate) {
            val pushable = PushablePredicate.unapply(catalystExpr)
            assert(pushable.isDefined)
            assert(pushable.get.isInstanceOf[V2Predicate])
          } else {
            if (noAssert) {
              val pushable = PushablePredicate.unapply(catalystExpr)
              assert(pushable.isEmpty)
            } else {
              intercept[java.lang.AssertionError] {
                PushablePredicate.unapply(catalystExpr)
              }
            }
          }
        }
      }
    }
  }
}
