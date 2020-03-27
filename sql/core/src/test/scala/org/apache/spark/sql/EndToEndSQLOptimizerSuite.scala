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

import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.test.SharedSparkSession

class EndToEndSQLOptimizerSuite extends QueryTest with SharedSparkSession {

  test("SPARK-31280: Perform propagating empty relation after RewritePredicateSubquery") {
    val df1 = sql(
      s"""
         |SELECT *
         |FROM VALUES(1), (2) t1(key)
         |WHERE key IN
         |  (SELECT key FROM VALUES(1) t2(key) WHERE 1=0)
       """.stripMargin)
    assert(df1.queryExecution.optimizedPlan.isInstanceOf[LocalRelation])
    checkAnswer(df1, Nil)

    val df2 = sql(
      s"""
         |SELECT *
         |FROM VALUES(1), (2) t1(key)
         |WHERE key NOT IN
         |  (SELECT key FROM VALUES(1) t2(key) WHERE 1=0)
       """.stripMargin)

    assert(df2.queryExecution.optimizedPlan.isInstanceOf[LocalRelation])
    checkAnswer(df2, Seq(Row(1), Row(2)))

    // Because [[RewriteNonCorrelatedExists]] will rewrite non-correlated exists subqueries to
    // scalar expressions early, so this only take effects on correlated exists subqueries
    val df3 = sql(
      s"""
         |SELECT *
         |FROM VALUES(1), (2) t1(key)
         |WHERE EXISTS
         |  (SELECT key FROM VALUES(1) t2(key) WHERE t1.key = 1 AND 1=0)
       """.stripMargin)

    assert(df3.queryExecution.optimizedPlan.isInstanceOf[LocalRelation])
    checkAnswer(df3, Nil)
  }
}
