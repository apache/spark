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

package org.apache.spark.sql.execution

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{GlobalLimit, LocalLimit, LocalRelation}
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

class BasicOperatorsStrategySuite extends QueryTest with SharedSparkSession {

  test("SPARK-35355: In the case of DataWritingCommand`s query is limit, " +
    "resolve query as CollectLimitExec to improve execution performance ") {
    val testRelation = LocalRelation.fromExternalRows(Seq(AttributeReference("id", IntegerType)())
      , data = Seq(Row(1)))
    val limitScan = GlobalLimit(Literal(5, IntegerType), LocalLimit(Literal(5, IntegerType)
      , testRelation))
    val plan = InsertIntoHadoopFsRelationCommand(null, null, false, null, null, null, null
      , limitScan, null, null, null, null)
    val sparkPlan = spark.sessionState.planner.plan(plan).next()

    assert(sparkPlan.children.head.isInstanceOf[CollectLimitExec])
  }
}
