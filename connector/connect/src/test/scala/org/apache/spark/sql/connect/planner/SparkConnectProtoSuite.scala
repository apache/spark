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
package org.apache.spark.sql.connect.planner

import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation

/**
 * This suite is based on connect DSL and test that given same dataframe operations, whether
 * connect could construct a proto plan that can be translated back, and after analyzed, be the
 * same as Spark dataframe's generated plan.
 */
class SparkConnectProtoSuite extends PlanTest with SparkConnectPlanTest {

  lazy val connectTestRelation = createLocalRelationProto(Seq($"id".int))

  lazy val sparkTestRelation: LocalRelation = LocalRelation($"id".int)

  test("Basic select") {
    val connectPlan = {
      // TODO: Scala only allows one implicit per scope so we keep proto implicit imports in
      // this scope. Need to find a better way to make two implicits work in the same scope.
      import org.apache.spark.sql.connect.dsl.expressions._
      import org.apache.spark.sql.connect.dsl.plans._
      transform(connectTestRelation.select("id".protoAttr))
    }
    val sparkPlan = sparkTestRelation.select($"id")
    comparePlans(connectPlan.analyze, sparkPlan.analyze, false)
  }

  private def createLocalRelationProto(attrs: Seq[AttributeReference]): proto.Relation = {
    val localRelationBuilder = proto.LocalRelation.newBuilder()
    // TODO: set data types for each local relation attribute one proto supports data type.
    for (attr <- attrs) {
      localRelationBuilder.addAttributes(
        proto.Expression.QualifiedAttribute.newBuilder().setName(attr.name).build()
      )
    }
    proto.Relation.newBuilder().setLocalRelation(localRelationBuilder.build()).build()
  }
}
