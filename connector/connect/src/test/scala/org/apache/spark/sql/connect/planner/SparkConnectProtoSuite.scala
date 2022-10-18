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
import org.apache.spark.connect.proto.Join.JoinType
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, PlanTest, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation

/**
 * This suite is based on connect DSL and test that given same dataframe operations, whether
 * connect could construct a proto plan that can be translated back, and after analyzed, be the
 * same as Spark dataframe's generated plan.
 */
class SparkConnectProtoSuite extends PlanTest with SparkConnectPlanTest {

  lazy val connectTestRelation = createLocalRelationProto(Seq($"id".int, $"name".string))

  lazy val connectTestRelation2 = createLocalRelationProto(Seq($"key".int, $"value".int))

  lazy val sparkTestRelation: LocalRelation = LocalRelation($"id".int, $"name".string)

  lazy val sparkTestRelation2: LocalRelation = LocalRelation($"key".int, $"value".int)

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

  test("UnresolvedFunction resolution.") {
    {
      import org.apache.spark.sql.connect.dsl.expressions._
      import org.apache.spark.sql.connect.dsl.plans._
      assertThrows[IllegalArgumentException] {
        transform(connectTestRelation.select(callFunction("default.hex", Seq("id".protoAttr))))
      }
    }

    val connectPlan = {
      import org.apache.spark.sql.connect.dsl.expressions._
      import org.apache.spark.sql.connect.dsl.plans._
      transform(
        connectTestRelation.select(callFunction(Seq("default", "hex"), Seq("id".protoAttr))))
    }

    assertThrows[UnsupportedOperationException] {
      connectPlan.analyze
    }

    val validPlan = {
      import org.apache.spark.sql.connect.dsl.expressions._
      import org.apache.spark.sql.connect.dsl.plans._
      transform(connectTestRelation.select(callFunction(Seq("hex"), Seq("id".protoAttr))))
    }
    assert(validPlan.analyze != null)
  }

  test("Basic filter") {
    val connectPlan = {
      import org.apache.spark.sql.connect.dsl.expressions._
      import org.apache.spark.sql.connect.dsl.plans._
      transform(connectTestRelation.where("id".protoAttr < 0))
    }

    val sparkPlan = sparkTestRelation.where($"id" < 0).analyze
    comparePlans(connectPlan.analyze, sparkPlan.analyze, false)
  }

  test("Basic joins with different join types") {
    val connectPlan = {
      import org.apache.spark.sql.connect.dsl.plans._
      transform(connectTestRelation.join(connectTestRelation2))
    }
    val sparkPlan = sparkTestRelation.join(sparkTestRelation2)
    comparePlans(connectPlan.analyze, sparkPlan.analyze, false)

    val connectPlan2 = {
      import org.apache.spark.sql.connect.dsl.plans._
      transform(connectTestRelation.join(connectTestRelation2, condition = None))
    }
    val sparkPlan2 = sparkTestRelation.join(sparkTestRelation2, condition = None)
    comparePlans(connectPlan2.analyze, sparkPlan2.analyze, false)
    for ((t, y) <- Seq(
        (JoinType.JOIN_TYPE_LEFT_OUTER, LeftOuter),
        (JoinType.JOIN_TYPE_RIGHT_OUTER, RightOuter),
        (JoinType.JOIN_TYPE_FULL_OUTER, FullOuter),
        (JoinType.JOIN_TYPE_LEFT_ANTI, LeftAnti),
        (JoinType.JOIN_TYPE_LEFT_SEMI, LeftSemi),
        (JoinType.JOIN_TYPE_INNER, Inner))) {
      val connectPlan3 = {
        import org.apache.spark.sql.connect.dsl.plans._
        transform(connectTestRelation.join(connectTestRelation2, t))
      }
      val sparkPlan3 = sparkTestRelation.join(sparkTestRelation2, y)
      comparePlans(connectPlan3.analyze, sparkPlan3.analyze, false)
    }
  }

  test("Test sample") {
    val connectPlan = {
      import org.apache.spark.sql.connect.dsl.plans._
      transform(connectTestRelation.sample(0, 0.2, false, 1))
    }
    val sparkPlan = sparkTestRelation.sample(0, 0.2, false, 1)
    comparePlans(connectPlan.analyze, sparkPlan.analyze, false)
  }

  test("column alias") {
    val connectPlan = {
      import org.apache.spark.sql.connect.dsl.expressions._
      import org.apache.spark.sql.connect.dsl.plans._
      transform(connectTestRelation.select("id".protoAttr.as("id2")))
    }
    val sparkPlan = sparkTestRelation.select($"id".as("id2"))
    comparePlans(connectPlan.analyze, sparkPlan.analyze, false)
  }

  test("Aggregate with more than 1 grouping expressions") {
    val connectPlan = {
      import org.apache.spark.sql.connect.dsl.expressions._
      import org.apache.spark.sql.connect.dsl.plans._
      transform(connectTestRelation.groupBy("id".protoAttr, "name".protoAttr)())
    }
    val sparkPlan = sparkTestRelation.groupBy($"id", $"name")()
    comparePlans(connectPlan.analyze, sparkPlan.analyze, false)
  }

  test("Test as(alias: String)") {
    val connectPlan = {
      import org.apache.spark.sql.connect.dsl.plans._
      transform(connectTestRelation.as("target_table"))
    }

    val sparkPlan = sparkTestRelation.as("target_table")
    comparePlans(connectPlan.analyze, sparkPlan.analyze, false)
  }

  test("Test StructType in LocalRelation") {
    val connectPlan = {
      import org.apache.spark.sql.connect.dsl.expressions._
      transform(createLocalRelationProtoByQualifiedAttributes(Seq("a".struct("id".int))))
    }
    val sparkPlan = LocalRelation($"a".struct($"id".int))
    comparePlans(connectPlan.analyze, sparkPlan.analyze, false)
  }

  test("Test limit offset") {
    val connectPlan = {
      import org.apache.spark.sql.connect.dsl.plans._
      transform(connectTestRelation.limit(10))
    }
    val sparkPlan = sparkTestRelation.limit(10)
    comparePlans(connectPlan.analyze, sparkPlan.analyze, false)

    val connectPlan2 = {
      import org.apache.spark.sql.connect.dsl.plans._
      transform(connectTestRelation.offset(2))
    }
    val sparkPlan2 = sparkTestRelation.offset(2)
    comparePlans(connectPlan2.analyze, sparkPlan2.analyze, false)

    val connectPlan3 = {
      import org.apache.spark.sql.connect.dsl.plans._
      transform(connectTestRelation.limit(10).offset(2))
    }
    val sparkPlan3 = sparkTestRelation.limit(10).offset(2)
    comparePlans(connectPlan3.analyze, sparkPlan3.analyze, false)

    val connectPlan4 = {
      import org.apache.spark.sql.connect.dsl.plans._
      transform(connectTestRelation.offset(2).limit(10))
    }
    val sparkPlan4 = sparkTestRelation.offset(2).limit(10)
    comparePlans(connectPlan4.analyze, sparkPlan4.analyze, false)
  }

  private def createLocalRelationProtoByQualifiedAttributes(
      attrs: Seq[proto.Expression.QualifiedAttribute]): proto.Relation = {
    val localRelationBuilder = proto.LocalRelation.newBuilder()
    for (attr <- attrs) {
      localRelationBuilder.addAttributes(attr)
    }
    proto.Relation.newBuilder().setLocalRelation(localRelationBuilder.build()).build()
  }
}
