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

import org.apache.spark.{SparkFunSuite, TestUtils}
import org.apache.spark.connect.proto
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sparkconnect.planner.{InvalidPlanInput, SparkConnectPlanner}

trait SparkConnectPlanTest {
  def transform(rel: proto.Relation): LogicalPlan = {
    SparkConnectPlanner(rel, None.orNull).transform()
  }

  def readRel: proto.Relation =
    proto
      .Relation()
      .withRead(proto.Read().withNamedTable(proto.Read.NamedTable().withParts(Seq("table"))))
}

trait SparkConnectSessionTest {
  protected var spark: SparkSession

}

class SparkConnectPlannerSuite extends SparkFunSuite with SparkConnectPlanTest {

  protected var spark: SparkSession = null

  override def beforeAll(): Unit = {
    super.beforeAll()

    TestUtils.configTestLog4j2("INFO")
//    spark = SparkSession
//      .builder()
//      .master("local-cluster[2,1,512]")
//      .config(EXECUTOR_MEMORY.key, "512m")
//      .appName("testing")
//      .getOrCreate()
  }

  test("LIMIT simple") {
    assertThrows[IndexOutOfBoundsException] {
      SparkConnectPlanner(proto.Relation().withFetch(proto.Fetch(limit = 10)), None.orNull)
        .transform()
    }
  }

  test("InvalidInputs") {
    // No Relation Set
    intercept[IndexOutOfBoundsException](
      SparkConnectPlanner(proto.Relation(), None.orNull).transform())

    intercept[InvalidPlanInput](
      SparkConnectPlanner(proto.Relation().withUnknown(proto.Unknown()), None.orNull).transform())

  }

  test("READ simple") {
    val read = proto.Read()
    // Invalid read without Table name.
    intercept[InvalidPlanInput](transform(proto.Relation().withRead(read)))
    val readWithTable = read.withNamedTable(proto.Read.NamedTable().withParts(Seq("name")))
    assert(transform(proto.Relation().withRead(readWithTable)) !== null)
  }

  test("SORT") {
    val sort = proto.Sort().withSortFields(Seq(proto.Sort.SortField()))
    intercept[IndexOutOfBoundsException](
      transform(proto.Relation().withSort(sort)),
      "No Input set.")

    val f = proto.Sort
      .SortField()
      .withNulls(proto.Sort.SortNulls.SORT_NULLS_LAST)
      .withDirection(proto.Sort.SortDirection.SORT_DIRECTION_DESCENDING)
      .withExpression(
        proto
          .Expression()
          .withUnresolvedAttribute(proto.Expression.UnresolvedAttribute().withParts(Seq("col"))))

    transform(proto.Relation().withSort(proto.Sort().withSortFields(Seq(f)).withInput(readRel)))

  }

  test("UNION") {
    intercept[AssertionError](transform(proto.Relation().withUnion(proto.Union())))
    val union = proto.Relation().withUnion(proto.Union().withInputs(Seq(readRel, readRel)))
    val msg = intercept[InvalidPlanInput] {
      transform(union)
    }
    assert(msg.getMessage.contains("Unsupported set operation"))

    transform(
      proto
        .Relation()
        .withUnion(
          proto
            .Union()
            .withInputs(Seq(readRel, readRel))
            .withUnionType(proto.Union.UnionType.UNION_TYPE_ALL)))
  }

}
