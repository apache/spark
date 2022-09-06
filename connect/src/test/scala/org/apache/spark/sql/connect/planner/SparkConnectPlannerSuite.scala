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

import scala.collection.JavaConverters._

import org.apache.spark.{SparkFunSuite, TestUtils}
import org.apache.spark.connect.proto
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sparkconnect.planner.{InvalidPlanInput, SparkConnectPlanner}

trait SparkConnectPlanTest {
  def transform(rel: proto.Relation): LogicalPlan = {
    new SparkConnectPlanner(rel, None.orNull).transform()
  }

  def readRel: proto.Relation =
    proto.Relation
      .newBuilder()
      .setRead(
        proto.Read
          .newBuilder()
          .setNamedTable(proto.Read.NamedTable.newBuilder().addParts("table"))
          .build())
      .build()
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
      new SparkConnectPlanner(
        proto.Relation.newBuilder.setFetch(proto.Fetch.newBuilder.setLimit(10).build()).build(),
        None.orNull)
        .transform()
    }
  }

  test("InvalidInputs") {
    // No Relation Set
    intercept[IndexOutOfBoundsException](
      new SparkConnectPlanner(proto.Relation.newBuilder().build(), None.orNull).transform())

    intercept[InvalidPlanInput](
      new SparkConnectPlanner(
        proto.Relation.newBuilder.setUnknown(proto.Unknown.newBuilder().build()).build(),
        None.orNull).transform())

  }

  test("READ simple") {
    val read = proto.Read.newBuilder().build()
    // Invalid read without Table name.
    intercept[InvalidPlanInput](transform(proto.Relation.newBuilder.setRead(read).build()))
    val readWithTable = read.toBuilder
      .setNamedTable(proto.Read.NamedTable.newBuilder.addParts("name").build())
      .build()
    assert(transform(proto.Relation.newBuilder.setRead(readWithTable).build()) !== null)
  }

  test("SORT") {
    val sort = proto.Sort.newBuilder
      .addAllSortFields(Seq(proto.Sort.SortField.newBuilder().build()).asJava)
      .build()
    intercept[IndexOutOfBoundsException](
      transform(proto.Relation.newBuilder().setSort(sort).build()),
      "No Input set.")

    val f = proto.Sort.SortField
      .newBuilder()
      .setNulls(proto.Sort.SortNulls.SORT_NULLS_LAST)
      .setDirection(proto.Sort.SortDirection.SORT_DIRECTION_DESCENDING)
      .setExpression(proto.Expression.newBuilder
        .setUnresolvedAttribute(
          proto.Expression.UnresolvedAttribute.newBuilder.addAllParts(Seq("col").asJava).build())
        .build())
      .build()

    transform(
      proto.Relation.newBuilder
        .setSort(proto.Sort.newBuilder.addAllSortFields(Seq(f).asJava).setInput(readRel))
        .build())

  }

  test("UNION") {
    intercept[AssertionError](
      transform(proto.Relation.newBuilder.setUnion(proto.Union.newBuilder.build()).build))
    val union = proto.Relation.newBuilder
      .setUnion(proto.Union.newBuilder.addAllInputs(Seq(readRel, readRel).asJava).build())
      .build()
    val msg = intercept[InvalidPlanInput] {
      transform(union)
    }
    assert(msg.getMessage.contains("Unsupported set operation"))

    transform(
      proto.Relation.newBuilder
        .setUnion(
          proto.Union.newBuilder
            .addAllInputs(Seq(readRel, readRel).asJava)
            .setUnionType(proto.Union.UnionType.UNION_TYPE_ALL)
            .build())
        .build())
  }

}
