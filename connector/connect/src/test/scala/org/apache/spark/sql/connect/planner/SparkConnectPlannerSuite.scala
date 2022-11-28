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

import com.google.protobuf.ByteString

import org.apache.spark.SparkFunSuite
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.Expression.UnresolvedStar
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Testing trait for SparkConnect tests with some helper methods to make it easier to create new
 * test cases.
 */
trait SparkConnectPlanTest extends SharedSparkSession {

  def transform(rel: proto.Relation): logical.LogicalPlan = {
    new SparkConnectPlanner(spark).transformRelation(rel)
  }

  def transform(cmd: proto.Command): Unit = {
    new SparkConnectPlanner(spark).process(cmd)
  }

  def readRel: proto.Relation =
    proto.Relation
      .newBuilder()
      .setRead(
        proto.Read
          .newBuilder()
          .setNamedTable(proto.Read.NamedTable.newBuilder().setUnparsedIdentifier("table"))
          .build())
      .build()

  /**
   * Creates a local relation for testing purposes. The local relation is mapped to it's
   * equivalent in Catalyst and can be easily used for planner testing.
   *
   * @param attrs
   *   the attributes of LocalRelation
   * @param data
   *   the data of LocalRelation
   * @return
   */
  def createLocalRelationProto(
      attrs: Seq[AttributeReference],
      data: Seq[InternalRow]): proto.Relation = {
    val localRelationBuilder = proto.LocalRelation.newBuilder()

    val bytes = ArrowConverters
      .toBatchWithSchemaIterator(
        data.iterator,
        StructType.fromAttributes(attrs.map(_.toAttribute)),
        Long.MaxValue,
        Long.MaxValue,
        null)
      .next()

    localRelationBuilder.setData(ByteString.copyFrom(bytes))
    proto.Relation.newBuilder().setLocalRelation(localRelationBuilder.build()).build()
  }
}

/**
 * This is a rudimentary test class for SparkConnect. The main goal of these basic tests is to
 * ensure that the transformation from Proto to LogicalPlan works and that the right nodes are
 * generated.
 */
class SparkConnectPlannerSuite extends SparkFunSuite with SparkConnectPlanTest {

  test("Simple Limit") {
    assertThrows[IndexOutOfBoundsException] {
      new SparkConnectPlanner(None.orNull)
        .transformRelation(
          proto.Relation.newBuilder
            .setLimit(proto.Limit.newBuilder.setLimit(10))
            .build())
    }
  }

  test("InvalidInputs") {
    // No Relation Set
    intercept[IndexOutOfBoundsException](
      new SparkConnectPlanner(None.orNull).transformRelation(proto.Relation.newBuilder().build()))

    intercept[InvalidPlanInput](
      new SparkConnectPlanner(None.orNull)
        .transformRelation(
          proto.Relation.newBuilder.setUnknown(proto.Unknown.newBuilder().build()).build()))
  }

  test("Simple Read") {
    val read = proto.Read.newBuilder().build()
    // Invalid read without Table name.
    intercept[InvalidPlanInput](transform(proto.Relation.newBuilder.setRead(read).build()))
    val readWithTable = read.toBuilder
      .setNamedTable(proto.Read.NamedTable.newBuilder.setUnparsedIdentifier("name").build())
      .build()
    val res = transform(proto.Relation.newBuilder.setRead(readWithTable).build())
    assert(res !== null)
    assert(res.nodeName == "UnresolvedRelation")
  }

  test("Simple Project") {
    val readWithTable = proto.Read
      .newBuilder()
      .setNamedTable(proto.Read.NamedTable.newBuilder.setUnparsedIdentifier("name").build())
      .build()
    val project =
      proto.Project
        .newBuilder()
        .setInput(proto.Relation.newBuilder().setRead(readWithTable).build())
        .addExpressions(
          proto.Expression
            .newBuilder()
            .setUnresolvedStar(UnresolvedStar.newBuilder().build())
            .build())
        .build()
    val res = transform(proto.Relation.newBuilder.setProject(project).build())
    assert(res !== null)
    assert(res.nodeName == "Project")
  }

  test("Simple Sort") {
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
      .setExpression(
        proto.Expression.newBuilder
          .setUnresolvedAttribute(
            proto.Expression.UnresolvedAttribute.newBuilder.setUnparsedIdentifier("col").build())
          .build())
      .build()

    val res = transform(
      proto.Relation.newBuilder
        .setSort(
          proto.Sort.newBuilder
            .addAllSortFields(Seq(f).asJava)
            .setInput(readRel)
            .setIsGlobal(true))
        .build())
    assert(res.nodeName == "Sort")
    assert(res.asInstanceOf[logical.Sort].global)

    val res2 = transform(
      proto.Relation.newBuilder
        .setSort(
          proto.Sort.newBuilder
            .addAllSortFields(Seq(f).asJava)
            .setInput(readRel)
            .setIsGlobal(false))
        .build())
    assert(res2.nodeName == "Sort")
    assert(!res2.asInstanceOf[logical.Sort].global)
  }

  test("Simple Union") {
    intercept[AssertionError](
      transform(proto.Relation.newBuilder.setSetOp(proto.SetOperation.newBuilder.build()).build))
    val union = proto.Relation.newBuilder
      .setSetOp(
        proto.SetOperation.newBuilder.setLeftInput(readRel).setRightInput(readRel).build())
      .build()
    val msg = intercept[InvalidPlanInput] {
      transform(union)
    }
    assert(msg.getMessage.contains("Unsupported set operation"))

    val res = transform(
      proto.Relation.newBuilder
        .setSetOp(
          proto.SetOperation.newBuilder
            .setLeftInput(readRel)
            .setRightInput(readRel)
            .setSetOpType(proto.SetOperation.SetOpType.SET_OP_TYPE_UNION)
            .setIsAll(true)
            .build())
        .build())
    assert(res.nodeName == "Union")
  }

  test("Simple Join") {
    val incompleteJoin =
      proto.Relation.newBuilder.setJoin(proto.Join.newBuilder.setLeft(readRel)).build()
    intercept[AssertionError](transform(incompleteJoin))

    // Join type JOIN_TYPE_UNSPECIFIED is not supported.
    intercept[InvalidPlanInput] {
      val simpleJoin = proto.Relation.newBuilder
        .setJoin(proto.Join.newBuilder.setLeft(readRel).setRight(readRel))
        .build()
      transform(simpleJoin)
    }

    // Construct a simple Join.
    val unresolvedAttribute = proto.Expression
      .newBuilder()
      .setUnresolvedAttribute(
        proto.Expression.UnresolvedAttribute.newBuilder().setUnparsedIdentifier("left").build())
      .build()

    val joinCondition = proto.Expression.newBuilder.setUnresolvedFunction(
      proto.Expression.UnresolvedFunction.newBuilder
        .addAllParts(Seq("==").asJava)
        .addArguments(unresolvedAttribute)
        .addArguments(unresolvedAttribute)
        .build())

    val simpleJoin = proto.Relation.newBuilder
      .setJoin(
        proto.Join.newBuilder
          .setLeft(readRel)
          .setRight(readRel)
          .setJoinType(proto.Join.JoinType.JOIN_TYPE_INNER)
          .setJoinCondition(joinCondition)
          .build())
      .build()

    val res = transform(simpleJoin)
    assert(res.nodeName == "Join")
    assert(res != null)

    val e = intercept[InvalidPlanInput] {
      val simpleJoin = proto.Relation.newBuilder
        .setJoin(
          proto.Join.newBuilder
            .setLeft(readRel)
            .setRight(readRel)
            .addUsingColumns("test_col")
            .setJoinCondition(joinCondition))
        .build()
      transform(simpleJoin)
    }
    assert(
      e.getMessage.contains(
        "Using columns or join conditions cannot be set at the same time in Join"))
  }

  test("Simple Projection") {
    val project = proto.Project.newBuilder
      .setInput(readRel)
      .addExpressions(
        proto.Expression.newBuilder
          .setLiteral(proto.Expression.Literal.newBuilder.setI32(32))
          .build())
      .build()

    val res = transform(proto.Relation.newBuilder.setProject(project).build())
    assert(res.nodeName == "Project")
  }

  test("Simple Aggregation") {
    val unresolvedAttribute = proto.Expression
      .newBuilder()
      .setUnresolvedAttribute(
        proto.Expression.UnresolvedAttribute.newBuilder().setUnparsedIdentifier("left").build())
      .build()

    val sum =
      proto.Expression
        .newBuilder()
        .setUnresolvedFunction(
          proto.Expression.UnresolvedFunction
            .newBuilder()
            .addParts("sum")
            .addArguments(unresolvedAttribute))
        .build()

    val agg = proto.Aggregate.newBuilder
      .setInput(readRel)
      .addResultExpressions(sum)
      .addGroupingExpressions(unresolvedAttribute)
      .build()

    val res = transform(proto.Relation.newBuilder.setAggregate(agg).build())
    assert(res.nodeName == "Aggregate")
  }

  test("Invalid DataSource") {
    val dataSource = proto.Read.DataSource.newBuilder()

    val e = intercept[InvalidPlanInput](
      transform(
        proto.Relation
          .newBuilder()
          .setRead(proto.Read.newBuilder().setDataSource(dataSource))
          .build()))
    assert(e.getMessage.contains("DataSource requires a format"))
  }

  test("Test invalid deduplicate") {
    val deduplicate = proto.Deduplicate
      .newBuilder()
      .setInput(readRel)
      .setAllColumnsAsKeys(true)
      .addColumnNames("test")

    val e = intercept[InvalidPlanInput] {
      transform(proto.Relation.newBuilder.setDeduplicate(deduplicate).build())
    }
    assert(
      e.getMessage.contains("Cannot deduplicate on both all columns and a subset of columns"))

    val deduplicate2 = proto.Deduplicate
      .newBuilder()
      .setInput(readRel)
    val e2 = intercept[InvalidPlanInput] {
      transform(proto.Relation.newBuilder.setDeduplicate(deduplicate2).build())
    }
    assert(e2.getMessage.contains("either deduplicate on all columns or a subset of columns"))
  }

  test("Test invalid intersect, except") {
    // Except with union_by_name=true
    val except = proto.SetOperation
      .newBuilder()
      .setLeftInput(readRel)
      .setRightInput(readRel)
      .setByName(true)
      .setSetOpType(proto.SetOperation.SetOpType.SET_OP_TYPE_EXCEPT)
    val e =
      intercept[InvalidPlanInput](transform(proto.Relation.newBuilder.setSetOp(except).build()))
    assert(e.getMessage.contains("Except does not support union_by_name"))

    // Intersect with union_by_name=true
    val intersect = proto.SetOperation
      .newBuilder()
      .setLeftInput(readRel)
      .setRightInput(readRel)
      .setByName(true)
      .setSetOpType(proto.SetOperation.SetOpType.SET_OP_TYPE_INTERSECT)
    val e2 = intercept[InvalidPlanInput](
      transform(proto.Relation.newBuilder.setSetOp(intersect).build()))
    assert(e2.getMessage.contains("Intersect does not support union_by_name"))
  }

  test("transform LocalRelation") {
    val rows = (0 until 10).map { i =>
      InternalRow(i, UTF8String.fromString(s"str-$i"), InternalRow(i))
    }

    val schema = StructType(
      Seq(
        StructField("int", IntegerType),
        StructField("str", StringType),
        StructField("struct", StructType(Seq(StructField("inner", IntegerType))))))
    val inputRows = rows.map { row =>
      val proj = UnsafeProjection.create(schema)
      proj(row).copy()
    }

    val localRelation = createLocalRelationProto(schema.toAttributes, inputRows)
    val df = Dataset.ofRows(spark, transform(localRelation))
    val array = df.collect()
    assertResult(10)(array.length)
    assert(schema == df.schema)
    for (i <- 0 until 10) {
      assert(i == array(i).getInt(0))
      assert(s"str-$i" == array(i).getString(1))
      assert(i == array(i).getStruct(2).getInt(0))
    }
  }

  test("Empty ArrowBatch") {
    val schema = StructType(Seq(StructField("int", IntegerType)))
    val data = ArrowConverters.createEmptyArrowBatch(schema, null)
    val localRelation = proto.Relation
      .newBuilder()
      .setLocalRelation(
        proto.LocalRelation
          .newBuilder()
          .setData(ByteString.copyFrom(data))
          .build())
      .build()
    val df = Dataset.ofRows(spark, transform(localRelation))
    assert(schema == df.schema)
    assert(df.isEmpty)
  }

  test("Illegal LocalRelation data") {
    intercept[Exception] {
      transform(
        proto.Relation
          .newBuilder()
          .setLocalRelation(
            proto.LocalRelation
              .newBuilder()
              .setData(ByteString.copyFrom("illegal".getBytes()))
              .build())
          .build())
    }
  }
}
