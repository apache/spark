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

import scala.jdk.CollectionConverters._

import com.google.protobuf.ByteString

import org.apache.spark.SparkFunSuite
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.Expression.{Alias, ExpressionString, UnresolvedStar}
import org.apache.spark.sql.{AnalysisException, Dataset, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.connect.common.InvalidPlanInput
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter.toLiteralProto
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
    SparkConnectPlannerTestUtils.transform(spark, rel)
  }

  def transform(cmd: proto.Command): Unit = {
    SparkConnectPlannerTestUtils.transform(spark, cmd)
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
   * @param schema
   *   the schema of LocalRelation
   * @param data
   *   the data of LocalRelation
   * @return
   */
  def createLocalRelationProto(schema: StructType, data: Seq[InternalRow]): proto.Relation = {
    createLocalRelationProto(DataTypeUtils.toAttributes(schema), data)
  }

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
      data: Seq[InternalRow],
      timeZoneId: String = "UTC"): proto.Relation = {
    val localRelationBuilder = proto.LocalRelation.newBuilder()

    val bytes = ArrowConverters
      .toBatchWithSchemaIterator(
        data.iterator,
        DataTypeUtils.fromAttributes(attrs.map(_.toAttribute)),
        Long.MaxValue,
        Long.MaxValue,
        timeZoneId,
        true)
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
      new SparkConnectPlanner(SparkConnectTestUtils.createDummySessionHolder(None.orNull))
        .transformRelation(
          proto.Relation.newBuilder
            .setLimit(proto.Limit.newBuilder.setLimit(10))
            .build())
    }
  }

  test("InvalidInputs") {
    // No Relation Set
    intercept[IndexOutOfBoundsException](
      new SparkConnectPlanner(SparkConnectTestUtils.createDummySessionHolder(None.orNull))
        .transformRelation(proto.Relation.newBuilder().build()))

    intercept[InvalidPlanInput](
      new SparkConnectPlanner(SparkConnectTestUtils.createDummySessionHolder(None.orNull))
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

  test("Simple Table with options") {
    val read = proto.Read.newBuilder().build()
    // Invalid read without Table name.
    intercept[InvalidPlanInput](transform(proto.Relation.newBuilder.setRead(read).build()))
    val readWithTable = read.toBuilder
      .setNamedTable(
        proto.Read.NamedTable.newBuilder
          .setUnparsedIdentifier("name")
          .putOptions("p1", "v1")
          .build())
      .build()
    val res = transform(proto.Relation.newBuilder.setRead(readWithTable).build())
    res match {
      case e: UnresolvedRelation => assert(e.options.get("p1") == "v1")
      case _ => assert(false, "Do not have expected options")
    }
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
      .addAllOrder(Seq(proto.Expression.SortOrder.newBuilder().build()).asJava)
      .build()
    intercept[IndexOutOfBoundsException](
      transform(proto.Relation.newBuilder().setSort(sort).build()),
      "No Input set.")

    val f = proto.Expression.SortOrder
      .newBuilder()
      .setNullOrdering(proto.Expression.SortOrder.NullOrdering.SORT_NULLS_LAST)
      .setDirection(proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_DESCENDING)
      .setChild(
        proto.Expression.newBuilder
          .setUnresolvedAttribute(
            proto.Expression.UnresolvedAttribute.newBuilder.setUnparsedIdentifier("col").build())
          .build())
      .build()

    val res = transform(
      proto.Relation.newBuilder
        .setSort(
          proto.Sort.newBuilder
            .addAllOrder(Seq(f).asJava)
            .setInput(readRel)
            .setIsGlobal(true))
        .build())
    assert(res.nodeName == "Sort")
    assert(res.asInstanceOf[logical.Sort].global)

    val res2 = transform(
      proto.Relation.newBuilder
        .setSort(
          proto.Sort.newBuilder
            .addAllOrder(Seq(f).asJava)
            .setInput(readRel)
            .setIsGlobal(false))
        .build())
    assert(res2.nodeName == "Sort")
    assert(!res2.asInstanceOf[logical.Sort].global)
  }

  test("Simple Union") {
    intercept[InvalidPlanInput](
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

  test("Union By Name") {
    val union = proto.Relation.newBuilder
      .setSetOp(
        proto.SetOperation.newBuilder
          .setLeftInput(readRel)
          .setRightInput(readRel)
          .setSetOpType(proto.SetOperation.SetOpType.SET_OP_TYPE_UNION)
          .setByName(false)
          .setAllowMissingColumns(true)
          .build())
      .build()
    val msg = intercept[InvalidPlanInput] {
      transform(union)
    }
    assert(
      msg.getMessage.contains(
        "UnionByName `allowMissingCol` can be true only if `byName` is true."))
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
        .setFunctionName("==")
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
          .setLiteral(proto.Expression.Literal.newBuilder.setInteger(32))
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
            .setFunctionName("sum")
            .addArguments(unresolvedAttribute))
        .build()

    val agg = proto.Aggregate.newBuilder
      .setInput(readRel)
      .addAggregateExpressions(sum)
      .addGroupingExpressions(unresolvedAttribute)
      .setGroupType(proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY)
      .build()

    val res = transform(proto.Relation.newBuilder.setAggregate(agg).build())
    assert(res.nodeName == "Aggregate")
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

  test("Test invalid deduplicateWithinWatermark") {
    val deduplicateWithinWatermark = proto.Deduplicate
      .newBuilder()
      .setInput(readRel)
      .setAllColumnsAsKeys(true)
      .addColumnNames("test")
      .setWithinWatermark(true)

    val e = intercept[InvalidPlanInput] {
      transform(
        proto.Relation.newBuilder
          .setDeduplicate(deduplicateWithinWatermark)
          .build())
    }
    assert(
      e.getMessage.contains("Cannot deduplicate on both all columns and a subset of columns"))

    val deduplicateWithinWatermark2 = proto.Deduplicate
      .newBuilder()
      .setInput(readRel)
      .setWithinWatermark(true)
    val e2 = intercept[InvalidPlanInput] {
      transform(
        proto.Relation.newBuilder
          .setDeduplicate(deduplicateWithinWatermark2)
          .build())
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

    val localRelation = createLocalRelationProto(schema, inputRows)
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
    val data = ArrowConverters.createEmptyArrowBatch(schema, null, true)
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

  test("Test duplicated names in WithColumns") {
    intercept[AnalysisException] {
      transform(
        proto.Relation
          .newBuilder()
          .setWithColumns(
            proto.WithColumns
              .newBuilder()
              .setInput(readRel)
              .addAliases(proto.Expression.Alias
                .newBuilder()
                .addName("test")
                .setExpr(proto.Expression.newBuilder
                  .setLiteral(proto.Expression.Literal.newBuilder.setInteger(32))))
              .addAliases(proto.Expression.Alias
                .newBuilder()
                .addName("test")
                .setExpr(proto.Expression.newBuilder
                  .setLiteral(proto.Expression.Literal.newBuilder.setInteger(32)))))
          .build())
    }
  }

  test("Test multi nameparts for column names in WithColumns") {
    val e = intercept[InvalidPlanInput] {
      transform(
        proto.Relation
          .newBuilder()
          .setWithColumns(
            proto.WithColumns
              .newBuilder()
              .setInput(readRel)
              .addAliases(
                proto.Expression.Alias
                  .newBuilder()
                  .addName("part1")
                  .addName("part2")
                  .setExpr(proto.Expression.newBuilder
                    .setLiteral(proto.Expression.Literal.newBuilder.setInteger(32)))))
          .build())
    }
    assert(e.getMessage.contains("part1, part2"))
  }

  test("transform UnresolvedStar and ExpressionString") {
    val sql =
      "SELECT * FROM VALUES (1,'spark',1), (2,'hadoop',2), (3,'kafka',3) AS tab(id, name, value)"
    val input = proto.Relation
      .newBuilder()
      .setSql(
        proto.SQL
          .newBuilder()
          .setQuery(sql)
          .build())

    val project =
      proto.Project
        .newBuilder()
        .setInput(input)
        .addExpressions(
          proto.Expression
            .newBuilder()
            .setUnresolvedStar(UnresolvedStar.newBuilder().build())
            .build())
        .addExpressions(
          proto.Expression
            .newBuilder()
            .setExpressionString(ExpressionString.newBuilder().setExpression("name").build())
            .build())
        .build()

    val df =
      Dataset.ofRows(spark, transform(proto.Relation.newBuilder.setProject(project).build()))
    val array = df.collect()
    assert(array.length == 3)
    assert(array(0).toString == InternalRow(1, "spark", 1, "spark").toString)
    assert(array(1).toString == InternalRow(2, "hadoop", 2, "hadoop").toString)
    assert(array(2).toString == InternalRow(3, "kafka", 3, "kafka").toString)
  }

  test("transform UnresolvedStar with target field") {
    val rows = (0 until 10).map { i =>
      InternalRow(InternalRow(InternalRow(i, i + 1)))
    }

    val schema = StructType(
      Seq(
        StructField(
          "a",
          StructType(Seq(StructField(
            "b",
            StructType(Seq(StructField("c", IntegerType), StructField("d", IntegerType)))))))))
    val inputRows = rows.map { row =>
      val proj = UnsafeProjection.create(schema)
      proj(row).copy()
    }

    val localRelation = createLocalRelationProto(schema, inputRows)

    val project =
      proto.Project
        .newBuilder()
        .setInput(localRelation)
        .addExpressions(
          proto.Expression
            .newBuilder()
            .setUnresolvedStar(UnresolvedStar.newBuilder().setUnparsedTarget("a.b.*").build())
            .build())
        .build()

    val df =
      Dataset.ofRows(spark, transform(proto.Relation.newBuilder.setProject(project).build()))
    assertResult(df.schema)(
      StructType(Seq(StructField("c", IntegerType), StructField("d", IntegerType))))

    val array = df.collect()
    assert(array.length == 10)
    for (i <- 0 until 10) {
      assert(i == array(i).getInt(0))
      assert(i + 1 == array(i).getInt(1))
    }
  }

  test("transform Project with Alias") {
    val input = proto.Expression
      .newBuilder()
      .setLiteral(
        proto.Expression.Literal
          .newBuilder()
          .setInteger(1)
          .build())

    val project =
      proto.Project
        .newBuilder()
        .addExpressions(
          proto.Expression
            .newBuilder()
            .setAlias(Alias.newBuilder().setExpr(input).addName("id").build())
            .build())
        .build()

    val df =
      Dataset.ofRows(spark, transform(proto.Relation.newBuilder.setProject(project).build()))
    assert(df.schema.fields.toSeq.map(_.name) == Seq("id"))
  }

  test("Hint") {
    val input = proto.Relation
      .newBuilder()
      .setSql(
        proto.SQL
          .newBuilder()
          .setQuery("select id from range(10)")
          .build())

    val logical = transform(
      proto.Relation
        .newBuilder()
        .setHint(proto.Hint
          .newBuilder()
          .setInput(input)
          .setName("REPARTITION")
          .addParameters(proto.Expression.newBuilder().setLiteral(toLiteralProto(10000)).build()))
        .build())

    val df = Dataset.ofRows(spark, logical)
    assert(df.rdd.partitions.length == 10000)
  }

  test("Hint with illegal name will be ignored") {
    val input = proto.Relation
      .newBuilder()
      .setSql(
        proto.SQL
          .newBuilder()
          .setQuery("select id from range(10)")
          .build())

    val logical = transform(
      proto.Relation
        .newBuilder()
        .setHint(
          proto.Hint
            .newBuilder()
            .setInput(input)
            .setName("illegal"))
        .build())
    assert(10 === Dataset.ofRows(spark, logical).count())
  }

  test("Hint with string attribute parameters") {
    val input = proto.Relation
      .newBuilder()
      .setSql(
        proto.SQL
          .newBuilder()
          .setQuery("select id from range(10)")
          .build())

    val logical = transform(
      proto.Relation
        .newBuilder()
        .setHint(proto.Hint
          .newBuilder()
          .setInput(input)
          .setName("REPARTITION")
          .addParameters(proto.Expression.newBuilder().setLiteral(toLiteralProto("id")).build()))
        .build())
    assert(10 === Dataset.ofRows(spark, logical).count())
  }

  test("Hint with wrong parameters") {
    val input = proto.Relation
      .newBuilder()
      .setSql(
        proto.SQL
          .newBuilder()
          .setQuery("select id from range(10)")
          .build())

    val logical = transform(
      proto.Relation
        .newBuilder()
        .setHint(proto.Hint
          .newBuilder()
          .setInput(input)
          .setName("REPARTITION")
          .addParameters(proto.Expression.newBuilder().setLiteral(toLiteralProto(true)).build()))
        .build())
    intercept[AnalysisException](Dataset.ofRows(spark, logical))
  }

  test("transform SortOrder") {
    val input = proto.Relation
      .newBuilder()
      .setSql(
        proto.SQL
          .newBuilder()
          .setQuery("SELECT id FROM VALUES (5),(1),(2),(6),(4),(3),(7),(9),(8),(null) AS tab(id)")
          .build())

    val relation = proto.Relation
      .newBuilder()
      .setSort(
        proto.Sort
          .newBuilder()
          .setInput(input)
          .setIsGlobal(false)
          .addOrder(
            proto.Expression.SortOrder
              .newBuilder()
              .setDirectionValue(
                proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING_VALUE)
              .setNullOrdering(proto.Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST)
              .setChild(proto.Expression
                .newBuilder()
                .setExpressionString(
                  proto.Expression.ExpressionString.newBuilder().setExpression("id")))))
      .build()
    val df = Dataset.ofRows(spark, transform(relation))
    df.foreachPartition { p: Iterator[Row] =>
      var previousValue: Int = -1
      p.foreach { r =>
        val v = r.getAs[Int](0)
        // null will be converted to 0
        if (v == 0) {
          assert(previousValue == -1, "null should be first")
        }
        if (previousValue != -1) {
          assert(v > previousValue, "Partition is not ordered.")
        }
        previousValue = v
      }
    }
  }

  test("RepartitionByExpression") {
    val input = proto.Relation
      .newBuilder()
      .setSql(
        proto.SQL
          .newBuilder()
          .setQuery("select id from range(10)")
          .build())

    val logical = transform(
      proto.Relation
        .newBuilder()
        .setRepartitionByExpression(
          proto.RepartitionByExpression
            .newBuilder()
            .setInput(input)
            .setNumPartitions(3)
            .addPartitionExprs(proto.Expression.newBuilder
              .setExpressionString(proto.Expression.ExpressionString.newBuilder
                .setExpression("id % 2"))))
        .build())

    val df = Dataset.ofRows(spark, logical)
    assert(df.rdd.partitions.length == 3)
    val valueToPartition = df
      .selectExpr("id", "spark_partition_id()")
      .rdd
      .map(row => (row.getLong(0), row.getInt(1)))
      .collectAsMap()
    for ((value, partition) <- valueToPartition) {
      if (value % 2 == 0) {
        assert(partition == valueToPartition(0), "dataframe is not partitioned by `id % 2`")
      } else {
        assert(partition == valueToPartition(1), "dataframe is not partitioned by `id % 2`")
      }
    }
  }

  test("Repartition by range") {
    val input = proto.Relation
      .newBuilder()
      .setSql(
        proto.SQL
          .newBuilder()
          .setQuery("select id from range(10)")
          .build())

    val logical = transform(
      proto.Relation
        .newBuilder()
        .setRepartitionByExpression(
          proto.RepartitionByExpression
            .newBuilder()
            .setInput(input)
            .setNumPartitions(3)
            .addPartitionExprs(
              proto.Expression.newBuilder
                .setSortOrder(
                  proto.Expression.SortOrder.newBuilder
                    .setDirectionValue(
                      proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING_VALUE)
                    .setNullOrdering(proto.Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST)
                    .setChild(proto.Expression
                      .newBuilder()
                      .setExpressionString(
                        proto.Expression.ExpressionString.newBuilder().setExpression("id"))))))
        .build())

    val df = Dataset.ofRows(spark, logical)
    assert(df.rdd.partitions.length == 3)
    df.rdd.foreachPartition { p =>
      var previousValue = -1L
      p.foreach { r =>
        val v = r.getLong(0)
        if (previousValue != -1L) {
          assert(previousValue < v, "partition is not ordered.")
        }
        previousValue = v
      }
    }
  }

  test("RepartitionByExpression with wrong parameters") {
    val input = proto.Relation
      .newBuilder()
      .setSql(
        proto.SQL
          .newBuilder()
          .setQuery("select id from range(10)")
          .build())

    val logical = transform(
      proto.Relation
        .newBuilder()
        .setRepartitionByExpression(
          proto.RepartitionByExpression
            .newBuilder()
            .setInput(input)
            .addPartitionExprs(proto.Expression.newBuilder
              .setExpressionString(proto.Expression.ExpressionString.newBuilder
                .setExpression("illegal"))))
        .build())

    intercept[AnalysisException](Dataset.ofRows(spark, logical))
  }
}
