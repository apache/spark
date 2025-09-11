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
package org.apache.spark.sql.connect.client

import java.util.TimeZone
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.util.Random

import com.google.protobuf.{Any => PAny}
import io.grpc.inprocess.InProcessChannelBuilder
import org.apache.arrow.memory.RootAllocator
import org.apache.commons.lang3.mutable.MutableInt
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.WithRelations.ResolutionMethod
import org.apache.spark.sql.{Column, Encoder, Encoders}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.agnosticEncoderFor
import org.apache.spark.sql.connect.{ColumnNodeToProtoConverter, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.connect.client.arrow.ArrowSerializer
import org.apache.spark.sql.connect.test.ConnectFunSuite
import org.apache.spark.sql.functions.{col, max, min}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.types.StructType

/**
 * Test suite for the [[PlanOptimizer]].
 */
class PlanOptimizerSuite extends ConnectFunSuite with BeforeAndAfterEach {
  import PlanOptimizer.PlanId

  private implicit val longEncoder: Encoder[Long] = Encoders.scalaLong

  private implicit val longLongTupleEncoder: Encoder[(Long, Long)] =
    Encoders.tuple(longEncoder, longEncoder)

  private var spark: SparkSession = _

  private def newSparkSession(): SparkSession = {
    val client = SparkConnectClient(
      InProcessChannelBuilder.forName(getClass.getName).directExecutor().build())
    val session = new SparkSession(client, planIdGenerator = new AtomicLong)
    session.releaseSessionOnClose = false
    session
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark = newSparkSession()
  }

  override def afterEach(): Unit = {
    try {
      if (spark != null) {
        spark.close()
        spark = null
      }
    } finally {
      super.afterEach()
    }
  }

  private case class PlanStats(
      numRelations: Int,
      planIds: Set[Long],
      numDuplicatePlanIds: Int,
      numDuplicateRelations: Int = 0)

  private def collectPlanStats(plan: proto.Plan): PlanStats = {
    assert(plan.hasRoot)
    var numRelations = 0
    val plansIdCounts = mutable.Map.empty[Long, MutableInt]
    RelationTreeUtils.visit(plan.getRoot) { relation =>
      PlanId.get(relation).foreach { id =>
        plansIdCounts.getOrElseUpdate(id, new MutableInt(0)).incrementAndGet()
      }
      numRelations += 1
      true
    }
    PlanStats(
      numRelations,
      plansIdCounts.keySet.toSet,
      plansIdCounts.map(_._2.intValue()).count(_ > 1),
      plansIdCounts.map(_._2.intValue()).filter(_ > 1).sum)
  }

  private def checkNoDeduplication(df: Dataset[_]): Unit = {
    val plan = df.plan
    val optimizedPlan = df.optimizedPlan
    assert(plan eq optimizedPlan)
    val planStats = collectPlanStats(plan)
    assert(planStats.numDuplicatePlanIds == 0)
    assert(planStats.numDuplicateRelations == 0)
  }

  private def checkDeduplication(
      df: Dataset[_],
      numRelationsReduction: Int,
      sizeReduction: Long): Unit = {
    val plan = df.plan
    val optimizedPlan = df.optimizedPlan
    assert(plan != optimizedPlan)
    assert(optimizedPlan.getRoot.hasWithRelations)
    val withRelations = optimizedPlan.getRoot.getWithRelations
    assert(withRelations.getResolutionMethod == ResolutionMethod.BY_REFERENCE_ID)

    val planStats = collectPlanStats(plan)
    assert(planStats.numDuplicatePlanIds > 0)
    assert(planStats.numDuplicateRelations > 0)

    // An optimized plan should contain all the plan ids of the original plan.
    val optimizedPlanStats = collectPlanStats(optimizedPlan)
    assert(planStats.planIds.equals(optimizedPlanStats.planIds - PlanId(optimizedPlan.getRoot)))

    // Idempotency. Once optimized there should not be any optimization opportunity left.
    assert(optimizedPlanStats.numDuplicatePlanIds == 0)
    assert(optimizedPlanStats.numDuplicateRelations == 0)
    assert(PlanOptimizer.optimize(optimizedPlan, () => 0L) eq optimizedPlan)

    // Relations reduction.
    assert(planStats.numRelations == optimizedPlanStats.numRelations + numRelationsReduction)

    // Size reduction.
    val actualSizeReduction = plan.getSerializedSize - optimizedPlan.getSerializedSize
    assert(
      actualSizeReduction == sizeReduction,
      s"Actual reduction in plan size does not match expected reduction in plan: " +
        s"$actualSizeReduction != $sizeReduction")
  }

  test("un-optimizable plan remains unchanged - leafs") {
    checkNoDeduplication(spark.range(10))
    // checkNoDeduplication(spark.sql("select 1"))
    checkNoDeduplication(spark.emptyDataset[(Long, Long)])
    checkNoDeduplication(spark.read.format("parquet").load("s3://my-bucket/my-dir"))
    checkNoDeduplication(spark.newDataFrame(_ => ()))
    checkNoDeduplication(spark.newDataFrame(_.getUnknownBuilder))
    checkNoDeduplication(spark.newDataFrame(_.getCachedLocalRelationBuilder.setHash("1234")))
    checkNoDeduplication(
      spark.newDataFrame(_.getCachedRemoteRelationBuilder.setRelationId("rel1")))
    checkNoDeduplication(spark.newDataFrame(_.setReferencedPlanId(1)))
    checkNoDeduplication(spark.newDataFrame(_.setExtension(PAny.pack(spark.range(10).plan))))
    checkNoDeduplication(spark.newDataFrame {
      _.getCommonInlineUserDefinedDataSourceBuilder.setName("noop")
    })
    checkNoDeduplication(spark.newDataFrame {
      _.getCommonInlineUserDefinedTableFunctionBuilder.setFunctionName("noop")
    })
    checkNoDeduplication(spark.newDataFrame {
      _.getUnresolvedTableValuedFunctionBuilder.setFunctionName("noop")
    })
    checkNoDeduplication(spark.newDataFrame {
      _.getCatalogBuilder.getListCatalogsBuilder.setPattern("tbl*")
    })
  }

  test("un-optimizable plan remains unchanged - unary") {
    val input = spark.range(10)
    val id = col("id")
    checkNoDeduplication(input.select((id + 1).as("plus1")))
    checkNoDeduplication(input.filter(id > 1))
    checkNoDeduplication(input.sort(id.desc))
    checkNoDeduplication(input.limit(2))
    checkNoDeduplication(input.groupBy(id).count())
    checkNoDeduplication(input.sample(0.5))
    checkNoDeduplication(input.offset(3))
    checkNoDeduplication(input.dropDuplicates().as("q"))
    checkNoDeduplication(input.repartition(3))
    checkNoDeduplication(input.repartition(id))
    checkNoDeduplication(input.toDF("id"))
    checkNoDeduplication(input.withColumnRenamed("id", "di"))
    checkNoDeduplication(input.drop("id"))
    checkNoDeduplication(input.withColumn("id_plus1", id + 1))
    checkNoDeduplication(input.hint("broadcast"))
    checkNoDeduplication(input.to(new StructType().add("id", "string")))
    checkNoDeduplication(input.mapPartitions(_.map(_.toLong)))
    checkNoDeduplication(
      input
        .select(id, (id / 2).as("d2"), (id * 2).as("m2"))
        .unpivot(Array(id), Array(col("d2"), col("m2")), "var", "val"))
    checkNoDeduplication(input.withColumn("grp", id % 2).transpose(col("grp")))
    checkNoDeduplication(input.observe("simple", min(id), max(id)))
    checkNoDeduplication(spark.read.csv(input.map(i => s"$i,$i")(Encoders.STRING)))
    checkNoDeduplication(input.withWatermark("id", "1 minute"))
    checkNoDeduplication(input.describe("id"))
    checkNoDeduplication(input.summary("max", "min"))
    checkNoDeduplication(input.withColumn("b", id).stat.crosstab("id", "b"))
    checkNoDeduplication(input.stat.freqItems(Array("id")))
    checkNoDeduplication(input.stat.sampleBy(id, Map(0L -> 0.03, 1L -> 0.02), 33L))
    checkNoDeduplication(input.na.drop())
    checkNoDeduplication(input.na.fill(true))
    checkNoDeduplication(input.na.replace("id", Map(0L -> 1L)))

    // Manual ones...
    checkNoDeduplication(spark.newDataFrame {
      _.getShowStringBuilder
        .setInput(input.plan.getRoot)
        .setNumRows(10)
        .setTruncate(20)
        .setVertical(false)
    })
    checkNoDeduplication(spark.newDataFrame {
      _.getHtmlStringBuilder.setInput(input.plan.getRoot).setNumRows(10).setTruncate(20)
    })
    checkNoDeduplication(spark.newDataFrame {
      _.getTailBuilder.setInput(input.plan.getRoot).setLimit(4)
    })
    checkNoDeduplication(spark.newDataFrame {
      _.getCovBuilder.setInput(input.plan.getRoot).setCol1("a").setCol2("b")
    })
    checkNoDeduplication(spark.newDataFrame {
      _.getCorrBuilder.setInput(input.plan.getRoot).setCol1("a").setCol2("b")
    })
    checkNoDeduplication(spark.newDataFrame {
      _.getApplyInPandasWithStateBuilder
        .setInput(input.plan.getRoot)
        .addGroupingExpressions(toExpr(id))
    })
    checkNoDeduplication(spark.newDataFrame {
      _.getApproxQuantileBuilder
        .setInput(input.plan.getRoot)
        .addCols("id")
        .addProbabilities(0.1)
        .addProbabilities(0.2)
        .setRelativeError(0.01)
    })
    checkNoDeduplication(spark.newDataFrame { builder =>
      val transform = builder.getMlRelationBuilder.getTransformBuilder
        .setInput(input.plan.getRoot)
      transform.getTransformerBuilder
        .setName("oneHotEncoder")
        .setType(proto.MlOperator.OperatorType.OPERATOR_TYPE_TRANSFORMER)
    })
  }

  private def testBinaryOperationDeduplication(
      name: String,
      sizeReduction1: Int,
      sizeReduction2: Int,
      numRelationsReduction1: Int = -1,
      numRelationsReduction2: Int = 0)(
      f: ((DataFrame, Column), (DataFrame, Column)) => Dataset[_]): Unit = {
    test("optimize plan with duplicated relations - " + name) {
      val left = spark.range(10).as("a").toDF()
      val right = spark.range(11).as("b").toDF()
      // No deduplication.
      val df1 = f((left, left("id")), (right, right("id")))
      checkNoDeduplication(df1)
      // Deduplication
      val df2 = f((left, left("id")), (left, left("id")))
      checkDeduplication(
        df2,
        numRelationsReduction = numRelationsReduction1,
        sizeReduction = sizeReduction1)
      // Deeper tree
      val df3 = f((df2.toDF(), df2("id")), (left, left("id")))
      checkDeduplication(
        df3,
        numRelationsReduction = numRelationsReduction2,
        sizeReduction = sizeReduction2)
    }
  }

  testBinaryOperationDeduplication("join", 3, 24) { case ((left, leftKey), (right, rightKey)) =>
    left.join(right, leftKey === rightKey)
  }

  testBinaryOperationDeduplication("lateralJoin", 3, 24) {
    case ((left, leftKey), (right, rightKey)) =>
      left.lateralJoin(right, leftKey === rightKey)
  }

  testBinaryOperationDeduplication("union", 5, 26) { case ((left, _), (right, _)) =>
    left.union(right)
  }

  testBinaryOperationDeduplication("intersect", 5, 26) { case ((left, _), (right, _)) =>
    left.intersect(right)
  }

  testBinaryOperationDeduplication("except", 5, 26) { case ((left, _), (right, _)) =>
    left.except(right)
  }

  testBinaryOperationDeduplication("subquery - exists", 3, 24) {
    case ((left, leftKey), (right, rightKey)) =>
      left.filter(right.filter(rightKey === leftKey).exists())
  }

  testBinaryOperationDeduplication("subquery - scalar", 3, 24) {
    case ((left, _), (right, rightKey)) =>
      left.select(right.agg(min(rightKey)).scalar())
  }

  testBinaryOperationDeduplication("subquery - in", 8, 34, 0, 2) {
    case ((left, leftKey), (right, _)) =>
      left.filter(!leftKey.isin(right))
  }

  testBinaryOperationDeduplication("groupMap", 3, 22) {
    case ((left, leftKey), (right, rightKey)) =>
      val initialState = right.groupBy(rightKey).as[Long, Long]
      left
        .groupBy(leftKey)
        .as[Long, Long]
        .mapGroupsWithState(GroupStateTimeout.EventTimeTimeout(), initialState) {
          (key: Long, values: Iterator[Long], state: GroupState[Long]) =>
            (key, values.sum + state.get)
        }
  }

  testBinaryOperationDeduplication("coGroup", 3, 24) {
    case ((left, leftKey), (right, rightKey)) =>
      val leftKv = left.groupBy(leftKey).as[Long, Long]
      val rightKv = right.groupBy(rightKey).as[Long, Long]
      leftKv.cogroup(rightKv) {
        (key: Long, leftValues: Iterator[Long], rightValues: Iterator[Long]) =>
          leftValues.zipAll(rightValues, 0L, 0L).map { lr =>
            (key, lr._1 + lr._2)
          }
      }
  }

  test("optimize plan with duplicated relations - asOfJoin") {
    val input = spark.range(10).as("x")
    val id = ColumnNodeToProtoConverter.toExpr(input("id"))
    val relation = input.plan.getRoot
    val df = spark.newDataFrame { builder =>
      builder.getAsOfJoinBuilder
        .setLeft(relation)
        .setLeftAsOf(id)
        .setRight(relation)
        .setRightAsOf(id)
        .setDirection("backward")
        .setAllowExactMatches(true)
    }
    checkDeduplication(df, numRelationsReduction = -1, sizeReduction = 3)
  }

  test("optimize plan with duplicated relations - MLRelation - fetch") {
    val input = spark.range(10).as("x")
    val other = spark.read.format("parquet").load()
    val lit = proto.Expression.Literal.newBuilder().setLong(11L).build()
    val df = spark.newDataFrame { builder =>
      val fetch = builder.getMlRelationBuilder.getFetchBuilder
      fetch.getObjRefBuilder.setId("21345")
      fetch
        .addMethodsBuilder()
        .setMethod("discombobulate")
        .addArgs(proto.Fetch.Method.Args.newBuilder().setParam(lit))
        .addArgs(proto.Fetch.Method.Args.newBuilder().setInput(input.plan.getRoot))
      fetch
        .addMethodsBuilder()
        .setMethod("fluster")
        .addArgs(proto.Fetch.Method.Args.newBuilder().setInput(other.plan.getRoot))
        .addArgs(proto.Fetch.Method.Args.newBuilder().setInput(input.plan.getRoot))
    }
    checkDeduplication(df, numRelationsReduction = -1, sizeReduction = 6)
  }

  test("optimize plan with duplicated relations - subquery WithRelations rewrite") {
    val input1 = spark.range(10)
    val input2 = spark.emptyDataset[Long]
    val input3 = spark.range(1, 1, 1).as("ref")
    val df = input1
      .union(input3)
      .filter(
        col("id").isin(input1) &&
          col("id").isin(input2) &&
          col("id").isin(input3))
    checkDeduplication(df, numRelationsReduction = 0, sizeReduction = 19)

    // Check if the original WithRelations node is retained and has the proper references.
    val root = df.optimizedPlan.getRoot
    RelationTreeUtils.visit(root) { relation =>
      if (relation.hasWithRelations && (relation ne root)) {
        val withRelations = relation.getWithRelations
        assert(PlanId(df.plan.getRoot) == PlanId(relation))
        assert(withRelations.getReferencesCount == 1)
        assert(withRelations.getReferences(0) eq input2.plan.getRoot)
      }
      true
    }
  }

  test("optimize plan with duplicated relations - 64 duplicate local relations") {
    // Manually build a local relation.
    val input = spark.newDataFrame { builder =>
      val schema = new StructType()
        .add("key", "long")
        .add("value", "binary")
      val rng = new Random(61209389765L)
      val allocator = new RootAllocator()
      val byteString =
        try {
          ArrowSerializer.serialize(
            Iterator.tabulate(128) { i =>
              i.toLong -> rng.nextBytes(1024)
            },
            agnosticEncoderFor(Encoders.tuple(longEncoder, Encoders.BINARY)),
            allocator,
            timeZoneId = TimeZone.getDefault.toString,
            largeVarTypes = false)
        } finally {
          allocator.close()
        }
      builder.getLocalRelationBuilder.setSchema(schema.json).setData(byteString)
    }

    // Build a tree with massive duplication. It will contain 64 duplicate local relations.
    val df = Iterator.range(0, 6).foldLeft(input) { case (current, _) =>
      current.union(current)
    }
    // Optimization reduces size by 98.4%
    checkDeduplication(df, numRelationsReduction = 107, sizeReduction = 8396066)
  }

  private def join(input: Dataset[_], numJoins: Int): DataFrame = {
    Iterator.fill(numJoins + 1)(input.toDF()).reduce(_.join(_))
  }

  test("optimize can increase number of relations") {
    // Optimize can increase the number of relations in a plan. This happens when the number of
    // relations removed does not offset the addition of the references and the WithRelations node.

    // A single relation duplicated subtree. Optimization always adds two relations
    val input = spark.range(10)
    checkDeduplication(join(input, 1), numRelationsReduction = -2, sizeReduction = -7)
    checkDeduplication(join(input, 2), numRelationsReduction = -2, sizeReduction = 2)
    checkDeduplication(join(input, 3), numRelationsReduction = -2, sizeReduction = 11)
    checkDeduplication(join(input, 4), numRelationsReduction = -2, sizeReduction = 20)
    checkDeduplication(join(input, 5), numRelationsReduction = -2, sizeReduction = 30)

    // A 2-relation duplicated subtree. Optimization only adds a relation if there is a single
    // relation with 2 duplicates in the tree.
    val input2 = input.as("a")
    checkDeduplication(join(input2, 1), numRelationsReduction = -1, sizeReduction = 5)
    checkDeduplication(join(input2, 2), numRelationsReduction = 0, sizeReduction = 26)
    checkDeduplication(join(input2, 3), numRelationsReduction = 1, sizeReduction = 48)
    checkDeduplication(join(input2, 4), numRelationsReduction = 2, sizeReduction = 71)
    checkDeduplication(join(input2, 5), numRelationsReduction = 3, sizeReduction = 94)

    // A 3-relation duplicated subtree. Optimization always reduces the number of relations.
    val input3 = input2.select(col("id"))
    checkDeduplication(join(input3, 1), numRelationsReduction = 0, sizeReduction = 207)
    checkDeduplication(join(input3, 2), numRelationsReduction = 2, sizeReduction = 432)
    checkDeduplication(join(input3, 3), numRelationsReduction = 4, sizeReduction = 657)
  }

  test("optimize can increase the size of the plan") {
    // Optimize can increase the size of a plan. This happens when the size of the references and
    // the withRelations node is larger than the sum of the deduplicated relations.

    // An unknown relation is tiny (7-16 bytes depending on planId). That is smaller than the
    // WithRelations node (6-19 bytes depending on the withRelations planId, and the size of root
    // plan) and the two references (per reference 3-13 bytes depending on planId) added by the
    // optimization; as a result the size increases.
    val input1 = spark.newDataFrame { builder =>
      builder.getUnknownBuilder
    }

    // A single relation duplicated subtree. Optimization initially increases size.
    checkDeduplication(join(input1, 1), numRelationsReduction = -2, sizeReduction = -12)
    checkDeduplication(join(input1, 2), numRelationsReduction = -2, sizeReduction = -8)
    checkDeduplication(join(input1, 3), numRelationsReduction = -2, sizeReduction = -4)
    checkDeduplication(join(input1, 4), numRelationsReduction = -2, sizeReduction = 0)
    checkDeduplication(join(input1, 5), numRelationsReduction = -2, sizeReduction = 4)

    // A 2-relation duplicated subtree. Optimization almost always increases size.
    val input2 = input1.as("a")
    checkDeduplication(join(input2, 1), numRelationsReduction = -1, sizeReduction = 0)
    checkDeduplication(join(input2, 2), numRelationsReduction = 0, sizeReduction = 16)
    checkDeduplication(join(input2, 3), numRelationsReduction = 1, sizeReduction = 32)
  }
}
