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

package org.apache.spark.sql.connect.rdd

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.atomic.AtomicInteger

import com.google.protobuf.ByteString

import org.apache.spark.connect.proto
import org.apache.spark.resource.{ResourceProfileBuilder, TaskResourceRequests}
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.connect.common.InvalidPlanInput
import org.apache.spark.sql.connect.planner.{SparkConnectPlanner, SparkConnectPlanTest}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, IntegerType, StructField, StructType}

/** Tests the planning of the relations and the commands of the RDD API of the Python client. */
class RDDPlannerSuite extends SparkConnectPlanTest {

  private def relation(f: proto.RDDRelation.Builder => Unit): proto.Relation = {
    val builder = proto.RDDRelation.newBuilder()
    f(builder)
    proto.Relation.newBuilder().setRddRelation(builder).build()
  }

  /** Local data of one binary column. */
  private def binaryData(values: String*): proto.Relation = createLocalRelationProto(
    StructType(Seq(StructField("value", BinaryType))),
    values.map(v => InternalRow(v.getBytes(UTF_8))))

  private def parallelize(numSlices: Int, values: String*): proto.Relation = relation { r =>
    r.getParallelizeBuilder.setData(binaryData(values: _*)).setNumSlices(numSlices)
  }

  private def values(rel: proto.Relation): Seq[Seq[String]] = {
    val plan = transform(rel)
    Dataset
      .ofRows(spark, plan)
      .queryExecution
      .toRdd
      .map(_.getBinary(0))
      .glom()
      .collect()
      .map(_.map(new String(_, UTF_8)).toSeq)
      .toSeq
  }

  test("parallelize") {
    val rel = parallelize(3, "a", "b", "c", "d")
    assert(transform(rel).isInstanceOf[PythonRDDParallelize])
    assert(values(rel) === Seq(Seq("a"), Seq("b"), Seq("c", "d")))
  }

  test("parallelize rejects data that is not one binary column") {
    val data = createLocalRelationProto(
      StructType(Seq(StructField("n", IntegerType))),
      Seq(InternalRow(1)))
    val rel = relation(_.getParallelizeBuilder.setData(data).setNumSlices(1))
    checkError(
      exception = intercept[InvalidPlanInput](transform(rel)),
      condition = "CONNECT_INVALID_PLAN.RDD_PARALLELIZE_INVALID_DATA",
      parameters = Map("schema" -> "\"STRUCT<n: INT>\""))
  }

  test("union, coalesce and zip of partitions") {
    val union = relation { r =>
      r.getUnionBuilder.addInputs(parallelize(1)).addInputs(parallelize(2, "a", "b"))
    }
    assert(values(union) === Seq(Seq(), Seq("a"), Seq("b")))
    assert(values(relation(_.getUnionBuilder)) === Seq.empty)

    val coalesce = relation(_.getCoalesceBuilder.setInput(union).setNumPartitions(1))
    assert(transform(coalesce).isInstanceOf[PythonRDDCoalesce])
    assert(values(coalesce) === Seq(Seq("a", "b")))

    val concatenate = relation { r =>
      r.getZipPartitionsBuilder
        .addInputs(parallelize(2, "a", "b"))
        .addInputs(parallelize(2, "c", "d"))
        .setConcatenate(true)
    }
    assert(values(concatenate) === Seq(Seq("a", "c"), Seq("b", "d")))
  }

  test("zip takes two inputs") {
    val rel = relation { r =>
      r.getZipPartitionsBuilder
        .addInputs(parallelize(1))
        .addInputs(parallelize(1))
        .addInputs(parallelize(1))
    }
    checkError(
      exception = intercept[InvalidPlanInput](transform(rel)),
      condition = "CONNECT_INVALID_PLAN.RDD_ZIP_INVALID_INPUTS",
      parameters = Map("numInputs" -> "3"))
  }

  test("map partitions runs a Python function with the environment of PySpark") {
    val rel = relation { r =>
      r.getMapPartitionsBuilder
        .setInput(parallelize(1))
        .setCommand(ByteString.copyFrom(Array[Byte](1, 2, 3)))
        .setPythonVer("3.11")
        .setPythonHashSeed("42")
        .setOutputPairs(true)
    }
    withSQLConf(SQLConf.PYTHON_UDF_PROFILER.key -> "perf") {
      val plan = transform(rel).asInstanceOf[PythonRDDMapPartitions]
      assert(plan.outputPairs)
      assert(plan.output.map(_.dataType) === Seq(IntegerType, BinaryType))
      assert(plan.func.command === Seq[Byte](1, 2, 3))
      assert(plan.func.pythonVer === "3.11")
      assert(plan.func.envVars.get("PYTHONHASHSEED") === "42")
      assert(plan.func.envVars.get("PYSPARK_CONNECT_RDD_PROFILER") === "perf")
      assert(!plan.isBarrier)
    }
    val plan = transform(rel).asInstanceOf[PythonRDDMapPartitions]
    assert(!plan.func.envVars.containsKey("PYSPARK_CONNECT_RDD_PROFILER"))
  }

  test("shuffle, values and elements") {
    val pairs = createLocalRelationProto(
      StructType(Seq(StructField("key", IntegerType), StructField("value", BinaryType))),
      Seq(InternalRow(1, "a".getBytes(UTF_8)), InternalRow(0, "b".getBytes(UTF_8))))
    val shuffle = relation(_.getShuffleBuilder.setInput(pairs).setNumPartitions(3))
    val shuffled = relation(_.getValuesBuilder.setInput(shuffle))
    assert(values(shuffled) === Seq(Seq("b"), Seq("a"), Seq()))

    val elements = relation { r =>
      r.getElementsBuilder
        .setInput(shuffled)
        .setPartitions(proto.Ints.newBuilder().addValues(1))
    }
    val plan = transform(elements).asInstanceOf[PythonRDDElements]
    assert(plan.partitions === Some(Seq(1)))
    val rows = Dataset.ofRows(spark, plan).collect()
    assert(rows.map(_.getInt(0)).toSeq === Seq(1))
  }

  test("with resources looks up the resource profile of its ID") {
    val profile = new ResourceProfileBuilder().require(new TaskResourceRequests().cpus(1)).build()
    spark.sparkContext.resourceProfileManager.addResourceProfile(profile)
    val rel = relation { r =>
      r.getWithResourcesBuilder.setInput(parallelize(2, "a", "b")).setProfileId(profile.id)
    }
    val plan = transform(rel).asInstanceOf[PythonRDDWithResources]
    assert(plan.profile === profile)
    assert(values(rel) === Seq(Seq("a"), Seq("b")))
  }

  test("to DataFrame takes a struct type") {
    val rel = relation { r =>
      r.getToDataFrameBuilder
        .setInput(parallelize(1))
        .setSchema(proto.DataType.newBuilder().setInteger(proto.DataType.Integer.newBuilder()))
    }
    checkError(
      exception = intercept[InvalidPlanInput](transform(rel)),
      condition = "INVALID_SCHEMA_TYPE_NON_STRUCT",
      parameters = Map("dataType" -> "\"INT\""))
  }

  test("describe returns the number of partitions") {
    val planner = new SparkConnectPlanner(SparkConnectTestUtils.createDummySessionHolder(spark))
    val handler = new RDDCommandHandler(planner)
    def describe(rel: proto.Relation): Int = {
      val command = proto.RDDCommand.newBuilder()
      command.getDescribeBuilder.setRdd(rel)
      handler.handle(command.build()).getInfo.getNumPartitions
    }
    assert(describe(parallelize(4)) === 4)
    // A relation that is not an RDD of the RDD API is planned to count its partitions.
    val data = binaryData("a", "b")
    val expected = Dataset.ofRows(spark, transform(data)).queryExecution.toRdd.getNumPartitions
    assert(describe(data) === expected)
  }

  test("pin plans an RDD once, so that the jobs on it share its shuffles") {
    val holder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val handler = new RDDCommandHandler(new SparkConnectPlanner(holder))
    val pairs = createLocalRelationProto(
      StructType(Seq(StructField("key", IntegerType), StructField("value", BinaryType))),
      Seq(InternalRow(1, "a".getBytes(UTF_8)), InternalRow(0, "b".getBytes(UTF_8))))
    val shuffle = relation(_.getShuffleBuilder.setInput(pairs).setNumPartitions(3))
    val command = proto.RDDCommand.newBuilder()
    command.getPinBuilder.setRdd(relation(_.getValuesBuilder.setInput(shuffle)))

    val mapStages = new AtomicInteger()
    val listener = new SparkListener {
      override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
        if (event.stageInfo.shuffleDepId.isDefined) mapStages.incrementAndGet()
      }
    }
    spark.sparkContext.listenerBus.waitUntilEmpty()
    spark.sparkContext.addSparkListener(listener)
    try {
      val result = handler.handle(command.build()).getPinned
      assert(result.getNumPartitions === 3)
      val pinned = holder.getDataFrameOrThrow(result.getRelation.getRelationId)
      assert(pinned.asInstanceOf[Dataset[_]].logicalPlan.isInstanceOf[LogicalRDD])
      // Two jobs on the pinned plan.
      for (_ <- 0 until 2) {
        val values = pinned.collect().map(r => new String(r.getAs[Array[Byte]](0), UTF_8))
        assert(values.toSeq === Seq("b", "a"))
      }
      spark.sparkContext.listenerBus.waitUntilEmpty()
      assert(mapStages.get() === 1)
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  test("context info") {
    val planner = new SparkConnectPlanner(SparkConnectTestUtils.createDummySessionHolder(spark))
    val command = proto.RDDCommand.newBuilder()
    command.getGetContextInfoBuilder
    val info = new RDDCommandHandler(planner).handle(command.build()).getContextInfo
    assert(info.getDefaultParallelism === spark.sparkContext.defaultParallelism)
    assert(info.getStartTime === spark.sparkContext.startTime)
    assert(info.getPythonWorkerMemory === "512m")
  }
}
