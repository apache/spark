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

package org.apache.spark.sql.execution.metric

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import scala.collection.mutable

import org.apache.xbean.asm5._
import org.apache.xbean.asm5.Opcodes._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql._
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.SparkPlanGraph
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.{AccumulatorContext, JsonProtocol, Utils}


class SQLMetricsSuite extends SparkFunSuite with SharedSQLContext {
  import testImplicits._

  test("SQLMetric should not box Long") {
    val l = SQLMetrics.createMetric(sparkContext, "long")
    val f = () => {
      l += 1L
      l.add(1L)
    }
    val cl = BoxingFinder.getClassReader(f.getClass)
    val boxingFinder = new BoxingFinder()
    cl.accept(boxingFinder, 0)
    assert(boxingFinder.boxingInvokes.isEmpty, s"Found boxing: ${boxingFinder.boxingInvokes}")
  }

  test("Normal accumulator should do boxing") {
    // We need this test to make sure BoxingFinder works.
    val l = sparkContext.accumulator(0L)
    val f = () => { l += 1L }
    val cl = BoxingFinder.getClassReader(f.getClass)
    val boxingFinder = new BoxingFinder()
    cl.accept(boxingFinder, 0)
    assert(boxingFinder.boxingInvokes.nonEmpty, "Found find boxing in this test")
  }

  /**
   * Call `df.collect()` and verify if the collected metrics are same as "expectedMetrics".
   *
   * @param df `DataFrame` to run
   * @param expectedNumOfJobs number of jobs that will run
   * @param expectedMetrics the expected metrics. The format is
   *                        `nodeId -> (operatorName, metric name -> metric value)`.
   */
  private def testSparkPlanMetrics(
      df: DataFrame,
      expectedNumOfJobs: Int,
      expectedMetrics: Map[Long, (String, Map[String, Any])]): Unit = {
    val previousExecutionIds = spark.listener.executionIdToData.keySet
    withSQLConf("spark.sql.codegen.wholeStage" -> "false") {
      df.collect()
    }
    sparkContext.listenerBus.waitUntilEmpty(10000)
    val executionIds = spark.listener.executionIdToData.keySet.diff(previousExecutionIds)
    assert(executionIds.size === 1)
    val executionId = executionIds.head
    val jobs = spark.listener.getExecution(executionId).get.jobs
    // Use "<=" because there is a race condition that we may miss some jobs
    // TODO Change it to "=" once we fix the race condition that missing the JobStarted event.
    assert(jobs.size <= expectedNumOfJobs)
    if (jobs.size == expectedNumOfJobs) {
      // If we can track all jobs, check the metric values
      val metricValues = spark.listener.getExecutionMetrics(executionId)
      val actualMetrics = SparkPlanGraph(SparkPlanInfo.fromSparkPlan(
        df.queryExecution.executedPlan)).allNodes.filter { node =>
        expectedMetrics.contains(node.id)
      }.map { node =>
        val nodeMetrics = node.metrics.map { metric =>
          val metricValue = metricValues(metric.accumulatorId)
          (metric.name, metricValue)
        }.toMap
        (node.id, node.name -> nodeMetrics)
      }.toMap

      assert(expectedMetrics.keySet === actualMetrics.keySet)
      for (nodeId <- expectedMetrics.keySet) {
        val (expectedNodeName, expectedMetricsMap) = expectedMetrics(nodeId)
        val (actualNodeName, actualMetricsMap) = actualMetrics(nodeId)
        assert(expectedNodeName === actualNodeName)
        for (metricName <- expectedMetricsMap.keySet) {
          assert(expectedMetricsMap(metricName).toString === actualMetricsMap(metricName))
        }
      }
    } else {
      // TODO Remove this "else" once we fix the race condition that missing the JobStarted event.
      // Since we cannot track all jobs, the metric values could be wrong and we should not check
      // them.
      logWarning("Due to a race condition, we miss some jobs and cannot verify the metric values")
    }
  }

  test("Filter metrics") {
    // Assume the execution plan is
    // PhysicalRDD(nodeId = 1) -> Filter(nodeId = 0)
    val df = person.filter('age < 25)
    testSparkPlanMetrics(df, 1, Map(
      0L -> ("Filter", Map(
        "number of output rows" -> 1L)))
    )
  }

  test("WholeStageCodegen metrics") {
    // Assume the execution plan is
    // WholeStageCodegen(nodeId = 0, Range(nodeId = 2) -> Filter(nodeId = 1))
    // TODO: update metrics in generated operators
    val ds = spark.range(10).filter('id < 5)
    testSparkPlanMetrics(ds.toDF(), 1, Map.empty)
  }

  test("TungstenAggregate metrics") {
    // Assume the execution plan is
    // ... -> TungstenAggregate(nodeId = 2) -> Exchange(nodeId = 1)
    // -> TungstenAggregate(nodeId = 0)
    val df = testData2.groupBy().count() // 2 partitions
    testSparkPlanMetrics(df, 1, Map(
      2L -> ("TungstenAggregate", Map(
        "number of output rows" -> 2L)),
      0L -> ("TungstenAggregate", Map(
        "number of output rows" -> 1L)))
    )

    // 2 partitions and each partition contains 2 keys
    val df2 = testData2.groupBy('a).count()
    testSparkPlanMetrics(df2, 1, Map(
      2L -> ("TungstenAggregate", Map(
        "number of output rows" -> 4L)),
      0L -> ("TungstenAggregate", Map(
        "number of output rows" -> 3L)))
    )
  }

  test("Sort metrics") {
    // Assume the execution plan is
    // WholeStageCodegen(nodeId = 0, Range(nodeId = 2) -> Sort(nodeId = 1))
    val ds = spark.range(10).sort('id)
    testSparkPlanMetrics(ds.toDF(), 2, Map.empty)
  }

  test("SortMergeJoin metrics") {
    // Because SortMergeJoin may skip different rows if the number of partitions is different, this
    // test should use the deterministic number of partitions.
    val testDataForJoin = testData2.filter('a < 2) // TestData2(1, 1) :: TestData2(1, 2)
    testDataForJoin.createOrReplaceTempView("testDataForJoin")
    withTempTable("testDataForJoin") {
      // Assume the execution plan is
      // ... -> SortMergeJoin(nodeId = 1) -> TungstenProject(nodeId = 0)
      val df = spark.sql(
        "SELECT * FROM testData2 JOIN testDataForJoin ON testData2.a = testDataForJoin.a")
      testSparkPlanMetrics(df, 1, Map(
        0L -> ("SortMergeJoin", Map(
          // It's 4 because we only read 3 rows in the first partition and 1 row in the second one
          "number of output rows" -> 4L)))
      )
    }
  }

  test("SortMergeJoin(outer) metrics") {
    // Because SortMergeJoin may skip different rows if the number of partitions is different,
    // this test should use the deterministic number of partitions.
    val testDataForJoin = testData2.filter('a < 2) // TestData2(1, 1) :: TestData2(1, 2)
    testDataForJoin.createOrReplaceTempView("testDataForJoin")
    withTempTable("testDataForJoin") {
      // Assume the execution plan is
      // ... -> SortMergeJoin(nodeId = 1) -> TungstenProject(nodeId = 0)
      val df = spark.sql(
        "SELECT * FROM testData2 left JOIN testDataForJoin ON testData2.a = testDataForJoin.a")
      testSparkPlanMetrics(df, 1, Map(
        0L -> ("SortMergeJoin", Map(
          // It's 4 because we only read 3 rows in the first partition and 1 row in the second one
          "number of output rows" -> 8L)))
      )

      val df2 = spark.sql(
        "SELECT * FROM testDataForJoin right JOIN testData2 ON testData2.a = testDataForJoin.a")
      testSparkPlanMetrics(df2, 1, Map(
        0L -> ("SortMergeJoin", Map(
          // It's 4 because we only read 3 rows in the first partition and 1 row in the second one
          "number of output rows" -> 8L)))
      )
    }
  }

  test("BroadcastHashJoin metrics") {
    val df1 = Seq((1, "1"), (2, "2")).toDF("key", "value")
    val df2 = Seq((1, "1"), (2, "2"), (3, "3"), (4, "4")).toDF("key", "value")
    // Assume the execution plan is
    // ... -> BroadcastHashJoin(nodeId = 1) -> TungstenProject(nodeId = 0)
    val df = df1.join(broadcast(df2), "key")
    testSparkPlanMetrics(df, 2, Map(
      1L -> ("BroadcastHashJoin", Map(
        "number of output rows" -> 2L)))
    )
  }

  test("BroadcastHashJoin(outer) metrics") {
    val df1 = Seq((1, "a"), (1, "b"), (4, "c")).toDF("key", "value")
    val df2 = Seq((1, "a"), (1, "b"), (2, "c"), (3, "d")).toDF("key2", "value")
    // Assume the execution plan is
    // ... -> BroadcastHashJoin(nodeId = 0)
    val df = df1.join(broadcast(df2), $"key" === $"key2", "left_outer")
    testSparkPlanMetrics(df, 2, Map(
      0L -> ("BroadcastHashJoin", Map(
        "number of output rows" -> 5L)))
    )

    val df3 = df1.join(broadcast(df2), $"key" === $"key2", "right_outer")
    testSparkPlanMetrics(df3, 2, Map(
      0L -> ("BroadcastHashJoin", Map(
        "number of output rows" -> 6L)))
    )
  }

  test("BroadcastNestedLoopJoin metrics") {
    val testDataForJoin = testData2.filter('a < 2) // TestData2(1, 1) :: TestData2(1, 2)
    testDataForJoin.createOrReplaceTempView("testDataForJoin")
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      withTempTable("testDataForJoin") {
        // Assume the execution plan is
        // ... -> BroadcastNestedLoopJoin(nodeId = 1) -> TungstenProject(nodeId = 0)
        val df = spark.sql(
          "SELECT * FROM testData2 left JOIN testDataForJoin ON " +
            "testData2.a * testDataForJoin.a != testData2.a + testDataForJoin.a")
        testSparkPlanMetrics(df, 3, Map(
          1L -> ("BroadcastNestedLoopJoin", Map(
            "number of output rows" -> 12L)))
        )
      }
    }
  }

  test("BroadcastLeftSemiJoinHash metrics") {
    val df1 = Seq((1, "1"), (2, "2")).toDF("key", "value")
    val df2 = Seq((1, "1"), (2, "2"), (3, "3"), (4, "4")).toDF("key2", "value")
    // Assume the execution plan is
    // ... -> BroadcastHashJoin(nodeId = 0)
    val df = df1.join(broadcast(df2), $"key" === $"key2", "leftsemi")
    testSparkPlanMetrics(df, 2, Map(
      0L -> ("BroadcastHashJoin", Map(
        "number of output rows" -> 2L)))
    )
  }

  test("CartesianProduct metrics") {
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      val testDataForJoin = testData2.filter('a < 2) // TestData2(1, 1) :: TestData2(1, 2)
      testDataForJoin.createOrReplaceTempView("testDataForJoin")
      withTempTable("testDataForJoin") {
        // Assume the execution plan is
        // ... -> CartesianProduct(nodeId = 1) -> TungstenProject(nodeId = 0)
        val df = spark.sql(
          "SELECT * FROM testData2 JOIN testDataForJoin")
        testSparkPlanMetrics(df, 1, Map(
          0L -> ("CartesianProduct", Map("number of output rows" -> 12L)))
        )
      }
    }
  }

  test("save metrics") {
    withTempPath { file =>
      val previousExecutionIds = spark.listener.executionIdToData.keySet
      // Assume the execution plan is
      // PhysicalRDD(nodeId = 0)
      person.select('name).write.format("json").save(file.getAbsolutePath)
      sparkContext.listenerBus.waitUntilEmpty(10000)
      val executionIds = spark.listener.executionIdToData.keySet.diff(previousExecutionIds)
      assert(executionIds.size === 1)
      val executionId = executionIds.head
      val jobs = spark.listener.getExecution(executionId).get.jobs
      // Use "<=" because there is a race condition that we may miss some jobs
      // TODO Change "<=" to "=" once we fix the race condition that missing the JobStarted event.
      assert(jobs.size <= 1)
      val metricValues = spark.listener.getExecutionMetrics(executionId)
      // Because "save" will create a new DataFrame internally, we cannot get the real metric id.
      // However, we still can check the value.
      assert(metricValues.values.toSeq.exists(_ === "2"))
    }
  }

  test("metrics can be loaded by history server") {
    val metric = SQLMetrics.createMetric(sparkContext, "zanzibar")
    metric += 10L
    val metricInfo = metric.toInfo(Some(metric.value), None)
    metricInfo.update match {
      case Some(v: Long) => assert(v === 10L)
      case Some(v) => fail(s"metric value was not a Long: ${v.getClass.getName}")
      case _ => fail("metric update is missing")
    }
    assert(metricInfo.metadata === Some(AccumulatorContext.SQL_ACCUM_IDENTIFIER))
    // After serializing to JSON, the original value type is lost, but we can still
    // identify that it's a SQL metric from the metadata
    val metricInfoJson = JsonProtocol.accumulableInfoToJson(metricInfo)
    val metricInfoDeser = JsonProtocol.accumulableInfoFromJson(metricInfoJson)
    metricInfoDeser.update match {
      case Some(v: String) => assert(v.toLong === 10L)
      case Some(v) => fail(s"deserialized metric value was not a string: ${v.getClass.getName}")
      case _ => fail("deserialized metric update is missing")
    }
    assert(metricInfoDeser.metadata === Some(AccumulatorContext.SQL_ACCUM_IDENTIFIER))
  }

}

private case class MethodIdentifier[T](cls: Class[T], name: String, desc: String)

/**
 * If `method` is null, search all methods of this class recursively to find if they do some boxing.
 * If `method` is specified, only search this method of the class to speed up the searching.
 *
 * This method will skip the methods in `visitedMethods` to avoid potential infinite cycles.
 */
private class BoxingFinder(
    method: MethodIdentifier[_] = null,
    val boxingInvokes: mutable.Set[String] = mutable.Set.empty,
    visitedMethods: mutable.Set[MethodIdentifier[_]] = mutable.Set.empty)
  extends ClassVisitor(ASM5) {

  private val primitiveBoxingClassName =
    Set("java/lang/Long",
      "java/lang/Double",
      "java/lang/Integer",
      "java/lang/Float",
      "java/lang/Short",
      "java/lang/Character",
      "java/lang/Byte",
      "java/lang/Boolean")

  override def visitMethod(
      access: Int, name: String, desc: String, sig: String, exceptions: Array[String]):
    MethodVisitor = {
    if (method != null && (method.name != name || method.desc != desc)) {
      // If method is specified, skip other methods.
      return new MethodVisitor(ASM5) {}
    }

    new MethodVisitor(ASM5) {
      override def visitMethodInsn(
          op: Int, owner: String, name: String, desc: String, itf: Boolean) {
        if (op == INVOKESPECIAL && name == "<init>" || op == INVOKESTATIC && name == "valueOf") {
          if (primitiveBoxingClassName.contains(owner)) {
            // Find boxing methods, e.g, new java.lang.Long(l) or java.lang.Long.valueOf(l)
            boxingInvokes.add(s"$owner.$name")
          }
        } else {
          // scalastyle:off classforname
          val classOfMethodOwner = Class.forName(owner.replace('/', '.'), false,
            Thread.currentThread.getContextClassLoader)
          // scalastyle:on classforname
          val m = MethodIdentifier(classOfMethodOwner, name, desc)
          if (!visitedMethods.contains(m)) {
            // Keep track of visited methods to avoid potential infinite cycles
            visitedMethods += m
            val cl = BoxingFinder.getClassReader(classOfMethodOwner)
            visitedMethods += m
            cl.accept(new BoxingFinder(m, boxingInvokes, visitedMethods), 0)
          }
        }
      }
    }
  }
}

private object BoxingFinder {

  def getClassReader(cls: Class[_]): ClassReader = {
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    val resourceStream = cls.getResourceAsStream(className)
    val baos = new ByteArrayOutputStream(128)
    // Copy data over, before delegating to ClassReader -
    // else we can run out of open file handles.
    Utils.copyStream(resourceStream, baos, true)
    new ClassReader(new ByteArrayInputStream(baos.toByteArray))
  }

}
