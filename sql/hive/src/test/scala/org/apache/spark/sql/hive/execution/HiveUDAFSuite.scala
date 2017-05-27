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

package org.apache.spark.sql.hive.execution

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.ql.udf.UDAFPercentile
import org.apache.hadoop.hive.ql.udf.generic.{AbstractGenericUDAFResolver, GenericUDAFEvaluator, GenericUDAFMax}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.{AggregationBuffer, Mode}
import org.apache.hadoop.hive.ql.util.JavaDataModel
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class HiveUDAFSuite extends QueryTest with TestHiveSingleton with SQLTestUtils {
  import testImplicits._

  protected override def beforeAll(): Unit = {
    sql(s"CREATE TEMPORARY FUNCTION mock AS '${classOf[MockUDAF].getName}'")
    sql(s"CREATE TEMPORARY FUNCTION hive_max AS '${classOf[GenericUDAFMax].getName}'")

    Seq(
      (0: Integer) -> "val_0",
      (1: Integer) -> "val_1",
      (2: Integer) -> null,
      (3: Integer) -> null
    ).toDF("key", "value").repartition(2).createOrReplaceTempView("t")
  }

  protected override def afterAll(): Unit = {
    sql(s"DROP TEMPORARY FUNCTION IF EXISTS mock")
    sql(s"DROP TEMPORARY FUNCTION IF EXISTS hive_max")
  }

  test("built-in Hive UDAF") {
    val df = sql("SELECT key % 2, hive_max(key) FROM t GROUP BY key % 2")

    val aggs = df.queryExecution.executedPlan.collect {
      case agg: ObjectHashAggregateExec => agg
    }

    // There should be two aggregate operators, one for partial aggregation, and the other for
    // global aggregation.
    assert(aggs.length == 2)

    checkAnswer(df, Seq(
      Row(0, 2),
      Row(1, 3)
    ))
  }

  test("customized Hive UDAF") {
    val df = sql("SELECT key % 2, mock(value) FROM t GROUP BY key % 2")

    val aggs = df.queryExecution.executedPlan.collect {
      case agg: ObjectHashAggregateExec => agg
    }

    // There should be two aggregate operators, one for partial aggregation, and the other for
    // global aggregation.
    assert(aggs.length == 2)

    checkAnswer(df, Seq(
      Row(0, Row(1, 1)),
      Row(1, Row(1, 1))
    ))
  }

  test("non-deterministic children expressions of UDAF") {
    withTempView("view1") {
      spark.range(1).selectExpr("id as x", "id as y").createTempView("view1")
      withUserDefinedFunction("testUDAFPercentile" -> true) {
        // non-deterministic children of Hive UDAF
        sql(s"CREATE TEMPORARY FUNCTION testUDAFPercentile AS '${classOf[UDAFPercentile].getName}'")
        val e1 = intercept[AnalysisException] {
          sql("SELECT testUDAFPercentile(x, rand()) from view1 group by y")
        }.getMessage
        assert(Seq("nondeterministic expression",
          "should not appear in the arguments of an aggregate function").forall(e1.contains))
      }
    }
  }
}

/**
 * A testing Hive UDAF that computes the counts of both non-null values and nulls of a given column.
 */
class MockUDAF extends AbstractGenericUDAFResolver {
  override def getEvaluator(info: Array[TypeInfo]): GenericUDAFEvaluator = new MockUDAFEvaluator
}

class MockUDAFBuffer(var nonNullCount: Long, var nullCount: Long)
  extends GenericUDAFEvaluator.AbstractAggregationBuffer {

  override def estimate(): Int = JavaDataModel.PRIMITIVES2 * 2
}

class MockUDAFEvaluator extends GenericUDAFEvaluator {
  private val nonNullCountOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector

  private val nullCountOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector

  private val bufferOI = {
    val fieldNames = Seq("nonNullCount", "nullCount").asJava
    val fieldOIs = Seq(nonNullCountOI: ObjectInspector, nullCountOI: ObjectInspector).asJava
    ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs)
  }

  private val nonNullCountField = bufferOI.getStructFieldRef("nonNullCount")

  private val nullCountField = bufferOI.getStructFieldRef("nullCount")

  override def getNewAggregationBuffer: AggregationBuffer = new MockUDAFBuffer(0L, 0L)

  override def reset(agg: AggregationBuffer): Unit = {
    val buffer = agg.asInstanceOf[MockUDAFBuffer]
    buffer.nonNullCount = 0L
    buffer.nullCount = 0L
  }

  override def init(mode: Mode, parameters: Array[ObjectInspector]): ObjectInspector = bufferOI

  override def iterate(agg: AggregationBuffer, parameters: Array[AnyRef]): Unit = {
    val buffer = agg.asInstanceOf[MockUDAFBuffer]
    if (parameters.head eq null) {
      buffer.nullCount += 1L
    } else {
      buffer.nonNullCount += 1L
    }
  }

  override def merge(agg: AggregationBuffer, partial: Object): Unit = {
    if (partial ne null) {
      val nonNullCount = nonNullCountOI.get(bufferOI.getStructFieldData(partial, nonNullCountField))
      val nullCount = nullCountOI.get(bufferOI.getStructFieldData(partial, nullCountField))
      val buffer = agg.asInstanceOf[MockUDAFBuffer]
      buffer.nonNullCount += nonNullCount
      buffer.nullCount += nullCount
    }
  }

  override def terminatePartial(agg: AggregationBuffer): AnyRef = {
    val buffer = agg.asInstanceOf[MockUDAFBuffer]
    Array[Object](buffer.nonNullCount: java.lang.Long, buffer.nullCount: java.lang.Long)
  }

  override def terminate(agg: AggregationBuffer): AnyRef = terminatePartial(agg)
}
