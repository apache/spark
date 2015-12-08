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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Attribute, Literal, IsNull}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.apache.spark.unsafe.types.UTF8String

class RowFormatConvertersSuite extends SparkPlanTest with SharedSQLContext {

  private def getConverters(plan: SparkPlan): Seq[SparkPlan] = plan.collect {
    case c: ConvertToUnsafe => c
    case c: ConvertToSafe => c
  }

  private val outputsSafe = ReferenceSort(Nil, false, PhysicalRDD(Seq.empty, null, "name"))
  assert(!outputsSafe.outputsUnsafeRows)
  private val outputsUnsafe = Sort(Nil, false, PhysicalRDD(Seq.empty, null, "name"))
  assert(outputsUnsafe.outputsUnsafeRows)

  test("planner should insert unsafe->safe conversions when required") {
    val plan = Limit(10, outputsUnsafe)
    val preparedPlan = sqlContext.prepareForExecution.execute(plan)
    assert(preparedPlan.children.head.isInstanceOf[ConvertToSafe])
  }

  test("filter can process unsafe rows") {
    val plan = Filter(IsNull(IsNull(Literal(1))), outputsUnsafe)
    val preparedPlan = sqlContext.prepareForExecution.execute(plan)
    assert(getConverters(preparedPlan).size === 1)
    assert(preparedPlan.outputsUnsafeRows)
  }

  test("filter can process safe rows") {
    val plan = Filter(IsNull(IsNull(Literal(1))), outputsSafe)
    val preparedPlan = sqlContext.prepareForExecution.execute(plan)
    assert(getConverters(preparedPlan).isEmpty)
    assert(!preparedPlan.outputsUnsafeRows)
  }

  test("execute() fails an assertion if inputs rows are of different formats") {
    val e = intercept[AssertionError] {
      Union(Seq(outputsSafe, outputsUnsafe)).execute()
    }
    assert(e.getMessage.contains("format"))
  }

  test("union requires all of its input rows' formats to agree") {
    val plan = Union(Seq(outputsSafe, outputsUnsafe))
    assert(plan.canProcessSafeRows && plan.canProcessUnsafeRows)
    val preparedPlan = sqlContext.prepareForExecution.execute(plan)
    assert(preparedPlan.outputsUnsafeRows)
  }

  test("union can process safe rows") {
    val plan = Union(Seq(outputsSafe, outputsSafe))
    val preparedPlan = sqlContext.prepareForExecution.execute(plan)
    assert(!preparedPlan.outputsUnsafeRows)
  }

  test("union can process unsafe rows") {
    val plan = Union(Seq(outputsUnsafe, outputsUnsafe))
    val preparedPlan = sqlContext.prepareForExecution.execute(plan)
    assert(preparedPlan.outputsUnsafeRows)
  }

  test("round trip with ConvertToUnsafe and ConvertToSafe") {
    val input = Seq(("hello", 1), ("world", 2))
    checkAnswer(
      sqlContext.createDataFrame(input),
      plan => ConvertToSafe(ConvertToUnsafe(plan)),
      input.map(Row.fromTuple)
    )
  }

  test("SPARK-9683: copy UTF8String when convert unsafe array/map to safe") {
    SQLContext.setActive(sqlContext)
    val schema = ArrayType(StringType)
    val rows = (1 to 100).map { i =>
      InternalRow(new GenericArrayData(Array[Any](UTF8String.fromString(i.toString))))
    }
    val relation = LocalTableScan(Seq(AttributeReference("t", schema)()), rows)

    val plan =
      DummyPlan(
        ConvertToSafe(
          ConvertToUnsafe(relation)))
    assert(plan.execute().collect().map(_.getUTF8String(0).toString) === (1 to 100).map(_.toString))
  }
}

case class DummyPlan(child: SparkPlan) extends UnaryNode {

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitions { iter =>
      // This `DummyPlan` is in safe mode, so we don't need to do copy even we hold some
      // values gotten from the incoming rows.
      // we cache all strings here to make sure we have deep copied UTF8String inside incoming
      // safe InternalRow.
      val strings = new scala.collection.mutable.ArrayBuffer[UTF8String]
      iter.foreach { row =>
        strings += row.getArray(0).getUTF8String(0)
      }
      strings.map(InternalRow(_)).iterator
    }
  }

  override def output: Seq[Attribute] = Seq(AttributeReference("a", StringType)())
}
