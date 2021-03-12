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

package org.apache.spark.sql.execution.streaming.state

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.GroupStateImpl._
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types._


class FlatMapGroupsWithStateExecHelperSuite extends StreamTest {

  import testImplicits._
  import FlatMapGroupsWithStateExecHelper._

  // ============================ StateManagerImplV1 ============================

  test(s"StateManager v1 - primitive type - without timestamp") {
    val schema = new StructType().add("value", IntegerType, nullable = false)
    testStateManagerWithoutTimestamp[Int](version = 1, schema, Seq(0, 10))
  }

  test(s"StateManager v1 - primitive type - with timestamp") {
    val schema = new StructType()
      .add("value", IntegerType, nullable = false)
      .add("timeoutTimestamp", IntegerType, nullable = false)
    testStateManagerWithTimestamp[Int](version = 1, schema, Seq(0, 10))
  }

  test(s"StateManager v1 - nested type - without timestamp") {
    val schema = StructType(Seq(
      StructField("i", IntegerType, nullable = false),
      StructField("nested", StructType(Seq(
        StructField("d", DoubleType, nullable = false),
        StructField("str", StringType))
      ))
    ))

    val testValues = Seq(
      NestedStruct(1, Struct(1.0, "someString")),
      NestedStruct(0, Struct(0.0, "")),
      NestedStruct(0, null))

    testStateManagerWithoutTimestamp[NestedStruct](version = 1, schema, testValues)

    // Verify the limitation of v1 with null state
    intercept[Exception] {
      testStateManagerWithoutTimestamp[NestedStruct](version = 1, schema, testValues = Seq(null))
    }
  }

  test(s"StateManager v1 - nested type - with timestamp") {
    val schema = StructType(Seq(
      StructField("i", IntegerType, nullable = false),
      StructField("nested", StructType(Seq(
        StructField("d", DoubleType, nullable = false),
        StructField("str", StringType))
      )),
      StructField("timeoutTimestamp", IntegerType, nullable = false)
    ))

    val testValues = Seq(
      NestedStruct(1, Struct(1.0, "someString")),
      NestedStruct(0, Struct(0.0, "")),
      NestedStruct(0, null))

    testStateManagerWithTimestamp[NestedStruct](version = 1, schema, testValues)

    // Verify the limitation of v1 with null state
    intercept[Exception] {
      testStateManagerWithTimestamp[NestedStruct](version = 1, schema, testValues = Seq(null))
    }
  }

  // ============================ StateManagerImplV2 ============================

  test(s"StateManager v2 - primitive type - without timestamp") {
    val schema = new StructType()
      .add("groupState", new StructType().add("value", IntegerType, nullable = false))
    testStateManagerWithoutTimestamp[Int](version = 2, schema, Seq(0, 10))
  }

  test(s"StateManager v2 - primitive type - with timestamp") {
    val schema = new StructType()
      .add("groupState", new StructType().add("value", IntegerType, nullable = false))
      .add("timeoutTimestamp", LongType, nullable = false)
    testStateManagerWithTimestamp[Int](version = 2, schema, Seq(0, 10))
  }

  test(s"StateManager v2 - nested type - without timestamp") {
    val schema = StructType(Seq(
      StructField("groupState", StructType(Seq(
        StructField("i", IntegerType, nullable = false),
        StructField("nested", StructType(Seq(
          StructField("d", DoubleType, nullable = false),
          StructField("str", StringType)
        )))
      )))
    ))

    val testValues = Seq(
      NestedStruct(1, Struct(1.0, "someString")),
      NestedStruct(0, Struct(0.0, "")),
      NestedStruct(0, null),
      null)

    testStateManagerWithoutTimestamp[NestedStruct](version = 2, schema, testValues)
  }

  test(s"StateManager v2 - nested type - with timestamp") {
    val schema = StructType(Seq(
      StructField("groupState", StructType(Seq(
        StructField("i", IntegerType, nullable = false),
        StructField("nested", StructType(Seq(
          StructField("d", DoubleType, nullable = false),
          StructField("str", StringType)
        )))
      ))),
      StructField("timeoutTimestamp", LongType, nullable = false)
    ))

    val testValues = Seq(
      NestedStruct(1, Struct(1.0, "someString")),
      NestedStruct(0, Struct(0.0, "")),
      NestedStruct(0, null),
      null)

    testStateManagerWithTimestamp[NestedStruct](version = 2, schema, testValues)
  }


  def testStateManagerWithoutTimestamp[T: Encoder](
      version: Int,
      expectedStateSchema: StructType,
      testValues: Seq[T]): Unit = {
    val stateManager = newStateManager[T](version, withTimestamp = false)
    assert(stateManager.stateSchema === expectedStateSchema)
    testStateManager(stateManager, testValues, NO_TIMESTAMP)
  }

  def testStateManagerWithTimestamp[T: Encoder](
      version: Int,
      expectedStateSchema: StructType,
      testValues: Seq[T]): Unit = {
    val stateManager = newStateManager[T](version, withTimestamp = true)
    assert(stateManager.stateSchema === expectedStateSchema)
    for (timestamp <- Seq(NO_TIMESTAMP, 1000)) {
      testStateManager(stateManager, testValues, timestamp)
    }
  }

  private def testStateManager[T: Encoder](
      stateManager: StateManager,
      values: Seq[T],
      timestamp: Long): Unit = {
    val keys = (1 to values.size).map(_ => newKey())
    val store = new MemoryStateStore()

    // Test stateManager.getState(), putState(), removeState()
    keys.zip(values).foreach { case (key, value) =>
      try {
        stateManager.putState(store, key, value, timestamp)
        val data = stateManager.getState(store, key)
        assert(data.stateObj == value)
        assert(data.timeoutTimestamp === timestamp)
        stateManager.removeState(store, key)
        assert(stateManager.getState(store, key).stateObj == null)
      } catch {
        case e: Throwable =>
         fail(s"put/get/remove test with '$value' failed", e)
      }
    }

    // Test stateManager.getAllState()
    for (i <- keys.indices) {
      stateManager.putState(store, keys(i), values(i), timestamp)
    }
    val allData = stateManager.getAllState(store).map(_.copy()).toArray
    assert(allData.map(_.timeoutTimestamp).toSet == Set(timestamp))
    assert(allData.map(_.stateObj).toSet == values.toSet)
  }

  private def newStateManager[T: Encoder](version: Int, withTimestamp: Boolean): StateManager = {
    FlatMapGroupsWithStateExecHelper.createStateManager(
      implicitly[Encoder[T]].asInstanceOf[ExpressionEncoder[Any]],
      withTimestamp,
      version)
  }

  private val proj = UnsafeProjection.create(Array[DataType](IntegerType))
  private val keyCounter = new AtomicInteger(0)
  private def newKey(): UnsafeRow = {
    proj.apply(new GenericInternalRow(Array[Any](keyCounter.getAndDecrement()))).copy()
  }
}

case class Struct(d: Double, str: String)
case class NestedStruct(i: Int, nested: Struct)
