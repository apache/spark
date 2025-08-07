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

import java.sql.Timestamp
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, Expression, GenericInternalRow, LessThanOrEqual, Literal, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.LeftSide
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class SymmetricHashJoinStateManagerSuite extends StreamTest with BeforeAndAfter {

  before {
    SparkSession.setActiveSession(spark) // set this before force initializing 'joinExec'
    spark.streams.stateStoreCoordinator // initialize the lazy coordinator
  }

  SymmetricHashJoinStateManager.supportedVersions.foreach { version =>
    test(s"StreamingJoinStateManager V${version} - all operations") {
      testAllOperations(version)
    }
  }

  SymmetricHashJoinStateManager.supportedVersions.foreach { version =>
    test(s"StreamingJoinStateManager V${version} - all operations with nulls") {
      testAllOperationsWithNulls(version)
    }
  }

  SymmetricHashJoinStateManager.supportedVersions.foreach { version =>
    test(s"StreamingJoinStateManager V${version} - all operations with nulls in middle") {
      testAllOperationsWithNullsInMiddle(version)
    }
  }

  SymmetricHashJoinStateManager.supportedVersions.foreach { version =>
    test(s"SPARK-35689: StreamingJoinStateManager V${version} - " +
        "printable key of keyWithIndexToValue") {

      val keyExprs = Seq[Expression](
        Literal(false),
        Literal(10.0),
        Literal("string"),
        Literal(Timestamp.valueOf("2021-6-8 10:25:50")))
      val keyGen = UnsafeProjection.create(keyExprs.map(_.dataType).toArray)

      withJoinStateManager(inputValueAttribs, keyExprs, version) { manager =>
        val currentKey = keyGen.apply(new GenericInternalRow(Array[Any](
          false, 10.0, UTF8String.fromString("string"),
          Timestamp.valueOf("2021-6-8 10:25:50").getTime)))

        val projectedRow = manager.getInternalRowOfKeyWithIndex(currentKey)
        assert(s"$projectedRow" == "[false,10.0,string,1623173150000]")
      }
    }
  }

  private def testAllOperations(stateFormatVersion: Int): Unit = {
    withJoinStateManager(inputValueAttribs, joinKeyExprs, stateFormatVersion) { manager =>
      implicit val mgr = manager

      assert(get(20) === Seq.empty)     // initially empty
      append(20, 2)
      assert(get(20) === Seq(2))        // should first value correctly
      assertNumRows(stateFormatVersion, 1)

      append(20, 3)
      assert(get(20) === Seq(2, 3))     // should append new values
      append(20, 3)
      assert(get(20) === Seq(2, 3, 3))  // should append another copy if same value added again
      assertNumRows(stateFormatVersion, 3)

      assert(get(30) === Seq.empty)
      append(30, 1)
      assert(get(30) === Seq(1))
      assert(get(20) === Seq(2, 3, 3))  // add another key-value should not affect existing ones
      assertNumRows(stateFormatVersion, 4)

      removeByKey(25)
      assert(get(20) === Seq.empty)
      assert(get(30) === Seq(1))        // should remove 20, not 30
      assertNumRows(stateFormatVersion, 1)

      removeByKey(30)
      assert(get(30) === Seq.empty)     // should remove 30
      assertNumRows(stateFormatVersion, 0)

      appendAndTest(40, 100, 200, 300)
      appendAndTest(50, 125)
      appendAndTest(60, 275)              // prepare for testing removeByValue
      assertNumRows(stateFormatVersion, 5)

      removeByValue(125)
      assert(get(40) === Seq(200, 300))
      assert(get(50) === Seq.empty)
      assert(get(60) === Seq(275))        // should remove only some values, not all
      assertNumRows(stateFormatVersion, 3)

      append(40, 50)
      assert(get(40) === Seq(50, 200, 300))
      assertNumRows(stateFormatVersion, 4)

      removeByValue(200)
      assert(get(40) === Seq(300))
      assert(get(60) === Seq(275))        // should remove only some values, not all
      assertNumRows(stateFormatVersion, 2)

      removeByValue(300)
      assert(get(40) === Seq.empty)
      assert(get(60) === Seq.empty)       // should remove all values now
      assertNumRows(stateFormatVersion, 0)
    }
  }

  /* Test removeByValue with nulls simulated by updating numValues on the state manager */
  private def testAllOperationsWithNulls(stateFormatVersion: Int): Unit = {
    withJoinStateManager(inputValueAttribs, joinKeyExprs, stateFormatVersion) { manager =>
      implicit val mgr = manager

      appendAndTest(40, 100, 200, 300)
      appendAndTest(50, 125)
      appendAndTest(60, 275)              // prepare for testing removeByValue
      assertNumRows(stateFormatVersion, 5)

      updateNumValues(40, 5)   // update total values to 5 to create 2 nulls
      removeByValue(125)
      assert(get(40) === Seq(200, 300))
      assert(get(50) === Seq.empty)
      assert(get(60) === Seq(275))        // should remove only some values, not all and nulls
      assertNumRows(stateFormatVersion, 3)

      append(40, 50)
      assert(get(40) === Seq(50, 200, 300))
      assertNumRows(stateFormatVersion, 4)
      updateNumValues(40, 4)   // update total values to 4 to create 1 null

      removeByValue(200)
      assert(get(40) === Seq(300))
      assert(get(60) === Seq(275))        // should remove only some values, not all and nulls
      assertNumRows(stateFormatVersion, 2)
      updateNumValues(40, 2)   // update total values to simulate nulls
      updateNumValues(60, 4)

      removeByValue(300)
      assert(get(40) === Seq.empty)
      assert(get(60) === Seq.empty)       // should remove all values now including nulls
      assertNumRows(stateFormatVersion, 0)
    }
  }

  /* Test removeByValue with nulls in middle simulated by updating numValues on the state manager */
  private def testAllOperationsWithNullsInMiddle(stateFormatVersion: Int): Unit = {
    // Test with skipNullsForStreamStreamJoins set to false which would throw a
    // NullPointerException while iterating and also return null values as part of get
    withJoinStateManager(inputValueAttribs, joinKeyExprs, stateFormatVersion) { manager =>
      implicit val mgr = manager

      val ex = intercept[Exception] {
        appendAndTest(40, 50, 200, 300)
        assertNumRows(stateFormatVersion, 3)
        updateNumValues(40, 4) // create a null at the end
        append(40, 400)
        updateNumValues(40, 7) // create nulls in between and end
        removeByValue(50)
      }
      assert(ex.isInstanceOf[NullPointerException])
      assert(getNumValues(40) === 7)        // we should get 7 with no nulls skipped

      removeByValue(300)
      assert(getNumValues(40) === 1)         // only 400 should remain
      assert(get(40) === Seq(400))
      removeByValue(400)
      assert(get(40) === Seq.empty)
      assertNumRows(stateFormatVersion, 0)   // ensure all elements removed
    }

    // Test with skipNullsForStreamStreamJoins set to true which would skip nulls
    // and continue iterating as part of removeByValue as well as get
    val metric = new SQLMetric("sum")
    withJoinStateManager(inputValueAttribs, joinKeyExprs, stateFormatVersion, true,
        Some(metric)) { manager =>
      implicit val mgr = manager

      appendAndTest(40, 50, 200, 300)
      assertNumRows(stateFormatVersion, 3)
      updateNumValues(40, 4) // create a null at the end
      assert(getNumValues(40) === 3)
      assert(metric.value == 1)

      append(40, 400)
      assert(getNumValues(40) === 4)
      assert(metric.value == 2)

      updateNumValues(40, 7) // create nulls in between and end
      assert(getNumValues(40) === 4)
      assert(metric.value == 5)

      removeByValue(50)

      assert(getNumValues(40) === 3)       // we should now get (400, 200, 300) with nulls skipped

      removeByValue(300)
      assert(getNumValues(40) === 1)         // only 400 should remain
      assert(get(40) === Seq(400))
      removeByValue(400)
      assert(get(40) === Seq.empty)
      assertNumRows(stateFormatVersion, 0)   // ensure all elements removed
    }
  }

  val watermarkMetadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 10).build()
  val inputValueSchema = new StructType()
    .add(StructField("time", IntegerType, metadata = watermarkMetadata))
    .add(StructField("value", BooleanType))
  val inputValueAttribs = toAttributes(inputValueSchema)
  val inputValueAttribWithWatermark = inputValueAttribs(0)
  val joinKeyExprs = Seq[Expression](Literal(false), inputValueAttribWithWatermark, Literal(10.0))

  val inputValueGen = UnsafeProjection.create(inputValueAttribs.map(_.dataType).toArray)
  val joinKeyGen = UnsafeProjection.create(joinKeyExprs.map(_.dataType).toArray)


  def toInputValue(i: Int): UnsafeRow = {
    inputValueGen.apply(new GenericInternalRow(Array[Any](i, false)))
  }

  def toJoinKeyRow(i: Int): UnsafeRow = {
    joinKeyGen.apply(new GenericInternalRow(Array[Any](false, i, 10.0)))
  }

  def toValueInt(inputValueRow: UnsafeRow): Int = inputValueRow.getInt(0)

  def append(key: Int, value: Int)(implicit manager: SymmetricHashJoinStateManager): Unit = {
    // we only put matched = false for simplicity - StreamingJoinSuite will test the functionality
    manager.append(toJoinKeyRow(key), toInputValue(value), matched = false)
  }

  def appendAndTest(key: Int, values: Int*)
                   (implicit manager: SymmetricHashJoinStateManager): Unit = {
    values.foreach { value => append(key, value)}
    require(get(key) === values)
  }

  def updateNumValues(key: Int, numValues: Long)
                     (implicit manager: SymmetricHashJoinStateManager): Unit = {
    manager.updateNumValuesTestOnly(toJoinKeyRow(key), numValues)
  }

  def getNumValues(key: Int)
                  (implicit manager: SymmetricHashJoinStateManager): Int = {
    manager.get(toJoinKeyRow(key)).size
  }

  def get(key: Int)(implicit manager: SymmetricHashJoinStateManager): Seq[Int] = {
    manager.get(toJoinKeyRow(key)).map(toValueInt).toSeq.sorted
  }

  /** Remove keys (and corresponding values) where `time <= threshold` */
  def removeByKey(threshold: Long)(implicit manager: SymmetricHashJoinStateManager): Unit = {
    val expr =
      LessThanOrEqual(
        BoundReference(
          1, inputValueAttribWithWatermark.dataType, inputValueAttribWithWatermark.nullable),
        Literal(threshold))
    val iter = manager.removeByKeyCondition(GeneratePredicate.generate(expr).eval _)
    while (iter.hasNext) iter.next()
  }

  /** Remove values where `time <= threshold` */
  def removeByValue(watermark: Long)(implicit manager: SymmetricHashJoinStateManager): Unit = {
    val expr = LessThanOrEqual(inputValueAttribWithWatermark, Literal(watermark))
    val iter = manager.removeByValueCondition(
      GeneratePredicate.generate(expr, inputValueAttribs).eval _)
    while (iter.hasNext) iter.next()
  }

  def assertNumRows(stateFormatVersion: Int, target: Long)(
    implicit manager: SymmetricHashJoinStateManager): Unit = {
    // This suite originally uses HDFSBackStateStoreProvider, which provides instantaneous metrics
    // for numRows.
    // But for version 3 with virtual column families, RocksDBStateStoreProvider updates metrics
    // asynchronously. This means the number of keys obtained from the metrics are very likely
    // to be outdated right after a put/remove.
    if (stateFormatVersion <= 2) {
      assert(manager.metrics.numKeys == target)
    }
  }

  def withJoinStateManager(
      inputValueAttribs: Seq[Attribute],
      joinKeyExprs: Seq[Expression],
      stateFormatVersion: Int,
      skipNullsForStreamStreamJoins: Boolean = false,
      metric: Option[SQLMetric] = None)
      (f: SymmetricHashJoinStateManager => Unit): Unit = {
    // HDFS store providers do not support virtual column families
    val storeProvider = if (stateFormatVersion == 3) {
      classOf[RocksDBStateStoreProvider].getName
    } else {
      classOf[HDFSBackedStateStoreProvider].getName
    }
    withTempDir { file =>
      withSQLConf(
        SQLConf.STATE_STORE_SKIP_NULLS_FOR_STREAM_STREAM_JOINS.key ->
          skipNullsForStreamStreamJoins.toString,
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> storeProvider
      ) {
        val storeConf = new StateStoreConf(spark.sessionState.conf)
        val stateInfo = StatefulOperatorStateInfo(
          file.getAbsolutePath, UUID.randomUUID, 0, 0, 5, None)
        val manager = SymmetricHashJoinStateManager(
          LeftSide, inputValueAttribs, joinKeyExprs, Some(stateInfo), storeConf, new Configuration,
          partitionId = 0, None, None, stateFormatVersion, metric,
          joinStoreGenerator = new JoinStateManagerStoreGenerator())
        try {
          f(manager)
        } finally {
          manager.abortIfNeeded()
        }
      }
    }
    StateStore.stop()
  }
}
