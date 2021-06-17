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
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.LeftSide
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
      assert(numRows === 1)

      append(20, 3)
      assert(get(20) === Seq(2, 3))     // should append new values
      append(20, 3)
      assert(get(20) === Seq(2, 3, 3))  // should append another copy if same value added again
      assert(numRows === 3)

      assert(get(30) === Seq.empty)
      append(30, 1)
      assert(get(30) === Seq(1))
      assert(get(20) === Seq(2, 3, 3))  // add another key-value should not affect existing ones
      assert(numRows === 4)

      removeByKey(25)
      assert(get(20) === Seq.empty)
      assert(get(30) === Seq(1))        // should remove 20, not 30
      assert(numRows === 1)

      removeByKey(30)
      assert(get(30) === Seq.empty)     // should remove 30
      assert(numRows === 0)

      def appendAndTest(key: Int, values: Int*): Unit = {
        values.foreach { value => append(key, value)}
        require(get(key) === values)
      }

      appendAndTest(40, 100, 200, 300)
      appendAndTest(50, 125)
      appendAndTest(60, 275)              // prepare for testing removeByValue
      assert(numRows === 5)

      removeByValue(125)
      assert(get(40) === Seq(200, 300))
      assert(get(50) === Seq.empty)
      assert(get(60) === Seq(275))        // should remove only some values, not all
      assert(numRows === 3)

      append(40, 50)
      assert(get(40) === Seq(50, 200, 300))
      assert(numRows === 4)

      removeByValue(200)
      assert(get(40) === Seq(300))
      assert(get(60) === Seq(275))        // should remove only some values, not all
      assert(numRows === 2)

      removeByValue(300)
      assert(get(40) === Seq.empty)
      assert(get(60) === Seq.empty)       // should remove all values now
      assert(numRows === 0)
    }
  }
  val watermarkMetadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 10).build()
  val inputValueSchema = new StructType()
    .add(StructField("time", IntegerType, metadata = watermarkMetadata))
    .add(StructField("value", BooleanType))
  val inputValueAttribs = inputValueSchema.toAttributes
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

  def numRows(implicit manager: SymmetricHashJoinStateManager): Long = {
    manager.metrics.numKeys
  }


  def withJoinStateManager(
    inputValueAttribs: Seq[Attribute],
    joinKeyExprs: Seq[Expression],
    stateFormatVersion: Int)(f: SymmetricHashJoinStateManager => Unit): Unit = {

    withTempDir { file =>
      val storeConf = new StateStoreConf()
      val stateInfo = StatefulOperatorStateInfo(file.getAbsolutePath, UUID.randomUUID, 0, 0, 5)
      val manager = new SymmetricHashJoinStateManager(
        LeftSide, inputValueAttribs, joinKeyExprs, Some(stateInfo), storeConf, new Configuration,
        partitionId = 0, stateFormatVersion)
      try {
        f(manager)
      } finally {
        manager.abortIfNeeded()
      }
    }
    StateStore.stop()
  }
}
