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

import java.util.UUID

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, LessThanOrEqual, Literal, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.types._

class SessionWindowLinkedListStateSuite extends StreamTest {

  test("add sessions - normal case") {
    withSessionWindowLinkedListState(inputValueAttribs, keyExprs) { state =>
      implicit val st = state

      assert(get(20) === Seq.empty)
      setHead(20, 3, time = 3)
      assert(get(20) === Seq(3))
      assert(numRows === 1)

      // add element before head: 1 is the new head
      addBefore(20, 1, time = 1, targetTime = 3)
      assert(get(20) === Seq(1, 3))
      assert(numRows === 2)

      // add element before other element but after head
      addBefore(20, 2, time = 2, targetTime = 3)
      assert(get(20) === Seq(1, 2, 3))
      assert(numRows === 3)

      // add element at the end
      addAfter(20, 5, time = 5, targetTime = 3)
      assert(get(20) === Seq(1, 2, 3, 5))
      assert(numRows === 4)

      // add element after other element but before tail element
      addAfter(20, 4, time = 4, targetTime = 3)
      assert(get(20) === Seq(1, 2, 3, 4, 5))
      assert(numRows === 5)

      update(20, 100, time = 3)
      assert(get(20) === Seq(1, 2, 100, 4, 5))
      assert(numRows === 5)

      assert(get(30) === Seq.empty)
      setHead(30, 1, time = 1)
      assert(get(30) === Seq(1))
      assert(get(20) === Seq(1, 2, 100, 4, 5))
      assert(numRows === 6)
    }
  }

  test("add sessions - improper usage") {
    withSessionWindowLinkedListState(inputValueAttribs, keyExprs) { state =>
      implicit val st = state

      assert(get(20) === Seq.empty)

      setHead(20, 2, time = 2)
      // setting head twice
      intercept[IllegalArgumentException] {
        setHead(20, 2, time = 2)
      }

      // add element with dangling pointer
      intercept[IllegalArgumentException] {
        addBefore(20, 1, time = 1, targetTime = 3)
      }

      // add element with dangling pointer
      intercept[IllegalArgumentException] {
        addAfter(20, 2, time = 5, targetTime = 3)
      }
    }
  }

  test("remove sessions - normal usage") {
    withSessionWindowLinkedListState(inputValueAttribs, keyExprs) { state =>
      implicit val st = state

      assert(numRows === 0)

      setHead(20, 1, time = 1)
      addAfter(20, 2, time = 2, targetTime = 1)
      addAfter(20, 3, time = 3, targetTime = 2)
      addAfter(20, 4, time = 4, targetTime = 3)
      assert(numRows === 4)

      // remove head which list has another elements as well
      remove(20, time = 1)
      assert(get(20) === Seq(2, 3, 4))
      assert(numRows === 3)

      // remove intermediate element
      remove(20, time = 3)
      assert(get(20) === Seq(2, 4))
      assert(numRows === 2)

      // remove tail element
      remove(20, time = 4)
      assert(get(20) === Seq(2))
      assert(numRows === 1)

      // remove head which list has only one element
      remove(20, time = 2)
      assert(get(20) === Seq.empty)
      assert(numRows === 0)
    }
  }

  test("remove sessions - improper usage") {
    withSessionWindowLinkedListState(inputValueAttribs, keyExprs) { state =>
      implicit val st = state

      assert(get(20) === Seq.empty)
      setHead(20, 2, time = 2)

      // try to remove non-exist time
      intercept[IllegalArgumentException] {
        remove(20, 3)
      }

      assert(get(20) === Seq(2))
      assert(numRows === 1)
    }
  }

  test("get all pairs, iterate pointers, find first") {
    withSessionWindowLinkedListState(inputValueAttribs, keyExprs) { state =>
      implicit val st = state
      assert(numRows === 0)

      setHead(20, 1, time = 1)
      addAfter(20, 2, time = 2, targetTime = 1)
      addAfter(20, 3, time = 3, targetTime = 2)
      addAfter(20, 4, time = 4, targetTime = 3)

      setHead(30, 5, time = 5)
      addAfter(30, 6, time = 6, targetTime = 5)
      addAfter(30, 7, time = 7, targetTime = 6)
      addAfter(30, 8, time = 8, targetTime = 7)

      setHead(40, 10, time = 10)
      addAfter(40, 11, time = 11, targetTime = 10)
      addAfter(40, 12, time = 12, targetTime = 11)
      addAfter(40, 13, time = 13, targetTime = 12)

      assert(numRows === 12)

      // must keep input order per key
      val groupedTuples = getAllRowPairs.groupBy(_._1)
      assert(groupedTuples(20).map(_._2) === Seq(1, 2, 3, 4))
      assert(groupedTuples(30).map(_._2) === Seq(5, 6, 7, 8))
      assert(groupedTuples(40).map(_._2) === Seq(10, 11, 12, 13))

      // iterate pointers

      val expected = Seq((1, None, Some(2)), (2, Some(1), Some(3)), (3, Some(2), Some(4)),
        (4, Some(3), None))
      expected.foreach { case (current, expectedPrev, expectedNext) =>
        assert(getPrevTime(20, current) == expectedPrev)
        assert(getNextTime(20, current) == expectedNext)
      }

      assert(iterateTimes(20).toSeq === expected.map(s => (s._1, s._2, s._3)))

      // against non-exist key
      assert(iterateTimes(100).toSeq === Seq.empty)

      // find first

      assert(findFirstTime(20, time => time > 0) === Some(1))
      assert(findFirstTime(20, time => time > 3) === Some(4))
      assert(findFirstTime(20, time => time > 5) === None)

      // using start time to skip elements
      assert(findFirstTime(20, time => time > 0, startTime = 3) === Some(3))
      assert(findFirstTime(20, time => time > 3, startTime = 1) === Some(4))
      intercept[IllegalArgumentException] {
        findFirstTime(20, time => time > 3, startTime = 7)
      }

      // against non-exist key
      assert(findFirstTime(100, time => time > 1) === None)
    }
  }

  test("remove by watermark - stop on condition mismatch == true") {
    removeByWatermarkTest(stopOnConditionMismatch = true)
  }

  test("remove by watermark - stop on condition mismatch == false") {
    removeByWatermarkTest(stopOnConditionMismatch = false)
  }

  test("run chaos monkey") {

    // FIXME: too many args
    def printFailureInformation(
        ex: TestFailedException,
        state: SessionWindowLinkedListState,
        operation: Int,
        addBefore: Boolean,
        opIdx: Int,
        targetIdx: Int,
        key: UnsafeRow,
        headPointersBeforeOp: List[(Int, Long)],
        rawPointersBeforeOp: List[(Int, Long, Option[Long], Option[Long])],
        pointersBeforeOp: List[Long],
        valuesBeforeOp: List[(Int, Int)],
        refListBeforeOp: java.util.LinkedList[String],
        refList: java.util.LinkedList[String]): Unit = {
      logError("Assertion failure!", ex)

      logError("===== Operation information =====")
      val opString = operation match {
        case 0 => "Append"
        case 1 => "Remove"
        case 2 => "RemoveValuesByCondition"
        case _ => throw new IllegalStateException(s"Unknown operation $operation")
      }
      val addPositionString = if (addBefore) "AddBefore" else "AddAfter"
      logError(s"Operation Index: $opIdx")
      logError(s"Operation: $opString")
      logError(s"Position to add: $addPositionString")
      logError(s"Target index: $targetIdx")

      logError("===== Before applying operation =====")
      logError(s"Head pointers in state: $headPointersBeforeOp")
      logError(s"Raw pointers in state: $rawPointersBeforeOp")
      logError(s"Pointers in state via iteratePointers: $pointersBeforeOp")
      logError(s"Values in state: $valuesBeforeOp")
      logError(s"Values in reference list: $refListBeforeOp")

      logError("===== After applying operation =====")

      val headPointers = state.getIteratorOfHeadPointers.map { pair =>
        (toKeyInt(pair.key), pair.sessionStart)
      }.toList
      val pointers = state.iteratePointers(key).map(_._1).toList
      val rawPointers = state.getIteratorOfRawPointers.map { pointer =>
        (toKeyInt(pointer.key), pointer.sessionStart, pointer.prevSessionStart,
          pointer.nextSessionStart)
      }.toList
      val values = state.getIteratorOfRawValues.map { value =>
        (toKeyInt(value.key), toValueInt(value.value))
      }.toList

      logError(s"Head pointers in state: $headPointers")
      logError(s"Raw pointers in state: $rawPointers")
      logError(s"Pointers in state via iteratePointers: $pointers")
      logError(s"Values in state: $values")
      logError(s"Values in reference list: $refList")
    }

    withSessionWindowLinkedListState(inputValueAttribs, keyExprs) { state =>
      implicit val st = state

      assert(numRows === 0)

      val rand = new Random()

      val keys = (0 to 2).map(id => toKeyRow(id).copy())
      // using String type to avoid confusion in remove(int) vs remove(Object)
      // which LinkedList[Integer] will be remove(int) vs remove(Integer)
      val refLists = keys.map(_ => new java.util.LinkedList[String]())

      val maxOperations = 100000
      (0 until maxOperations).foreach { opIdx =>

        val selectedKeyIdx = rand.nextInt(keys.length)
        val selectedKey = keys(selectedKeyIdx)
        val selectedRefList = refLists(selectedKeyIdx)

        // 0: append, 1: remove, 2: removeValueByCondition
        val operation = rand.nextInt(3)
        val addBefore = rand.nextBoolean()
        val targetIdx = if (selectedRefList.isEmpty) -1 else rand.nextInt(selectedRefList.size())

        val headPointersBeforeOp = state.getIteratorOfHeadPointers.map { pair =>
          (toKeyInt(pair.key), pair.sessionStart)
        }.toList
        val pointersBeforeOp = state.iteratePointers(selectedKey).map(_._1).toList
        val rawPointersBeforeOp = state.getIteratorOfRawPointers.map { pointer =>
          (toKeyInt(pointer.key), pointer.sessionStart, pointer.prevSessionStart,
            pointer.nextSessionStart)
        }.toList
        val valuesBeforeOp = state.getIteratorOfRawValues.map { value =>
          (toKeyInt(value.key), toValueInt(value.value))
        }.toList

        val refListBeforeOp = new java.util.LinkedList[String](refLists(selectedKeyIdx))

        operation match {
          case 0 =>
            if (selectedRefList.isEmpty) {
              assert(state.isEmpty(selectedKey))
              state.setHead(selectedKey, opIdx, toInputValue(opIdx))
              selectedRefList.addFirst(String.valueOf(opIdx))
            } else {
              val addBefore = rand.nextBoolean()
              if (addBefore) {
                val idxToAddBefore = selectedRefList.get(targetIdx)
                selectedRefList.add(targetIdx, String.valueOf(opIdx))
                state.addBefore(selectedKey, opIdx, toInputValue(opIdx), idxToAddBefore.toInt)
              } else {
                val idxToAddAfter = selectedRefList.get(targetIdx)
                selectedRefList.add(targetIdx + 1, String.valueOf(opIdx))
                state.addAfter(selectedKey, opIdx, toInputValue(opIdx), idxToAddAfter.toInt)
              }
            }

          case 1 =>
            if (selectedRefList.isEmpty) {
              assert(state.isEmpty(selectedKey))
              // skip removing
            } else {
              val pointerToRemove = selectedRefList.get(targetIdx)
              selectedRefList.remove(targetIdx)
              state.remove(selectedKey, pointerToRemove.toInt)
            }

          case 2 =>
            if (selectedRefList.isEmpty) {
              assert(state.isEmpty(selectedKey))
              // skip removing
            } else {
              val pointerToRemove = selectedRefList.get(targetIdx)
              val removedIter = state.removeByValueCondition { r =>
                toValueInt(r) <= pointerToRemove.toInt
              }

              val valuesFromRef = new scala.collection.mutable.MutableList[Int]()
              refLists.foreach { refList =>
                val refIter = refList.iterator()
                while (refIter.hasNext) {
                  val ref = refIter.next()
                  if (ref.toInt <= pointerToRemove.toInt) {
                    valuesFromRef += ref.toInt
                    refIter.remove()
                  }
                }
              }

              try {
                assert(removedIter.map(pair => toValueInt(pair.value)).toSet ==
                  valuesFromRef.toSet)
              } catch {
                case ex: TestFailedException =>
                  printFailureInformation(ex, state, operation, addBefore, opIdx, targetIdx,
                    selectedKey, headPointersBeforeOp, rawPointersBeforeOp, pointersBeforeOp,
                    valuesBeforeOp, refListBeforeOp, selectedRefList)

                  throw ex
              }
            }
        }

        keys.indices.foreach { index =>
          val key = keys(index)
          val refList = refLists(index)

          try {
            if (refList.isEmpty) {
              assert(state.isEmpty(key), s"Reference list is empty but " +
                s"state list for $key is not empty")
            } else {
              import scala.collection.JavaConverters._
              val statePointers = state.iteratePointers(key).map(_._1).toList
              assert(refList.asScala.map(_.toInt) === statePointers,
                s"State pointers for $key is expected to be $refList but $statePointers")

              val stateValues = state.get(key).map(toValueInt).toList
              assert(refList.asScala.map(_.toInt) ===
                stateValues, s"State list for $key is expected to be $refList but $stateValues")
            }
          } catch {
            case ex: TestFailedException =>
              printFailureInformation(ex, state, operation, addBefore, opIdx, targetIdx,
                selectedKey, headPointersBeforeOp, rawPointersBeforeOp, pointersBeforeOp,
                valuesBeforeOp, refListBeforeOp, selectedRefList)

              throw ex
          }
        }
      }
    }
  }

  private def removeByWatermarkTest(stopOnConditionMismatch: Boolean): Unit = {
    withSessionWindowLinkedListState(inputValueAttribs, keyExprs) { state =>
      implicit val st = state
      assert(numRows === 0)

      setHead(20, 1, time = 1)
      addAfter(20, 2, time = 2, targetTime = 1)
      addAfter(20, 3, time = 3, targetTime = 2)
      addAfter(20, 4, time = 4, targetTime = 3)

      setHead(30, 5, time = 5)
      addAfter(30, 6, time = 6, targetTime = 5)
      addAfter(30, 7, time = 7, targetTime = 6)
      addAfter(30, 8, time = 8, targetTime = 7)

      setHead(40, 10, time = 10)
      addAfter(40, 11, time = 11, targetTime = 10)
      addAfter(40, 12, time = 12, targetTime = 11)
      addAfter(40, 13, time = 13, targetTime = 12)

      assert(numRows === 12)

      // must keep input order per key
      val groupedTuples = removeByValue(6, stopOnConditionMismatch).groupBy(_._1)
      assert(groupedTuples(20).map(_._2) === Seq(1, 2, 3, 4))
      assert(groupedTuples(30).map(_._2) === Seq(5, 6))
      assert(groupedTuples.get(40).isEmpty)

      assert(get(20) === Seq.empty)
      assert(get(30) === Seq(7, 8))
      assert(get(40) === Seq(10, 11, 12, 13))
      assert(numRows === 6)
    }
  }

  val watermarkMetadata = new MetadataBuilder().putLong(EventTimeWatermark.delayKey, 10).build()
  val inputValueSchema = new StructType()
    .add(StructField("time", IntegerType, metadata = watermarkMetadata))
    .add(StructField("value", BooleanType))
  val inputValueAttribs = inputValueSchema.toAttributes
  val inputValueAttribWithWatermark = inputValueAttribs(0)
  val keyExprs = Seq[Expression](Literal(false), inputValueAttribWithWatermark, Literal(10.0))

  val inputValueGen = UnsafeProjection.create(inputValueAttribs.map(_.dataType).toArray)
  val keyGen = UnsafeProjection.create(keyExprs.map(_.dataType).toArray)

  def toInputValue(i: Int): UnsafeRow = {
    inputValueGen.apply(new GenericInternalRow(Array[Any](i, false)))
  }

  def toKeyRow(i: Int): UnsafeRow = {
    keyGen.apply(new GenericInternalRow(Array[Any](false, i, 10.0)))
  }

  def toKeyInt(inputKeyRow: UnsafeRow): Int = inputKeyRow.getInt(1)

  def toValueInt(inputValueRow: UnsafeRow): Int = inputValueRow.getInt(0)

  def setHead(key: Int, value: Int, time: Int)
             (implicit state: SessionWindowLinkedListState): Unit = {
    state.setHead(toKeyRow(key), time, toInputValue(value))
  }

  def addBefore(key: Int, value: Int, time: Int, targetTime: Int)
               (implicit state: SessionWindowLinkedListState): Unit = {
    state.addBefore(toKeyRow(key), time, toInputValue(value), targetTime)
  }

  def addAfter(key: Int, value: Int, time: Int, targetTime: Int)
              (implicit state: SessionWindowLinkedListState): Unit = {
    state.addAfter(toKeyRow(key), time, toInputValue(value), targetTime)
  }

  def update(key: Int, value: Int, time: Int)
            (implicit state: SessionWindowLinkedListState): Unit = {
    state.update(toKeyRow(key), time, toInputValue(value))
  }

  def remove(key: Int, time: Int)(implicit state: SessionWindowLinkedListState): Unit = {
    state.remove(toKeyRow(key), time)
  }

  def get(key: Int)(implicit state: SessionWindowLinkedListState): Seq[Int] = {
    state.get(toKeyRow(key)).map(toValueInt).toSeq
  }

  def iterateTimes(key: Int)(implicit state: SessionWindowLinkedListState)
    : Iterator[(Int, Option[Int], Option[Int])] = {
    state.iteratePointers(toKeyRow(key)).map { s =>
      (s._1.toInt, s._2.map(_.toInt), s._3.map(_.toInt))
    }
  }

  def getPrevTime(key: Int, time: Int)(implicit state: SessionWindowLinkedListState)
    : Option[Int] = {
    state.getPrevSessionStart(toKeyRow(key), time).map(_.toInt)
  }

  def getNextTime(key: Int, time: Int)(implicit state: SessionWindowLinkedListState)
    : Option[Int] = {
    state.getNextSessionStart(toKeyRow(key), time).map(_.toInt)
  }

  def findFirstTime(key: Int, predicate: Int => Boolean)
                   (implicit state: SessionWindowLinkedListState): Option[Int] = {
    val ret = state.findFirstSessionStartEnsurePredicate(
      toKeyRow(key), (s: Long) => predicate.apply(s.intValue()))
    ret.map(_.intValue())
  }

  def findFirstTime(key: Int, predicate: Int => Boolean, startTime: Int)
                   (implicit state: SessionWindowLinkedListState): Option[Int] = {
    val ret = state.findFirstSessionStartEnsurePredicate(
      toKeyRow(key), (s: Long) => predicate.apply(s.intValue()), startTime)
    ret.map(_.intValue())
  }

  def getAllRowPairs(implicit state: SessionWindowLinkedListState): Seq[(Int, Int)] = {
    state.getAllRowPairs
      .map(pair => (toKeyInt(pair.key), toValueInt(pair.value)))
      .toSeq
  }

  /** Remove values where `time <= threshold` */
  def removeByValue(watermark: Long, stopOnConditionMismatch: Boolean)
                   (implicit state: SessionWindowLinkedListState)
    : Seq[(Int, Int)] = {
    val expr = LessThanOrEqual(inputValueAttribWithWatermark, Literal(watermark))
    state.removeByValueCondition(
      GeneratePredicate.generate(expr, inputValueAttribs).eval _,
      stopOnConditionMismatch)
      .map(pair => (toKeyInt(pair.key), toValueInt(pair.value)))
      .toSeq
  }

  def numRows(implicit state: SessionWindowLinkedListState): Long = {
    state.metrics.numKeys
  }

  def withSessionWindowLinkedListState(
      inputValueAttribs: Seq[Attribute],
      keyExprs: Seq[Expression])(f: SessionWindowLinkedListState => Unit): Unit = {

    withTempDir { file =>
      val storeConf = new StateStoreConf()
      val stateInfo = StatefulOperatorStateInfo(file.getAbsolutePath, UUID.randomUUID, 0, 0, 5)
      val state = new SessionWindowLinkedListState("testing", inputValueAttribs, keyExprs,
        Some(stateInfo), storeConf, new Configuration)
      try {
        f(state)
      } finally {
        state.abortIfNeeded()
      }
    }
    StateStore.stop()
  }
}
