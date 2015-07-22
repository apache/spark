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

package org.apache.spark.streaming

import java.io.NotSerializableException

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{HashPartitioner, SparkContext, SparkException, SparkFunSuite}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.ReturnStatementInClosureException

/**
 * Test that closures passed to DStream operations are actually cleaned.
 */
class DStreamClosureSuite extends SparkFunSuite with BeforeAndAfterAll {
  private var ssc: StreamingContext = null

  override def beforeAll(): Unit = {
    val sc = new SparkContext("local", "test")
    ssc = new StreamingContext(sc, Seconds(1))
  }

  override def afterAll(): Unit = {
    ssc.stop(stopSparkContext = true)
    ssc = null
  }

  test("user provided closures are actually cleaned") {
    val dstream = new DummyInputDStream(ssc)
    val pairDstream = dstream.map { i => (i, i) }
    // DStream
    testMap(dstream)
    testFlatMap(dstream)
    testFilter(dstream)
    testMapPartitions(dstream)
    testReduce(dstream)
    testForeach(dstream)
    testForeachRDD(dstream)
    testTransform(dstream)
    testTransformWith(dstream)
    testReduceByWindow(dstream)
    // PairDStreamFunctions
    testReduceByKey(pairDstream)
    testCombineByKey(pairDstream)
    testReduceByKeyAndWindow(pairDstream)
    testUpdateStateByKey(pairDstream)
    testMapValues(pairDstream)
    testFlatMapValues(pairDstream)
    // StreamingContext
    testTransform2(ssc, dstream)
  }

  /**
   * Verify that the expected exception is thrown.
   *
   * We use return statements as an indication that a closure is actually being cleaned.
   * We expect closure cleaner to find the return statements in the user provided closures.
   */
  private def expectCorrectException(body: => Unit): Unit = {
    try {
      body
    } catch {
      case rse: ReturnStatementInClosureException => // Success!
      case e @ (_: NotSerializableException | _: SparkException) =>
        throw new TestException(
          s"Expected ReturnStatementInClosureException, but got $e.\n" +
          "This means the closure provided by user is not actually cleaned.")
    }
  }

  // DStream operations
  private def testMap(ds: DStream[Int]): Unit = expectCorrectException {
    ds.map { _ => return; 1 }
  }
  private def testFlatMap(ds: DStream[Int]): Unit = expectCorrectException {
    ds.flatMap { _ => return; Seq.empty }
  }
  private def testFilter(ds: DStream[Int]): Unit = expectCorrectException {
    ds.filter { _ => return; true }
  }
  private def testMapPartitions(ds: DStream[Int]): Unit = expectCorrectException {
    ds.mapPartitions { _ => return; Seq.empty.toIterator }
  }
  private def testReduce(ds: DStream[Int]): Unit = expectCorrectException {
    ds.reduce { case (_, _) => return; 1 }
  }
  private def testForeach(ds: DStream[Int]): Unit = {
    val foreachF1 = (rdd: RDD[Int], t: Time) => return
    val foreachF2 = (rdd: RDD[Int]) => return
    expectCorrectException { ds.foreach(foreachF1) }
    expectCorrectException { ds.foreach(foreachF2) }
  }
  private def testForeachRDD(ds: DStream[Int]): Unit = {
    val foreachRDDF1 = (rdd: RDD[Int], t: Time) => return
    val foreachRDDF2 = (rdd: RDD[Int]) => return
    expectCorrectException { ds.foreachRDD(foreachRDDF1) }
    expectCorrectException { ds.foreachRDD(foreachRDDF2) }
  }
  private def testTransform(ds: DStream[Int]): Unit = {
    val transformF1 = (rdd: RDD[Int]) => { return; rdd }
    val transformF2 = (rdd: RDD[Int], time: Time) => { return; rdd }
    expectCorrectException { ds.transform(transformF1) }
    expectCorrectException { ds.transform(transformF2) }
  }
  private def testTransformWith(ds: DStream[Int]): Unit = {
    val transformF1 = (rdd1: RDD[Int], rdd2: RDD[Int]) => { return; rdd1 }
    val transformF2 = (rdd1: RDD[Int], rdd2: RDD[Int], time: Time) => { return; rdd2 }
    expectCorrectException { ds.transformWith(ds, transformF1) }
    expectCorrectException { ds.transformWith(ds, transformF2) }
  }
  private def testReduceByWindow(ds: DStream[Int]): Unit = {
    val reduceF = (_: Int, _: Int) => { return; 1 }
    expectCorrectException { ds.reduceByWindow(reduceF, Seconds(1), Seconds(2)) }
    expectCorrectException { ds.reduceByWindow(reduceF, reduceF, Seconds(1), Seconds(2)) }
  }

  // PairDStreamFunctions operations
  private def testReduceByKey(ds: DStream[(Int, Int)]): Unit = {
    val reduceF = (_: Int, _: Int) => { return; 1 }
    expectCorrectException { ds.reduceByKey(reduceF) }
    expectCorrectException { ds.reduceByKey(reduceF, 5) }
    expectCorrectException { ds.reduceByKey(reduceF, new HashPartitioner(5)) }
  }
  private def testCombineByKey(ds: DStream[(Int, Int)]): Unit = {
    expectCorrectException {
      ds.combineByKey[Int](
        { _: Int => return; 1 },
        { case (_: Int, _: Int) => return; 1 },
        { case (_: Int, _: Int) => return; 1 },
        new HashPartitioner(5)
      )
    }
  }
  private def testReduceByKeyAndWindow(ds: DStream[(Int, Int)]): Unit = {
    val reduceF = (_: Int, _: Int) => { return; 1 }
    val filterF = (_: (Int, Int)) => { return; false }
    expectCorrectException { ds.reduceByKeyAndWindow(reduceF, Seconds(1)) }
    expectCorrectException { ds.reduceByKeyAndWindow(reduceF, Seconds(1), Seconds(2)) }
    expectCorrectException { ds.reduceByKeyAndWindow(reduceF, Seconds(1), Seconds(2), 5) }
    expectCorrectException {
      ds.reduceByKeyAndWindow(reduceF, Seconds(1), Seconds(2), new HashPartitioner(5))
    }
    expectCorrectException { ds.reduceByKeyAndWindow(reduceF, reduceF, Seconds(2)) }
    expectCorrectException {
      ds.reduceByKeyAndWindow(
        reduceF, reduceF, Seconds(2), Seconds(3), new HashPartitioner(5), filterF)
    }
  }
  private def testUpdateStateByKey(ds: DStream[(Int, Int)]): Unit = {
    val updateF1 = (_: Seq[Int], _: Option[Int]) => { return; Some(1) }
    val updateF2 = (_: Iterator[(Int, Seq[Int], Option[Int])]) => { return; Seq((1, 1)).toIterator }
    val initialRDD = ds.ssc.sparkContext.emptyRDD[Int].map { i => (i, i) }
    expectCorrectException { ds.updateStateByKey(updateF1) }
    expectCorrectException { ds.updateStateByKey(updateF1, 5) }
    expectCorrectException { ds.updateStateByKey(updateF1, new HashPartitioner(5)) }
    expectCorrectException {
      ds.updateStateByKey(updateF1, new HashPartitioner(5), initialRDD)
    }
    expectCorrectException {
      ds.updateStateByKey(updateF2, new HashPartitioner(5), true)
    }
    expectCorrectException {
      ds.updateStateByKey(updateF2, new HashPartitioner(5), true, initialRDD)
    }
  }
  private def testMapValues(ds: DStream[(Int, Int)]): Unit = expectCorrectException {
    ds.mapValues { _ => return; 1 }
  }
  private def testFlatMapValues(ds: DStream[(Int, Int)]): Unit = expectCorrectException {
    ds.flatMapValues { _ => return; Seq.empty }
  }

  // StreamingContext operations
  private def testTransform2(ssc: StreamingContext, ds: DStream[Int]): Unit = {
    val transformF = (rdds: Seq[RDD[_]], time: Time) => { return; ssc.sparkContext.emptyRDD[Int] }
    expectCorrectException { ssc.transform(Seq(ds), transformF) }
  }

}
