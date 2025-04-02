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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.rdd.{RDD, RDDOperationScope}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.ui.{UIUtils => SparkUIUtils}
import org.apache.spark.util.ManualClock

/**
 * Tests whether scope information is passed from DStream operations to RDDs correctly.
 */
class DStreamScopeSuite
  extends SparkFunSuite
  with LocalStreamingContext {

  override def beforeEach(): Unit = {
    super.beforeEach()

    val conf = new SparkConf().setMaster("local").setAppName("test")
    conf.set("spark.streaming.clock", classOf[ManualClock].getName())
    val batchDuration: Duration = Seconds(1)
    ssc = new StreamingContext(new SparkContext(conf), batchDuration)

    assertPropertiesNotSet()
  }

  override def afterEach(): Unit = {
    try {
      assertPropertiesNotSet()
    } finally {
      super.afterEach()
    }
  }

  test("dstream without scope") {
    val dummyStream = new DummyDStream(ssc)
    dummyStream.initialize(Time(0))

    // This DStream is not instantiated in any scope, so all RDDs
    // created by this stream should similarly not have a scope
    assert(dummyStream.baseScope === None)
    assert(dummyStream.getOrCompute(Time(1000)).get.scope === None)
    assert(dummyStream.getOrCompute(Time(2000)).get.scope === None)
    assert(dummyStream.getOrCompute(Time(3000)).get.scope === None)
  }

  test("input dstream without scope") {
    val inputStream = new DummyInputDStream(ssc)
    inputStream.initialize(Time(0))

    val baseScope = inputStream.baseScope.map(RDDOperationScope.fromJson)
    val scope1 = inputStream.getOrCompute(Time(1000)).get.scope
    val scope2 = inputStream.getOrCompute(Time(2000)).get.scope
    val scope3 = inputStream.getOrCompute(Time(3000)).get.scope

    // This DStream is not instantiated in any scope, so all RDDs
    assertDefined(baseScope, scope1, scope2, scope3)
    assert(baseScope.get.name.startsWith("dummy stream"))
    assertScopeCorrect(baseScope.get, scope1.get, 1000)
    assertScopeCorrect(baseScope.get, scope2.get, 2000)
    assertScopeCorrect(baseScope.get, scope3.get, 3000)
  }

  test("scoping simple operations") {
    val inputStream = new DummyInputDStream(ssc)
    val mappedStream = inputStream.map { i => i + 1 }
    val filteredStream = mappedStream.filter { i => i % 2 == 0 }
    filteredStream.initialize(Time(0))

    val mappedScopeBase = mappedStream.baseScope.map(RDDOperationScope.fromJson)
    val mappedScope1 = mappedStream.getOrCompute(Time(1000)).get.scope
    val mappedScope2 = mappedStream.getOrCompute(Time(2000)).get.scope
    val mappedScope3 = mappedStream.getOrCompute(Time(3000)).get.scope
    val filteredScopeBase = filteredStream.baseScope.map(RDDOperationScope.fromJson)
    val filteredScope1 = filteredStream.getOrCompute(Time(1000)).get.scope
    val filteredScope2 = filteredStream.getOrCompute(Time(2000)).get.scope
    val filteredScope3 = filteredStream.getOrCompute(Time(3000)).get.scope

    // These streams are defined in their respective scopes "map" and "filter", so all
    // RDDs created by these streams should inherit the IDs and names of their parent
    // DStream's base scopes
    assertDefined(mappedScopeBase, mappedScope1, mappedScope2, mappedScope3)
    assertDefined(filteredScopeBase, filteredScope1, filteredScope2, filteredScope3)
    assert(mappedScopeBase.get.name === "map")
    assert(filteredScopeBase.get.name === "filter")
    assertScopeCorrect(mappedScopeBase.get, mappedScope1.get, 1000)
    assertScopeCorrect(mappedScopeBase.get, mappedScope2.get, 2000)
    assertScopeCorrect(mappedScopeBase.get, mappedScope3.get, 3000)
    assertScopeCorrect(filteredScopeBase.get, filteredScope1.get, 1000)
    assertScopeCorrect(filteredScopeBase.get, filteredScope2.get, 2000)
    assertScopeCorrect(filteredScopeBase.get, filteredScope3.get, 3000)
  }

  test("scoping nested operations") {
    val inputStream = new DummyInputDStream(ssc)
    // countByKeyAndWindow internally uses reduceByKeyAndWindow, but only countByKeyAndWindow
    // should appear in scope
    val countStream = inputStream.countByWindow(Seconds(10), Seconds(1))
    countStream.initialize(Time(0))

    val countScopeBase = countStream.baseScope.map(RDDOperationScope.fromJson)
    val countScope1 = countStream.getOrCompute(Time(1000)).get.scope
    val countScope2 = countStream.getOrCompute(Time(2000)).get.scope
    val countScope3 = countStream.getOrCompute(Time(3000)).get.scope

    // Assert that all children RDDs inherit the DStream operation name correctly
    assertDefined(countScopeBase, countScope1, countScope2, countScope3)
    assert(countScopeBase.get.name === "countByWindow")
    assertScopeCorrect(countScopeBase.get, countScope1.get, 1000)
    assertScopeCorrect(countScopeBase.get, countScope2.get, 2000)
    assertScopeCorrect(countScopeBase.get, countScope3.get, 3000)

    // All streams except the input stream should share the same scopes as `countStream`
    def testStream(stream: DStream[_]): Unit = {
      if (stream != inputStream) {
        val myScopeBase = stream.baseScope.map(RDDOperationScope.fromJson)
        val myScope1 = stream.getOrCompute(Time(1000)).get.scope
        val myScope2 = stream.getOrCompute(Time(2000)).get.scope
        val myScope3 = stream.getOrCompute(Time(3000)).get.scope
        assertDefined(myScopeBase, myScope1, myScope2, myScope3)
        assert(myScopeBase === countScopeBase)
        assert(myScope1 === countScope1)
        assert(myScope2 === countScope2)
        assert(myScope3 === countScope3)
        // Climb upwards to test the parent streams
        stream.dependencies.foreach(testStream)
      }
    }
    testStream(countStream)
  }

  test("transform should allow RDD operations to be captured in scopes") {
    val inputStream = new DummyInputDStream(ssc)
    val transformedStream = inputStream.transform { _.map { _ -> 1}.reduceByKey(_ + _) }
    transformedStream.initialize(Time(0))

    val transformScopeBase = transformedStream.baseScope.map(RDDOperationScope.fromJson)
    val transformScope1 = transformedStream.getOrCompute(Time(1000)).get.scope
    val transformScope2 = transformedStream.getOrCompute(Time(2000)).get.scope
    val transformScope3 = transformedStream.getOrCompute(Time(3000)).get.scope

    // Assert that all children RDDs inherit the DStream operation name correctly
    assertDefined(transformScopeBase, transformScope1, transformScope2, transformScope3)
    assert(transformScopeBase.get.name === "transform")
    assertNestedScopeCorrect(transformScope1.get, 1000)
    assertNestedScopeCorrect(transformScope2.get, 2000)
    assertNestedScopeCorrect(transformScope3.get, 3000)

    def assertNestedScopeCorrect(rddScope: RDDOperationScope, batchTime: Long): Unit = {
      assert(rddScope.name === "reduceByKey")
      assert(rddScope.parent.isDefined)
      assertScopeCorrect(transformScopeBase.get, rddScope.parent.get, batchTime)
    }
  }

  test("foreachRDD should allow RDD operations to be captured in scope") {
    val inputStream = new DummyInputDStream(ssc)
    val generatedRDDs = new ArrayBuffer[RDD[(Int, Int)]]
    inputStream.foreachRDD { rdd =>
      generatedRDDs += rdd.map { _ -> 1}.reduceByKey(_ + _)
    }
    val batchCounter = new BatchCounter(ssc)
    ssc.start()
    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    clock.advance(3000)
    batchCounter.waitUntilBatchesCompleted(3, 10000)
    assert(generatedRDDs.size === 3)

    val foreachBaseScope =
      ssc.graph.getOutputStreams().head.baseScope.map(RDDOperationScope.fromJson)
    assertDefined(foreachBaseScope)
    assert(foreachBaseScope.get.name === "foreachRDD")

    val rddScopes = generatedRDDs.map { _.scope }.toSeq
    assertDefined(rddScopes: _*)
    rddScopes.zipWithIndex.foreach { case (rddScope, idx) =>
      assert(rddScope.get.name === "reduceByKey")
      assert(rddScope.get.parent.isDefined)
      assertScopeCorrect(foreachBaseScope.get, rddScope.get.parent.get, (idx + 1) * 1000)
    }
  }

  /** Assert that the RDD operation scope properties are not set in our SparkContext. */
  private def assertPropertiesNotSet(): Unit = {
    assert(ssc != null)
    assert(ssc.sc.getLocalProperty(SparkContext.RDD_SCOPE_KEY) == null)
    assert(ssc.sc.getLocalProperty(SparkContext.RDD_SCOPE_NO_OVERRIDE_KEY) == null)
  }

  /** Assert that the given RDD scope inherits the name and ID of the base scope correctly. */
  private def assertScopeCorrect(
      baseScope: RDDOperationScope,
      rddScope: RDDOperationScope,
      batchTime: Long): Unit = {
    val (baseScopeId, baseScopeName) = (baseScope.id, baseScope.name)
    val formattedBatchTime = SparkUIUtils.formatBatchTime(
      batchTime, ssc.graph.batchDuration.milliseconds, showYYYYMMSS = false)
    assert(rddScope.id === s"${baseScopeId}_$batchTime")
    assert(rddScope.name.replaceAll("\\n", " ") === s"$baseScopeName @ $formattedBatchTime")
    assert(rddScope.parent.isEmpty)  // There should not be any higher scope
  }

  /** Assert that all the specified options are defined. */
  private def assertDefined[T](options: Option[T]*): Unit = {
    options.zipWithIndex.foreach { case (o, i) => assert(o.isDefined, s"Option $i was empty!") }
  }

}
