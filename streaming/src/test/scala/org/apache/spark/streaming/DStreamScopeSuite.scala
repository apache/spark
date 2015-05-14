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

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD, RDDOperationScope}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.ui.UIUtils

/**
 * Tests whether scope information is passed from DStream operations to RDDs correctly.
 */
class DStreamScopeSuite extends FunSuite with BeforeAndAfter with BeforeAndAfterAll {
  private var ssc: StreamingContext = null
  private val batchDuration: Duration = Seconds(1)

  override def beforeAll(): Unit = {
    ssc = new StreamingContext(new SparkContext("local", "test"), batchDuration)
  }

  override def afterAll(): Unit = {
    ssc.stop(stopSparkContext = true)
  }

  before { assertPropertiesNotSet() }
  after { assertPropertiesNotSet() }

  test("dstream without scope") {
    val inputStream = new DummyInputDStream(ssc)
    inputStream.initialize(Time(0))

    // This DStream is not instantiated in any scope, so all RDDs
    // created by this stream should similarly not have a scope
    assert(inputStream.baseScope === None)
    assert(inputStream.getOrCompute(Time(1000)).get.scope === None)
    assert(inputStream.getOrCompute(Time(2000)).get.scope === None)
    assert(inputStream.getOrCompute(Time(3000)).get.scope === None)
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

  test("scoping with custom names") {
    var baseScope: RDDOperationScope = null
    var rddScope: RDDOperationScope = null

    /** Make a stream in our own scoped DStream operation. */
    def makeStream(customName: Option[String]): Unit = ssc.withScope {
      val stream = new DummyInputDStream(ssc, customName)
      stream.initialize(Time(0))
      val _baseScope = stream.baseScope.map(RDDOperationScope.fromJson)
      val _rddScope = stream.getOrCompute(Time(1000)).get.scope
      assertDefined(_baseScope, _rddScope)
      baseScope = _baseScope.get
      rddScope = _rddScope.get
    }

    // By default, a DStream gets its scope name from the operation that created it
    makeStream(customName = None)
    assert(baseScope.name.startsWith("makeStream"))
    assertScopeCorrect(baseScope, rddScope, 1000)
    // If the DStream defines a custom scope name, however, use that instead of deriving it
    // from the method. Custom scope names are used extensively by real InputDStreams, which
    // are frequently created from methods with generic names (e.g. createStream)
    makeStream(customName = Some("dummy stream"))
    assert(baseScope.name.startsWith("makeStream")) // not used by RDDs
    assertScopeCorrect(baseScope.id, "dummy stream", rddScope, 1000)
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
    assertScopeCorrect(baseScope.id, baseScope.name, rddScope, batchTime)
  }

  /** Assert that the given RDD scope inherits the base name and ID correctly. */
  private def assertScopeCorrect(
      baseScopeId: String,
      baseScopeName: String,
      rddScope: RDDOperationScope,
      batchTime: Long): Unit = {
    assert(rddScope.id === s"${baseScopeId}_$batchTime")
    assert(rddScope.name.replaceAll("\\n", " ") ===
      s"$baseScopeName @ ${UIUtils.formatBatchTime(batchTime)}")
  }

  /** Assert that all the specified options are defined. */
  private def assertDefined[T](options: Option[T]*): Unit = {
    options.zipWithIndex.foreach { case (o, i) => assert(o.isDefined, s"Option $i was empty!") }
  }

}

/**
 * A dummy input stream that does absolutely nothing.
 */
private class DummyInputDStream(
    ssc: StreamingContext,
    customName: Option[String] = None)
  extends InputDStream[Int](ssc) {

  protected override val customScopeName: Option[String] = customName
  override def start(): Unit = { }
  override def stop(): Unit = { }
  override def compute(time: Time): Option[RDD[Int]] = Some(ssc.sc.emptyRDD[Int])
}
