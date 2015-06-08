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

package org.apache.spark.streaming.scheduler

import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver._
import org.apache.spark.util.Utils

/** Testsuite for receiver scheduling */
class SchedulerSuite extends TestSuiteBase {
  val sparkConf = new SparkConf().setMaster("local[8]").setAppName("test")
  val ssc = new StreamingContext(sparkConf, Milliseconds(100))
  val tracker = new ReceiverTracker(ssc)
  val launcher = new tracker.ReceiverLauncher()

  test("receiver scheduling - no preferredLocation") {
    val numReceivers = 10;
    val receivers = (1 to numReceivers).map(i => new DummyReceiver)
    val executors: List[String] = List("Host1", "Host2", "Host3", "Host4", "Host5")
    val locations = launcher.scheduleReceivers(receivers, executors)
    assert(locations(0)(0) === "Host1") 
    assert(locations(4)(0) === "Host5")  
    assert(locations(5)(0) === "Host1") 
    assert(locations(9)(0) === "Host5")  
  } 

  test("receiver scheduling - no preferredLocation, numExecutors > numReceivers") {
    val numReceivers = 3;
    val receivers = (1 to numReceivers).map(i => new DummyReceiver)
    val executors: List[String] = List("Host1", "Host2", "Host3", "Host4", "Host5")
    val locations = launcher.scheduleReceivers(receivers, executors)
    assert(locations(0)(0) === "Host1") 
    assert(locations(2)(0) === "Host3")  
    assert(locations(0)(1) === "Host4") 
    assert(locations(1)(1) === "Host5")  
  } 

  test("receiver scheduling - all have preferredLocation") {
    val numReceivers = 5;
    val receivers = (1 to numReceivers).map(i => new DummyReceiver(host = Some("Host" + i)))
    val executors: List[String] = List("Host1", "Host5", "Host4", "Host3", "Host2")
    val locations = launcher.scheduleReceivers(receivers, executors)
    assert(locations(1)(0) === "Host2") 
    assert(locations(4)(0) === "Host5")  
  } 

  test("receiver scheduling - some have preferredLocation") {
    val numReceivers = 3;
    val receivers: Seq[Receiver[_]] = Seq(
      new DummyReceiver(host = Some("Host2")),
      new DummyReceiver,
      new DummyReceiver)
    val executors: List[String] = List("Host1", "Host2", "Host3", "Host4", "Host5")
    val locations = launcher.scheduleReceivers(receivers, executors)
    assert(locations(0)(0) === "Host2") 
    assert(locations(1)(0) === "Host1") 
    assert(locations(2)(0) === "Host2")
    assert(locations(1)(1) === "Host3")
  } 
}

/**
 * Dummy receiver implementation
 */
class DummyReceiver(host: Option[String] = None) extends Receiver[Int](StorageLevel.MEMORY_ONLY) {

  def onStart() {
  }

  def onStop() {
  }

  override def preferredLocation: Option[String] = host
}
