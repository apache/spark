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

package org.apache.spark

import org.scalatest.FunSuite

import akka.actor._
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.AkkaUtils
import scala.concurrent.Await

/**
  * Test the AkkaUtils with various security settings.
  */
class AkkaUtilsSuite extends FunSuite with LocalSparkContext {
  private val conf = new SparkConf

  test("remote fetch security bad password") {
    System.setProperty("spark.authenticate", "true")
    System.setProperty("SPARK_SECRET", "good")

    val securityManager = new SecurityManager();
    val hostname = "localhost"
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", hostname, 0, 
      conf = conf, securityManager = securityManager)
    System.setProperty("spark.driver.port", boundPort.toString)    // Will be cleared by LocalSparkContext
    System.setProperty("spark.hostPort", hostname + ":" + boundPort)
    assert(securityManager.isAuthenticationEnabled() === true)

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerActor = actorSystem.actorOf(
        Props(new MapOutputTrackerMasterActor(masterTracker)), "MapOutputTracker")

    System.setProperty("spark.authenticate", "true")
    System.setProperty("SPARK_SECRET", "bad")
    val securityManagerBad= new SecurityManager();

    val (slaveSystem, _) = AkkaUtils.createActorSystem("spark-slave", hostname, 0, 
      conf = conf, securityManager = securityManagerBad)
    val slaveTracker = new MapOutputTracker(conf)
    val selection = slaveSystem.actorSelection(
      s"akka.tcp://spark@localhost:$boundPort/user/MapOutputTracker")
    val timeout = AkkaUtils.lookupTimeout(conf)
    slaveTracker.trackerActor = Await.result(selection.resolveOne(timeout), timeout)

    assert(securityManagerBad.isAuthenticationEnabled() === true)

    masterTracker.registerShuffle(10, 1)
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    val compressedSize1000 = MapOutputTracker.compressSize(1000L)
    val size1000 = MapOutputTracker.decompressSize(compressedSize1000)
    masterTracker.registerMapOutput(10, 0, new MapStatus(
      BlockManagerId("a", "hostA", 1000, 0), Array(compressedSize1000)))
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    // this should fail since password wrong
    intercept[SparkException] { slaveTracker.getServerStatuses(10, 0) }

    actorSystem.shutdown()
    slaveSystem.shutdown()
  }

  test("remote fetch security off") {
    System.setProperty("spark.authenticate", "false")
    System.setProperty("SPARK_SECRET", "bad")
    val securityManager = new SecurityManager();

    val hostname = "localhost"
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", hostname, 0, 
      conf = conf, securityManager = securityManager)
    System.setProperty("spark.driver.port", boundPort.toString)    // Will be cleared by LocalSparkContext
    System.setProperty("spark.hostPort", hostname + ":" + boundPort)

    assert(securityManager.isAuthenticationEnabled() === false)

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerActor = actorSystem.actorOf(
        Props(new MapOutputTrackerMasterActor(masterTracker)), "MapOutputTracker")

    System.setProperty("spark.authenticate", "false")
    System.setProperty("SPARK_SECRET", "good")

    val (slaveSystem, _) = AkkaUtils.createActorSystem("spark-slave", hostname, 0, 
      conf = conf, securityManager = securityManager)
    val slaveTracker = new MapOutputTracker(conf)
    val selection = slaveSystem.actorSelection(
      s"akka.tcp://spark@localhost:$boundPort/user/MapOutputTracker")
    val timeout = AkkaUtils.lookupTimeout(conf)
    slaveTracker.trackerActor = Await.result(selection.resolveOne(timeout), timeout)

    assert(securityManager.isAuthenticationEnabled() === false)

    masterTracker.registerShuffle(10, 1)
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    val compressedSize1000 = MapOutputTracker.compressSize(1000L)
    val size1000 = MapOutputTracker.decompressSize(compressedSize1000)
    masterTracker.registerMapOutput(10, 0, new MapStatus(
      BlockManagerId("a", "hostA", 1000, 0), Array(compressedSize1000)))
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    // this should succeed since security off
    assert(slaveTracker.getServerStatuses(10, 0).toSeq ===
           Seq((BlockManagerId("a", "hostA", 1000, 0), size1000)))

    actorSystem.shutdown()
    slaveSystem.shutdown()
  }

  test("remote fetch security pass") {
    System.setProperty("spark.authenticate", "true")
    System.setProperty("SPARK_SECRET", "good")
    val securityManager = new SecurityManager();

    val hostname = "localhost"
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", hostname, 0, 
      conf = conf, securityManager = securityManager)
    System.setProperty("spark.driver.port", boundPort.toString)    // Will be cleared by LocalSparkContext
    System.setProperty("spark.hostPort", hostname + ":" + boundPort)

    assert(securityManager.isAuthenticationEnabled() === true)

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerActor = actorSystem.actorOf(
        Props(new MapOutputTrackerMasterActor(masterTracker)), "MapOutputTracker")

    System.setProperty("spark.authenticate", "true")
    System.setProperty("SPARK_SECRET", "good")

    assert(securityManager.isAuthenticationEnabled() === true)

    val (slaveSystem, _) = AkkaUtils.createActorSystem("spark-slave", hostname, 0,
      conf = conf, securityManager = securityManager)
    val slaveTracker = new MapOutputTracker(conf)
    val selection = slaveSystem.actorSelection(
      s"akka.tcp://spark@localhost:$boundPort/user/MapOutputTracker")
    val timeout = AkkaUtils.lookupTimeout(conf)
    slaveTracker.trackerActor = Await.result(selection.resolveOne(timeout), timeout)

    masterTracker.registerShuffle(10, 1)
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    val compressedSize1000 = MapOutputTracker.compressSize(1000L)
    val size1000 = MapOutputTracker.decompressSize(compressedSize1000)
    masterTracker.registerMapOutput(10, 0, new MapStatus(
      BlockManagerId("a", "hostA", 1000, 0), Array(compressedSize1000)))
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    // this should succeed since security on and passwords match
    assert(slaveTracker.getServerStatuses(10, 0).toSeq ===
           Seq((BlockManagerId("a", "hostA", 1000, 0), size1000)))

    actorSystem.shutdown()
    slaveSystem.shutdown()
  }

  test("remote fetch security off client") {
    System.setProperty("spark.authenticate", "true")
    System.setProperty("SPARK_SECRET", "good")
    val securityManager = new SecurityManager();

    val hostname = "localhost"
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", hostname, 0, 
      conf = conf, securityManager = securityManager)
    System.setProperty("spark.driver.port", boundPort.toString)    // Will be cleared by LocalSparkContext
    System.setProperty("spark.hostPort", hostname + ":" + boundPort)

    assert(securityManager.isAuthenticationEnabled() === true)

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerActor = actorSystem.actorOf(
        Props(new MapOutputTrackerMasterActor(masterTracker)), "MapOutputTracker")

    System.setProperty("spark.authenticate", "false")
    System.setProperty("SPARK_SECRET", "bad")
    val securityManagerBad = new SecurityManager();

    val (slaveSystem, _) = AkkaUtils.createActorSystem("spark-slave", hostname, 0,
      conf = conf, securityManager = securityManagerBad)
    val slaveTracker = new MapOutputTracker(conf)
    val selection = slaveSystem.actorSelection(
      s"akka.tcp://spark@localhost:$boundPort/user/MapOutputTracker")
    val timeout = AkkaUtils.lookupTimeout(conf)
    slaveTracker.trackerActor = Await.result(selection.resolveOne(timeout), timeout)


    assert(securityManagerBad.isAuthenticationEnabled() === false)

    masterTracker.registerShuffle(10, 1)
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    val compressedSize1000 = MapOutputTracker.compressSize(1000L)
    val size1000 = MapOutputTracker.decompressSize(compressedSize1000)
    masterTracker.registerMapOutput(10, 0, new MapStatus(
      BlockManagerId("a", "hostA", 1000, 0), Array(compressedSize1000)))
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    // this should fail since security on in server and off in client
    intercept[SparkException] { slaveTracker.getServerStatuses(10, 0) }

    actorSystem.shutdown()
    slaveSystem.shutdown()
  }

}
