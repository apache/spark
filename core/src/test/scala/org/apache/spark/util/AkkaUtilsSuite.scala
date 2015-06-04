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

package org.apache.spark.util

import java.util.concurrent.TimeoutException

import akka.actor.ActorNotFound

import org.apache.spark._
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.SSLSampleConfigs._


/**
  * Test the AkkaUtils with various security settings.
  */
class AkkaUtilsSuite extends SparkFunSuite with LocalSparkContext with ResetSystemProperties {

  test("remote fetch security bad password") {
    val conf = new SparkConf
    conf.set("spark.rpc", "akka")
    conf.set("spark.authenticate", "true")
    conf.set("spark.authenticate.secret", "good")

    val securityManager = new SecurityManager(conf)
    val hostname = "localhost"
    val rpcEnv = RpcEnv.create("spark", hostname, 0, conf, securityManager)
    System.setProperty("spark.hostPort", rpcEnv.address.hostPort)
    assert(securityManager.isAuthenticationEnabled() === true)

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, masterTracker, conf))

    val badconf = new SparkConf
    badconf.set("spark.rpc", "akka")
    badconf.set("spark.authenticate", "true")
    badconf.set("spark.authenticate.secret", "bad")
    val securityManagerBad = new SecurityManager(badconf)

    assert(securityManagerBad.isAuthenticationEnabled() === true)

    val slaveRpcEnv = RpcEnv.create("spark-slave", hostname, 0, conf, securityManagerBad)
    val slaveTracker = new MapOutputTrackerWorker(conf)
    intercept[akka.actor.ActorNotFound] {
      slaveTracker.trackerEndpoint =
        slaveRpcEnv.setupEndpointRef("spark", rpcEnv.address, MapOutputTracker.ENDPOINT_NAME)
    }

    rpcEnv.shutdown()
    slaveRpcEnv.shutdown()
  }

  test("remote fetch security off") {
    val conf = new SparkConf
    conf.set("spark.authenticate", "false")
    conf.set("spark.authenticate.secret", "bad")
    val securityManager = new SecurityManager(conf)

    val hostname = "localhost"
    val rpcEnv = RpcEnv.create("spark", hostname, 0, conf, securityManager)
    System.setProperty("spark.hostPort", rpcEnv.address.hostPort)

    assert(securityManager.isAuthenticationEnabled() === false)

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, masterTracker, conf))

    val badconf = new SparkConf
    badconf.set("spark.authenticate", "false")
    badconf.set("spark.authenticate.secret", "good")
    val securityManagerBad = new SecurityManager(badconf)

    val slaveRpcEnv = RpcEnv.create("spark-slave", hostname, 0, badconf, securityManagerBad)
    val slaveTracker = new MapOutputTrackerWorker(conf)
    slaveTracker.trackerEndpoint =
      slaveRpcEnv.setupEndpointRef("spark", rpcEnv.address, MapOutputTracker.ENDPOINT_NAME)

    assert(securityManagerBad.isAuthenticationEnabled() === false)

    masterTracker.registerShuffle(10, 1)
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    val size1000 = MapStatus.decompressSize(MapStatus.compressSize(1000L))
    masterTracker.registerMapOutput(10, 0,
      MapStatus(BlockManagerId("a", "hostA", 1000), Array(1000L)))
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    // this should succeed since security off
    assert(slaveTracker.getServerStatuses(10, 0).toSeq ===
           Seq((BlockManagerId("a", "hostA", 1000), size1000)))

    rpcEnv.shutdown()
    slaveRpcEnv.shutdown()
  }

  test("remote fetch security pass") {
    val conf = new SparkConf
    conf.set("spark.authenticate", "true")
    conf.set("spark.authenticate.secret", "good")
    val securityManager = new SecurityManager(conf)

    val hostname = "localhost"
    val rpcEnv = RpcEnv.create("spark", hostname, 0, conf, securityManager)
    System.setProperty("spark.hostPort", rpcEnv.address.hostPort)

    assert(securityManager.isAuthenticationEnabled() === true)

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, masterTracker, conf))

    val goodconf = new SparkConf
    goodconf.set("spark.authenticate", "true")
    goodconf.set("spark.authenticate.secret", "good")
    val securityManagerGood = new SecurityManager(goodconf)

    assert(securityManagerGood.isAuthenticationEnabled() === true)

    val slaveRpcEnv = RpcEnv.create("spark-slave", hostname, 0, goodconf, securityManagerGood)
    val slaveTracker = new MapOutputTrackerWorker(conf)
    slaveTracker.trackerEndpoint =
      slaveRpcEnv.setupEndpointRef("spark", rpcEnv.address, MapOutputTracker.ENDPOINT_NAME)

    masterTracker.registerShuffle(10, 1)
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    val size1000 = MapStatus.decompressSize(MapStatus.compressSize(1000L))
    masterTracker.registerMapOutput(10, 0, MapStatus(
      BlockManagerId("a", "hostA", 1000), Array(1000L)))
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    // this should succeed since security on and passwords match
    assert(slaveTracker.getServerStatuses(10, 0).toSeq ===
           Seq((BlockManagerId("a", "hostA", 1000), size1000)))

    rpcEnv.shutdown()
    slaveRpcEnv.shutdown()
  }

  test("remote fetch security off client") {
    val conf = new SparkConf
    conf.set("spark.rpc", "akka")
    conf.set("spark.authenticate", "true")
    conf.set("spark.authenticate.secret", "good")

    val securityManager = new SecurityManager(conf)

    val hostname = "localhost"
    val rpcEnv = RpcEnv.create("spark", hostname, 0, conf, securityManager)
    System.setProperty("spark.hostPort", rpcEnv.address.hostPort)

    assert(securityManager.isAuthenticationEnabled() === true)

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, masterTracker, conf))

    val badconf = new SparkConf
    badconf.set("spark.rpc", "akka")
    badconf.set("spark.authenticate", "false")
    badconf.set("spark.authenticate.secret", "bad")
    val securityManagerBad = new SecurityManager(badconf)

    assert(securityManagerBad.isAuthenticationEnabled() === false)

    val slaveRpcEnv = RpcEnv.create("spark-slave", hostname, 0, badconf, securityManagerBad)
    val slaveTracker = new MapOutputTrackerWorker(conf)
    intercept[akka.actor.ActorNotFound] {
      slaveTracker.trackerEndpoint =
        slaveRpcEnv.setupEndpointRef("spark", rpcEnv.address, MapOutputTracker.ENDPOINT_NAME)
    }

    rpcEnv.shutdown()
    slaveRpcEnv.shutdown()
  }

  test("remote fetch ssl on") {
    val conf = sparkSSLConfig()
    val securityManager = new SecurityManager(conf)

    val hostname = "localhost"
    val rpcEnv = RpcEnv.create("spark", hostname, 0, conf, securityManager)
    System.setProperty("spark.hostPort", rpcEnv.address.hostPort)

    assert(securityManager.isAuthenticationEnabled() === false)

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, masterTracker, conf))

    val slaveConf = sparkSSLConfig()
    val securityManagerBad = new SecurityManager(slaveConf)

    val slaveRpcEnv = RpcEnv.create("spark-slaves", hostname, 0, slaveConf, securityManagerBad)
    val slaveTracker = new MapOutputTrackerWorker(conf)
    slaveTracker.trackerEndpoint =
      slaveRpcEnv.setupEndpointRef("spark", rpcEnv.address, MapOutputTracker.ENDPOINT_NAME)

    assert(securityManagerBad.isAuthenticationEnabled() === false)

    masterTracker.registerShuffle(10, 1)
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    val size1000 = MapStatus.decompressSize(MapStatus.compressSize(1000L))
    masterTracker.registerMapOutput(10, 0,
      MapStatus(BlockManagerId("a", "hostA", 1000), Array(1000L)))
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    // this should succeed since security off
    assert(slaveTracker.getServerStatuses(10, 0).toSeq ===
      Seq((BlockManagerId("a", "hostA", 1000), size1000)))

    rpcEnv.shutdown()
    slaveRpcEnv.shutdown()
  }


  test("remote fetch ssl on and security enabled") {
    val conf = sparkSSLConfig()
    conf.set("spark.authenticate", "true")
    conf.set("spark.authenticate.secret", "good")
    val securityManager = new SecurityManager(conf)

    val hostname = "localhost"
    val rpcEnv = RpcEnv.create("spark", hostname, 0, conf, securityManager)
    System.setProperty("spark.hostPort", rpcEnv.address.hostPort)

    assert(securityManager.isAuthenticationEnabled() === true)

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, masterTracker, conf))

    val slaveConf = sparkSSLConfig()
    slaveConf.set("spark.authenticate", "true")
    slaveConf.set("spark.authenticate.secret", "good")
    val securityManagerBad = new SecurityManager(slaveConf)

    val slaveRpcEnv = RpcEnv.create("spark-slave", hostname, 0, slaveConf, securityManagerBad)
    val slaveTracker = new MapOutputTrackerWorker(conf)
    slaveTracker.trackerEndpoint =
      slaveRpcEnv.setupEndpointRef("spark", rpcEnv.address, MapOutputTracker.ENDPOINT_NAME)

    assert(securityManagerBad.isAuthenticationEnabled() === true)

    masterTracker.registerShuffle(10, 1)
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    val size1000 = MapStatus.decompressSize(MapStatus.compressSize(1000L))
    masterTracker.registerMapOutput(10, 0,
      MapStatus(BlockManagerId("a", "hostA", 1000), Array(1000L)))
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    assert(slaveTracker.getServerStatuses(10, 0).toSeq ===
      Seq((BlockManagerId("a", "hostA", 1000), size1000)))

    rpcEnv.shutdown()
    slaveRpcEnv.shutdown()
  }


  test("remote fetch ssl on and security enabled - bad credentials") {
    val conf = sparkSSLConfig()
    conf.set("spark.rpc", "akka")
    conf.set("spark.authenticate", "true")
    conf.set("spark.authenticate.secret", "good")
    val securityManager = new SecurityManager(conf)

    val hostname = "localhost"
    val rpcEnv = RpcEnv.create("spark", hostname, 0, conf, securityManager)
    System.setProperty("spark.hostPort", rpcEnv.address.hostPort)

    assert(securityManager.isAuthenticationEnabled() === true)

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, masterTracker, conf))

    val slaveConf = sparkSSLConfig()
    slaveConf.set("spark.rpc", "akka")
    slaveConf.set("spark.authenticate", "true")
    slaveConf.set("spark.authenticate.secret", "bad")
    val securityManagerBad = new SecurityManager(slaveConf)

    val slaveRpcEnv = RpcEnv.create("spark-slave", hostname, 0, slaveConf, securityManagerBad)
    val slaveTracker = new MapOutputTrackerWorker(conf)
    intercept[akka.actor.ActorNotFound] {
      slaveTracker.trackerEndpoint =
        slaveRpcEnv.setupEndpointRef("spark", rpcEnv.address, MapOutputTracker.ENDPOINT_NAME)
    }

    rpcEnv.shutdown()
    slaveRpcEnv.shutdown()
  }


  test("remote fetch ssl on - untrusted server") {
    val conf = sparkSSLConfigUntrusted()
    val securityManager = new SecurityManager(conf)

    val hostname = "localhost"
    val rpcEnv = RpcEnv.create("spark", hostname, 0, conf, securityManager)
    System.setProperty("spark.hostPort", rpcEnv.address.hostPort)

    assert(securityManager.isAuthenticationEnabled() === false)

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, masterTracker, conf))

    val slaveConf = sparkSSLConfig()
    val securityManagerBad = new SecurityManager(slaveConf)

    val slaveRpcEnv = RpcEnv.create("spark-slave", hostname, 0, slaveConf, securityManagerBad)
    val slaveTracker = new MapOutputTrackerWorker(conf)
    try {
      slaveRpcEnv.setupEndpointRef("spark", rpcEnv.address, MapOutputTracker.ENDPOINT_NAME)
      fail("should receive either ActorNotFound or TimeoutException")
    } catch {
      case e: ActorNotFound =>
      case e: TimeoutException =>
    }

    rpcEnv.shutdown()
    slaveRpcEnv.shutdown()
  }

}
