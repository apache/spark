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

package org.apache.spark.deploy.master

import java.util.Date

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps

import akka.actor.Address
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually
import other.supplier.{CustomPersistenceEngine, CustomRecoveryModeFactory}

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.deploy._

class MasterSuite extends SparkFunSuite with Matchers with Eventually {

  test("toAkkaUrl") {
    val conf = new SparkConf(loadDefaults = false)
    val akkaUrl = Master.toAkkaUrl("spark://1.2.3.4:1234", "akka.tcp")
    assert("akka.tcp://sparkMaster@1.2.3.4:1234/user/Master" === akkaUrl)
  }

  test("toAkkaUrl with SSL") {
    val conf = new SparkConf(loadDefaults = false)
    val akkaUrl = Master.toAkkaUrl("spark://1.2.3.4:1234", "akka.ssl.tcp")
    assert("akka.ssl.tcp://sparkMaster@1.2.3.4:1234/user/Master" === akkaUrl)
  }

  test("toAkkaUrl: a typo url") {
    val conf = new SparkConf(loadDefaults = false)
    val e = intercept[SparkException] {
      Master.toAkkaUrl("spark://1.2. 3.4:1234", "akka.tcp")
    }
    assert("Invalid master URL: spark://1.2. 3.4:1234" === e.getMessage)
  }

  test("toAkkaAddress") {
    val conf = new SparkConf(loadDefaults = false)
    val address = Master.toAkkaAddress("spark://1.2.3.4:1234", "akka.tcp")
    assert(Address("akka.tcp", "sparkMaster", "1.2.3.4", 1234) === address)
  }

  test("toAkkaAddress with SSL") {
    val conf = new SparkConf(loadDefaults = false)
    val address = Master.toAkkaAddress("spark://1.2.3.4:1234", "akka.ssl.tcp")
    assert(Address("akka.ssl.tcp", "sparkMaster", "1.2.3.4", 1234) === address)
  }

  test("toAkkaAddress: a typo url") {
    val conf = new SparkConf(loadDefaults = false)
    val e = intercept[SparkException] {
      Master.toAkkaAddress("spark://1.2. 3.4:1234", "akka.tcp")
    }
    assert("Invalid master URL: spark://1.2. 3.4:1234" === e.getMessage)
  }

  test("can use a custom recovery mode factory") {
    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.deploy.recoveryMode", "CUSTOM")
    conf.set("spark.deploy.recoveryMode.factory",
      classOf[CustomRecoveryModeFactory].getCanonicalName)

    val instantiationAttempts = CustomRecoveryModeFactory.instantiationAttempts

    val commandToPersist = new Command(
      mainClass = "",
      arguments = Nil,
      environment = Map.empty,
      classPathEntries = Nil,
      libraryPathEntries = Nil,
      javaOpts = Nil
    )

    val appToPersist = new ApplicationInfo(
      startTime = 0,
      id = "test_app",
      desc = new ApplicationDescription(
        name = "",
        maxCores = None,
        memoryPerExecutorMB = 0,
        command = commandToPersist,
        appUiUrl = "",
        eventLogDir = None,
        eventLogCodec = None,
        coresPerExecutor = None),
      submitDate = new Date(),
      driver = null,
      defaultCores = 0
    )

    val driverToPersist = new DriverInfo(
      startTime = 0,
      id = "test_driver",
      desc = new DriverDescription(
        jarUrl = "",
        mem = 0,
        cores = 0,
        supervise = false,
        command = commandToPersist
      ),
      submitDate = new Date()
    )

    val workerToPersist = new WorkerInfo(
      id = "test_worker",
      host = "127.0.0.1",
      port = 10000,
      cores = 0,
      memory = 0,
      actor = null,
      webUiPort = 0,
      publicAddress = ""
    )

    val (actorSystem, port, uiPort, restPort) =
      Master.startSystemAndActor("127.0.0.1", 7077, 8080, conf)

    try {
      Await.result(actorSystem.actorSelection("/user/Master").resolveOne(10 seconds), 10 seconds)

      CustomPersistenceEngine.lastInstance.isDefined shouldBe true
      val persistenceEngine = CustomPersistenceEngine.lastInstance.get

      persistenceEngine.addApplication(appToPersist)
      persistenceEngine.addDriver(driverToPersist)
      persistenceEngine.addWorker(workerToPersist)

      val (apps, drivers, workers) = persistenceEngine.readPersistedData()

      apps.map(_.id) should contain(appToPersist.id)
      drivers.map(_.id) should contain(driverToPersist.id)
      workers.map(_.id) should contain(workerToPersist.id)

    } finally {
      actorSystem.shutdown()
      actorSystem.awaitTermination()
    }

    CustomRecoveryModeFactory.instantiationAttempts should be > instantiationAttempts
  }

  test("Master & worker web ui available") {
    implicit val formats = org.json4s.DefaultFormats
    val conf = new SparkConf()
    val localCluster = new LocalSparkCluster(2, 2, 512, conf)
    localCluster.start()
    try {
      eventually(timeout(5 seconds), interval(100 milliseconds)) {
        val json = Source.fromURL(s"http://localhost:${localCluster.masterWebUIPort}/json")
          .getLines().mkString("\n")
        val JArray(workers) = (parse(json) \ "workers")
        workers.size should be (2)
        workers.foreach { workerSummaryJson =>
          val JString(workerWebUi) = workerSummaryJson \ "webuiaddress"
          val workerResponse = parse(Source.fromURL(s"${workerWebUi}/json")
            .getLines().mkString("\n"))
          (workerResponse \ "cores").extract[Int] should be (2)
        }
      }
    } finally {
      localCluster.stop()
    }
  }

}
