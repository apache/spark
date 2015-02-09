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

package org.apache.spark.deploy

import scala.collection.mutable.ArrayBuffer

import akka.actor.ActorSystem

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.worker.Worker
import org.apache.spark.deploy.master.Master
import org.apache.spark.util.Utils

/**
 * Testing class that creates a Spark standalone process in-cluster (that is, running the
 * spark.deploy.master.Master and spark.deploy.worker.Workers in the same JVMs). Executors launched
 * by the Workers still run in separate JVMs. This can be used to test distributed operation and
 * fault recovery without spinning up a lot of processes.
 */
private[spark]
class LocalSparkCluster(
    numWorkers: Int,
    coresPerWorker: Int,
    memoryPerWorker: Int,
    conf: SparkConf)
  extends Logging {

  private val localHostname = Utils.localHostName()
  private val masterActorSystems = ArrayBuffer[ActorSystem]()
  private val workerActorSystems = ArrayBuffer[ActorSystem]()

  def start(): Array[String] = {
    logInfo("Starting a local Spark cluster with " + numWorkers + " workers.")

    // Disable REST server on Master in this mode unless otherwise specified
    val _conf = conf.clone().setIfMissing("spark.master.rest.enabled", "false")

    /* Start the Master */
    val (masterSystem, masterPort, _, _) = Master.startSystemAndActor(localHostname, 0, 0, _conf)
    masterActorSystems += masterSystem
    val masterUrl = "spark://" + localHostname + ":" + masterPort
    val masters = Array(masterUrl)

    /* Start the Workers */
    for (workerNum <- 1 to numWorkers) {
      val (workerSystem, _) = Worker.startSystemAndActor(localHostname, 0, 0, coresPerWorker,
        memoryPerWorker, masters, null, Some(workerNum))
      workerActorSystems += workerSystem
    }

    masters
  }

  def stop() {
    logInfo("Shutting down local Spark cluster.")
    // Stop the workers before the master so they don't get upset that it disconnected
    // TODO: In Akka 2.1.x, ActorSystem.awaitTermination hangs when you have remote actors!
    //       This is unfortunate, but for now we just comment it out.
    workerActorSystems.foreach(_.shutdown())
    // workerActorSystems.foreach(_.awaitTermination())
    masterActorSystems.foreach(_.shutdown())
    // masterActorSystems.foreach(_.awaitTermination())
    masterActorSystems.clear()
    workerActorSystems.clear()
  }
}
