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

package org.apache.spark.deploy.worker

import akka.actor._

import org.apache.spark.SparkConf
import org.apache.spark.util.{AkkaUtils, Utils}

/**
 * Utility object for launching driver programs such that they share fate with the Worker process.
 */
object DriverWrapper {
  def main(args: Array[String]) {
    args.toList match {
      case workerUrl :: mainClass :: extraArgs =>
        val (actorSystem, _) = AkkaUtils.createActorSystem("Driver",
          Utils.localHostName(), 0, false, new SparkConf())
        actorSystem.actorOf(Props(classOf[WorkerWatcher], workerUrl), name = "workerWatcher")

        // Delegate to supplied main class
        val clazz = Class.forName(args(1))
        val mainMethod = clazz.getMethod("main", classOf[Array[String]])
        mainMethod.invoke(null, extraArgs.toArray[String])

        actorSystem.shutdown()

      case _ =>
        System.err.println("Usage: DriverWrapper <workerUrl> <driverMainClass> [options]")
        System.exit(-1)
    }
  }
}