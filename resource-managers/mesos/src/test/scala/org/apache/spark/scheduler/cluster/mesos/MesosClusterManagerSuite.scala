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

package org.apache.spark.scheduler.cluster.mesos

import org.apache.spark._
import org.apache.spark.deploy.mesos.{config => mesosConfig}
import org.apache.spark.internal.config._

class MesosClusterManagerSuite extends SparkFunSuite with LocalSparkContext {
    def testURL(masterURL: String, expectedClass: Class[_], coarse: Boolean): Unit = {
      val conf = new SparkConf().set(mesosConfig.COARSE_MODE, coarse)
      sc = new SparkContext("local", "test", conf)
      val clusterManager = new MesosClusterManager()

      assert(clusterManager.canCreate(masterURL))
      val taskScheduler = clusterManager.createTaskScheduler(sc, masterURL)
      val sched = clusterManager.createSchedulerBackend(sc, masterURL, taskScheduler)
      assert(sched.getClass === expectedClass)
    }

    test("mesos fine-grained") {
      testURL("mesos://localhost:1234", classOf[MesosFineGrainedSchedulerBackend], coarse = false)
    }

    test("mesos coarse-grained") {
      testURL("mesos://localhost:1234", classOf[MesosCoarseGrainedSchedulerBackend], coarse = true)
    }

    test("mesos with zookeeper") {
      testURL("mesos://zk://localhost:1234,localhost:2345",
          classOf[MesosFineGrainedSchedulerBackend],
          coarse = false)
    }

    test("mesos with i/o encryption throws error") {
      val se = intercept[SparkException] {
        val conf = new SparkConf().setAppName("test").set(IO_ENCRYPTION_ENABLED, true)
        sc = new SparkContext("mesos", "test", conf)
      }
      assert(se.getCause().isInstanceOf[IllegalArgumentException])
    }
}
