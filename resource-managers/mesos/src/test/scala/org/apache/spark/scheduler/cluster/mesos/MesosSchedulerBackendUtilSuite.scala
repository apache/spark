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

import org.scalatest._
import org.scalatest.mock.MockitoSugar

import org.apache.spark.{SparkConf, SparkFunSuite}

class MesosSchedulerBackendUtilSuite extends SparkFunSuite with Matchers with MockitoSugar {

  test("Parse arbitrary parameter to pass into docker containerizer") {
    val parsed = MesosSchedulerBackendUtil.parseParamsSpec("a=1,b=2,c=3")
    parsed(0).getKey shouldBe "a"
    parsed(0).getValue shouldBe "1"
    parsed(1).getKey shouldBe "b"
    parsed(1).getValue shouldBe "2"
    parsed(2).getKey shouldBe "c"
    parsed(2).getValue shouldBe "3"

    val invalid = MesosSchedulerBackendUtil.parseParamsSpec("a,b")
    invalid.length shouldBe 0
  }

  test("ContainerInfo contains parsed arbitrary parameters") {
    val conf = new SparkConf()
    conf.set("spark.mesos.executor.docker.params", "a=1,b=2,c=3")
    conf.set("spark.mesos.executor.docker.image", "test")

    val containerInfo = MesosSchedulerBackendUtil.containerInfo(conf)
    val params = containerInfo.getDocker.getParametersList
    params.size() shouldBe 3
    params.get(0).getKey shouldBe "a"
    params.get(0).getValue shouldBe "1"
    params.get(1).getKey shouldBe "b"
    params.get(1).getValue shouldBe "2"
    params.get(2).getKey shouldBe "c"
    params.get(2).getValue shouldBe "3"
  }
}
