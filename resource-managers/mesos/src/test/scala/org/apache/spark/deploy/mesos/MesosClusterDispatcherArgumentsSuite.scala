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

package org.apache.spark.deploy.mesos

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.TestPrematureExit

class MesosClusterDispatcherArgumentsSuite extends SparkFunSuite
  with TestPrematureExit {

  test("test if spark config args are passed sucessfully") {
    val args = Array[String]("--master", "mesos://localhost:5050", "--conf", "key1=value1",
      "--conf", "spark.mesos.key2=value2", "--verbose")
    val conf = new SparkConf()
    new MesosClusterDispatcherArguments(args, conf)

    assert(conf.getOption("key1").isEmpty)
    assert(conf.get("spark.mesos.key2") == "value2")
  }

  test("test non conf settings") {
    val masterUrl = "mesos://localhost:5050"
    val port = "1212"
    val zookeeperUrl = "zk://localhost:2181"
    val host = "localhost"
    val webUiPort = "2323"
    val name = "myFramework"

    val args1 = Array("--master", masterUrl, "--verbose", "--name", name)
    val args2 = Array("-p", port, "-h", host, "-z", zookeeperUrl)
    val args3 = Array("--webui-port", webUiPort)

    val args = args1 ++ args2 ++ args3
    val conf = new SparkConf()
    val mesosDispClusterArgs = new MesosClusterDispatcherArguments(args, conf)

    assert(mesosDispClusterArgs.verbose)
    assert(mesosDispClusterArgs.confProperties.isEmpty)
    assert(mesosDispClusterArgs.host == host)
    assert(Option(mesosDispClusterArgs.masterUrl).isDefined)
    assert(mesosDispClusterArgs.masterUrl == masterUrl.stripPrefix("mesos://"))
    assert(Option(mesosDispClusterArgs.zookeeperUrl).isDefined)
    assert(mesosDispClusterArgs.zookeeperUrl == Some(zookeeperUrl))
    assert(mesosDispClusterArgs.name == name)
    assert(mesosDispClusterArgs.webUiPort == webUiPort.toInt)
    assert(mesosDispClusterArgs.port == port.toInt)
  }
}
