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

package org.apache.spark.network.netty

import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.network.util.NettyUtils

class SparkTransportConfSuite extends SparkFunSuite with MockitoSugar {
  val module = "rpc"
  val serThreads = "serverThreads"
  val cliThreads = "clientThreads"

  test("default value is get when neither role nor module is set") {
    val numUsableCores = 4
    val conf = new SparkConf()
    val sparkTransportConf = SparkTransportConf.fromSparkConf(conf, module, numUsableCores, None)
    val expected = NettyUtils.defaultNumThreads(numUsableCores)
    val serActual = sparkTransportConf.get(s"spark.$module.io.$serThreads", "")
    val cliActual = sparkTransportConf.get(s"spark.$module.io.$cliThreads", "")
    assert(serActual == expected.toString)
    assert(cliActual == expected.toString)
  }

  test("module value is get when role is not set") {
    val numUsableCores = 3
    val serExpected = "7"
    val cliExpected = "5"
    val conf = new SparkConf()
      .set(s"spark.$module.io.$serThreads", serExpected)
      .set(s"spark.$module.io.$cliThreads", cliExpected)
    val sparkTransportConf = SparkTransportConf.fromSparkConf(conf, module, numUsableCores, None)
    val serActual = sparkTransportConf.get(s"spark.$module.io.$serThreads", "")
    val cliActual = sparkTransportConf.get(s"spark.$module.io.$cliThreads", "")
    assert(serActual == serExpected)
    assert(cliActual == cliExpected)
  }

  test("use correct configuration when both module and role configs are present") {
    val role = Some("driver")
    val numUsableCores = 10
    val serModule = "7"
    val cliModule = "5"
    val serExpected = "8"
    val cliExpected = "6"
    val conf = new SparkConf()
      .set(s"spark.$module.io.$serThreads", serModule)
      .set(s"spark.$module.io.$cliThreads", cliModule)
      .set(s"spark.${role.get}.$module.io.$serThreads", serExpected)
      .set(s"spark.${role.get}.$module.io.$cliThreads", cliExpected)
    val sparkTransportConf = SparkTransportConf.fromSparkConf(conf, module, numUsableCores, role)
    val serActual = sparkTransportConf.get(s"spark.$module.io.$serThreads", "")
    val cliActual = sparkTransportConf.get(s"spark.$module.io.$cliThreads", "")
    assert(serActual == serExpected)
    assert(cliActual == cliExpected)

    val exeRole = Some("executor")
    val sparkTransConfExe = SparkTransportConf.fromSparkConf(conf, module, numUsableCores, exeRole)
    val serActualExe = sparkTransConfExe.get(s"spark.$module.io.$serThreads", "")
    val cliActualExe = sparkTransConfExe.get(s"spark.$module.io.$cliThreads", "")
    assert(serActualExe == serModule)
    assert(cliActualExe == cliModule)
  }
}
