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

import akka.actor.Address
import org.scalatest.FunSuite

import org.apache.spark.{SSLOptions, SparkConf, SparkException}

class MasterSuite extends FunSuite {

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
}
