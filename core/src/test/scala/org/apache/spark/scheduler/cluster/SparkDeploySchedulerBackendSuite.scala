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

package org.apache.spark.scheduler.cluster

import org.scalatest.PrivateMethodTester

import org.apache.spark._
import org.apache.spark.deploy.ApplicationDescription

class SparkDeploySchedulerBackendSuite
  extends SparkFunSuite with LocalSparkContext with PrivateMethodTester {

  val buildAppDesc = PrivateMethod[ApplicationDescription]('buildAppDescription)

  test("Auth secret shouldn't appear in the command when auth is not set") {
    val conf = new SparkConf
    // always set secret
    conf.set(SecurityManager.CLUSTER_AUTH_SECRET_CONF, "This is the secret sauce")
    sc = new SparkContext("local", "test", conf)
    val appDesc = SparkDeploySchedulerBackend invokePrivate buildAppDesc("", Option(1), sc)
    assert(appDesc.appSecret === None)
    assert(!appDesc.command.javaOpts.exists(
      _.startsWith("-D" + SecurityManager.CLUSTER_AUTH_CONF)))
    assert(!appDesc.command.javaOpts.exists(
      _.startsWith("-D" + SecurityManager.CLUSTER_AUTH_SECRET_CONF)))
  }

  test("Auth secret shouldn't appear in the command when auth is set to false") {
    val conf = new SparkConf
    conf.set(SecurityManager.CLUSTER_AUTH_CONF, "false")
    // always set secret
    conf.set(SecurityManager.CLUSTER_AUTH_SECRET_CONF, "This is the secret sauce")
    sc = new SparkContext("local", "test", conf)
    val appDesc = SparkDeploySchedulerBackend invokePrivate buildAppDesc("", Option(1), sc)
    assert(appDesc.appSecret === None)
    assert(appDesc.command.javaOpts.contains(
      "-D" + SecurityManager.CLUSTER_AUTH_CONF + "=false"))
    assert(!appDesc.command.javaOpts.exists(
      _.startsWith("-D" + SecurityManager.CLUSTER_AUTH_SECRET_CONF)))
  }

  test("Auth secret shouldn't appear in the command when auth is set to true") {
    val conf = new SparkConf
    conf.set(SecurityManager.CLUSTER_AUTH_CONF, "true")
    // always set secret
    conf.set(SecurityManager.CLUSTER_AUTH_SECRET_CONF, "This is the secret sauce")
    sc = new SparkContext("local", "test", conf)
    val appDesc = SparkDeploySchedulerBackend invokePrivate buildAppDesc("", Option(1), sc)
    assert(appDesc.appSecret === Some("This is the secret sauce"))
    assert(appDesc.command.javaOpts.contains(
      "-D" + SecurityManager.CLUSTER_AUTH_CONF + "=true"))
    assert(!appDesc.command.javaOpts.exists(
      _.startsWith("-D" + SecurityManager.CLUSTER_AUTH_SECRET_CONF)))
  }
}
