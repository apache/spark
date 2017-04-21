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

package org.apache.spark.deploy.yarn.security

import org.apache.hadoop.conf.Configuration
import org.scalatest.Matchers

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}

class YARNHadoopAccessManagerSuite extends SparkFunSuite with Matchers {

  test("check token renewer") {
    val hadoopConf = new Configuration()
    hadoopConf.set("yarn.resourcemanager.address", "myrm:8033")
    hadoopConf.set("yarn.resourcemanager.principal", "yarn/myrm:8032@SPARKTEST.COM")

    val sparkConf = new SparkConf()
    val yarnHadoopAccessManager = new YARNHadoopAccessManager(hadoopConf, sparkConf)

    val renewer = yarnHadoopAccessManager.getTokenRenewer
    renewer should be ("yarn/myrm:8032@SPARKTEST.COM")
  }

  test("check token renewer default") {
    val hadoopConf = new Configuration()
    val sparkConf = new SparkConf()
    val yarnHadoopAccessManager = new YARNHadoopAccessManager(hadoopConf, sparkConf)

    val caught =
      intercept[SparkException] {
        yarnHadoopAccessManager.getTokenRenewer
      }
    assert(caught.getMessage === "Can't get Master Kerberos principal for use as renewer")
  }
}
