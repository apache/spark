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

package org.apache.spark.deploy.yarn

import org.scalatest.{BeforeAndAfterAll, Matchers}

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}

class ResourceRequestValidatorSuite extends SparkFunSuite with Matchers with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  test("empty SparkConf should be valid") {
    val sparkConf = new SparkConf()
    ResourceRequestValidator.validateResources(sparkConf)
  }

  test("just normal resources are defined") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.memory", "3G")
    sparkConf.set("spark.driver.cores", "4")
    sparkConf.set("spark.executor.memory", "4G")
    sparkConf.set("spark.executor.cores", "2")

    ResourceRequestValidator.validateResources(sparkConf)
  }

  test("Memory defined with new config for executor") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.yarn.executor.resource.memory", "30G")

    val thrown = intercept[SparkException] {
      ResourceRequestValidator.validateResources(sparkConf)
    }
    thrown.getMessage should include ("spark.yarn.executor.resource.memory")
  }

  test("Cores defined with new config for executor") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.yarn.executor.resource.cores", "5")

    val thrown = intercept[SparkException] {
      ResourceRequestValidator.validateResources(sparkConf)
    }
    thrown.getMessage should include ("spark.yarn.executor.resource.cores")
  }

  test("Memory defined with new config, client mode") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.yarn.am.resource.memory", "1G")

    val thrown = intercept[SparkException] {
      ResourceRequestValidator.validateResources(sparkConf)
    }
    thrown.getMessage should include ("spark.yarn.am.resource.memory")
  }

  test("Memory defined with new config for driver, cluster mode") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.yarn.driver.resource.memory", "1G")

    val thrown = intercept[SparkException] {
      ResourceRequestValidator.validateResources(sparkConf)
    }
    thrown.getMessage should include ("spark.yarn.driver.resource.memory")
  }

  test("Cores defined with new config, client mode") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.memory", "2G")
    sparkConf.set("spark.driver.cores", "4")
    sparkConf.set("spark.executor.memory", "4G")
    sparkConf.set("spark.yarn.am.cores", "2")

    sparkConf.set("spark.yarn.am.resource.cores", "3")

    val thrown = intercept[SparkException] {
      ResourceRequestValidator.validateResources(sparkConf)
    }
    thrown.getMessage should include ("spark.yarn.am.resource.cores")
  }

  test("Cores defined with new config for driver, cluster mode") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.yarn.driver.resource.cores", "1G")

    val thrown = intercept[SparkException] {
      ResourceRequestValidator.validateResources(sparkConf)
    }
    thrown.getMessage should include ("spark.yarn.driver.resource.cores")
  }

  test("various duplicated definitions") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.memory", "2G")
    sparkConf.set("spark.executor.memory", "2G")
    sparkConf.set("spark.yarn.am.memory", "3G")
    sparkConf.set("spark.driver.cores", "2")
    sparkConf.set("spark.executor.cores", "4")

    sparkConf.set("spark.yarn.executor.resource.memory", "3G")
    sparkConf.set("spark.yarn.am.resource.memory", "2G")
    sparkConf.set("spark.yarn.driver.resource.memory", "2G")

    val thrown = intercept[SparkException] {
      ResourceRequestValidator.validateResources(sparkConf)
    }
    thrown.getMessage should (
      include("spark.yarn.executor.resource.memory") and
        include("spark.yarn.am.resource.memory") and
        include("spark.yarn.driver.resource.memory")
      )
  }

}
