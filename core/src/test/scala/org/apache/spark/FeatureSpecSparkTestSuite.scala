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

package org.apache.spark

import org.scalatest._
import org.scalatest.featurespec.AnyFeatureSpec

/**
 * This test is just for demonstration purpose.
 * Real test suites in Spark codebase should use SparkFunSuite.
 */
class FeatureSpecSparkTestSuite
  extends AnyFeatureSpec
    with GivenWhenThen
    with SparkTestSuite
    with LocalSparkContext {

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = new SparkContext("local", "test")
  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
      sc = null
    }
    super.afterAll()
  }

  Scenario("Compute count on an empty RDD") {
    Given("an empty RDD should have size 0")
    val rdd = sc.emptyRDD[Int]
    assert(rdd.count() === 0)
  }
}
