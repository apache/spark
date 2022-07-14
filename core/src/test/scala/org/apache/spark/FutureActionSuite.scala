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

import scala.concurrent.duration.Duration

import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.util.ThreadUtils


class FutureActionSuite
  extends SparkFunSuite
  with BeforeAndAfter
  with Matchers
  with LocalSparkContext {

  before {
    sc = new SparkContext("local", "FutureActionSuite")
  }

  test("simple async action") {
    val rdd = sc.parallelize(1 to 10, 2)
    val job = rdd.countAsync()
    val res = ThreadUtils.awaitResult(job, Duration.Inf)
    res should be (10)
    job.jobIds.size should be (1)
  }

  test("complex async action") {
    val rdd = sc.parallelize(1 to 15, 3)
    val job = rdd.takeAsync(10)
    val res = ThreadUtils.awaitResult(job, Duration.Inf)
    res should be (1 to 10)
    job.jobIds.size should be (2)
  }

}
