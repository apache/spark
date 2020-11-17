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

package org.apache.spark.deploy.k8s.submit

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite

class KubernetesClientUtilsSuite extends SparkFunSuite with BeforeAndAfter {
  test("verify truncate to size sorts based on size and then truncates to correct size") {
    val input = Seq("test" -> "test123", "z12" -> "zZ", "rere" -> "@31")
    val output = KubernetesClientUtils.truncateToSize(input, 15)
    val expectedOutput = Map("z12" -> "zZ", "rere" -> "@31")
    assert(output === expectedOutput)
  }
  test("verify truncate to size does not truncate, when the maxsize limit is not exceeded") {
    val input = Seq("test" -> "test123", "z12" -> "zZ", "rere" -> "@31")
    val output = KubernetesClientUtils.truncateToSize(input, 35)
    val expectedOutput = input.toMap
    assert(output === expectedOutput)
  }
  test("verify truncate to size does truncate, when all keys and values are of equal size") {
    val input = Seq("testConf.5" -> "test123", "testConf.1" -> "test123",
      "testConf.3" -> "test123", "testConf.2" -> "test123")
    val output = KubernetesClientUtils.truncateToSize(input, 40)
    val expectedOutput = Map("testConf.1" -> "test123", "testConf.2" -> "test123")
    assert(output === expectedOutput)
  }
  test("verify truncate to size does truncate, when keys are very large in number.") {
    val input = for (i <- 10000 to 1 by -1) yield (s"testConf.${i}" -> "test123456")
    val output = KubernetesClientUtils.truncateToSize(input, 60)
    val expectedOutput = Map("testConf.1" -> "test123456", "testConf.2" -> "test123456")
    assert(output === expectedOutput)
    val output1 = KubernetesClientUtils.truncateToSize(input, 250000)
    assert(output1 === input.toMap)
  }
}
