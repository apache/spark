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
package org.apache.spark.deploy.k8s

import org.apache.spark.{SparkConf, SparkFunSuite}

class KubernetesPodAffinityUtilsSuite extends SparkFunSuite {
  test("Parses affinity with defaults") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.key", "label")
    sparkConf.set("test.value", "value")

    val affinitySpec = KubernetesPodAffinityUtils
      .parsePodAffinityWithPrefix(sparkConf, "test.")

    assert(affinitySpec.key === "label")
    assert(affinitySpec.value === "value")
    assert(affinitySpec.operator === "In")
    assert(affinitySpec.topology === "kubernetes.io/hostname")
    assert(affinitySpec.weight === "100")
  }

  test("Parses affinity with all values") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.key", "label")
    sparkConf.set("test.value", "value")
    sparkConf.set("test.operator", "operator")
    sparkConf.set("test.topology", "topology")
    sparkConf.set("test.weight", "weight")

    val affinitySpec = KubernetesPodAffinityUtils
      .parsePodAffinityWithPrefix(sparkConf, "test.")

    assert(affinitySpec.key === "label")
    assert(affinitySpec.value === "value")
    assert(affinitySpec.operator === "operator")
    assert(affinitySpec.topology === "topology")
    assert(affinitySpec.weight === "weight")
  }

  test("Parses with no affinity") {
    val sparkConf = new SparkConf(false)
    val affinitySpec = KubernetesPodAffinityUtils
      .parsePodAffinityWithPrefix(sparkConf, "test.")


    assert(affinitySpec.key === null)
    assert(affinitySpec.value === null)

  }
}
