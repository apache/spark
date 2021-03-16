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

class KubernetesTolerationsUtilsSuite extends SparkFunSuite {
  test("Parses hostPath volumes correctly") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.0.effect", "NoSchedule")
    sparkConf.set("test.0.key", "key")
    sparkConf.set("test.0.operator", "Exists")
    sparkConf.set("test.1.effect", "NoSchedule")
    sparkConf.set("test.1.key", "key")
    sparkConf.set("test.1.operator", "Exists")

    val toletrationSpec = KubernetesTolerationsUtils
      .parseTolerationsWithPrefix(sparkConf, "test.").head.get

    assert(toletrationSpec.effect === "NoSchedule")
    assert(toletrationSpec.key === "key")
    assert(toletrationSpec.operator === "Exists")
    assert(toletrationSpec.tolerationSeconds === null)
    assert(toletrationSpec.value === null)

  }
}
