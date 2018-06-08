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

class KubernetesVolumeUtilsSuite extends SparkFunSuite {
  test("Parses volume options correctly") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.volumeType.volumeName.mount.path", "/path")
    sparkConf.set("test.volumeType.volumeName.mount.readOnly", "true")
    sparkConf.set("test.volumeType.volumeName.options.option1", "value1")
    sparkConf.set("test.volumeType.volumeName.options.option2", "value2")

    val volumeSpec = KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.").head
    assert(volumeSpec.volumeName === "volumeName")
    assert(volumeSpec.volumeType === "volumeType")
    assert(volumeSpec.mountPath === "/path")
    assert(volumeSpec.mountReadOnly === true)
    assert(volumeSpec.optionsSpec === Map("option1" -> "value1", "option2" -> "value2"))
  }
}
