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

package org.apache.spark.scheduler.cluster.mesos

import org.apache.spark.SparkFunSuite

class MesosProtoUtilsSuite extends SparkFunSuite {
  test("mesosLabels") {
    val labels = MesosProtoUtils.mesosLabels("key:value")
    assert(labels.getLabelsCount == 1)
    val label = labels.getLabels(0)
    assert(label.getKey == "key")
    assert(label.getValue == "value")

    val labels2 = MesosProtoUtils.mesosLabels("key:value\\:value")
    assert(labels2.getLabelsCount == 1)
    val label2 = labels2.getLabels(0)
    assert(label2.getKey == "key")
    assert(label2.getValue == "value:value")

    val labels3 = MesosProtoUtils.mesosLabels("key:value,key2:value2")
    assert(labels3.getLabelsCount == 2)
    assert(labels3.getLabels(0).getKey == "key")
    assert(labels3.getLabels(0).getValue == "value")
    assert(labels3.getLabels(1).getKey == "key2")
    assert(labels3.getLabels(1).getValue == "value2")

    val labels4 = MesosProtoUtils.mesosLabels("key:value\\,value")
    assert(labels4.getLabelsCount == 1)
    assert(labels4.getLabels(0).getKey == "key")
    assert(labels4.getLabels(0).getValue == "value,value")
  }
}
