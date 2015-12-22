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

import java.nio.ByteBuffer

import org.apache.spark.SparkFunSuite

class MesosTaskLaunchDataSuite extends SparkFunSuite {
  test("serialize and deserialize data must be same") {
    val serializedTask = ByteBuffer.allocate(40)
    (Range(100, 110).map(serializedTask.putInt(_)))
    serializedTask.rewind
    val attemptNumber = 100
    val byteString = MesosTaskLaunchData(serializedTask, attemptNumber).toByteString
    serializedTask.rewind
    val mesosTaskLaunchData = MesosTaskLaunchData.fromByteString(byteString)
    assert(mesosTaskLaunchData.attemptNumber == attemptNumber)
    assert(mesosTaskLaunchData.serializedTask.equals(serializedTask))
  }
}
