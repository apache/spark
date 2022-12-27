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

package org.apache.spark.sql.streaming.protobuf

import java.util.UUID

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.streaming.ui.StreamingQueryData
import org.apache.spark.status.protobuf.KVStoreProtobufSerializer

class KVStoreProtobufSerializerSuite extends SparkFunSuite {

  private val serializer = new KVStoreProtobufSerializer()

  test("StreamingQueryData") {
    val id = UUID.randomUUID()
    val input = new StreamingQueryData(
      name = "some-query",
      id = id,
      runId = id.toString,
      isActive = false,
      exception = Some("Some Exception"),
      startTimestamp = 1L,
      endTimestamp = Some(2L)
    )
    val bytes = serializer.serialize(input)
    val result = serializer.deserialize(bytes, classOf[StreamingQueryData])
    assert(result.name == input.name)
    assert(result.id == input.id)
    assert(result.runId == input.runId)
    assert(result.isActive == input.isActive)
    assert(result.exception == input.exception)
    assert(result.startTimestamp == input.startTimestamp)
    assert(result.endTimestamp == input.endTimestamp)
  }
}
