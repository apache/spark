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
package org.apache.spark.udf.worker.core

import com.google.protobuf.ByteString
// scalastyle:off funsuite
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.udf.worker.{DataResponse, UDFWorkerDataFormat}

class WorkerMessagesSuite extends AnyFunSuite {
// scalastyle:on funsuite

  test("udfPayload copies the bytes and sets format, eval type, and an optional name") {
    val unnamed = WorkerMessages.udfPayload(
      payload = Array[Byte](1, 2),
      format = "pyspark-udf-experimental",
      evalType = "100",
      name = None)
    assert(unnamed.getPayload.toByteArray.sameElements(Array[Byte](1, 2)))
    assert(unnamed.getFormat == "pyspark-udf-experimental")
    assert(unnamed.getEvalType == "100")
    assert(!unnamed.hasName)
    assert(unnamed.getName == "")

    val named = WorkerMessages.udfPayload(
      payload = Array[Byte](3),
      format = "pyspark-udf-experimental",
      evalType = "100",
      name = Some("add_one"))
    assert(named.getPayload.toByteArray.sameElements(Array[Byte](3)))
    assert(named.hasName)
    assert(named.getName == "add_one")
  }

  test("init builds the session message the Python adapter sends") {
    val udf = WorkerMessages.udfPayload(
      payload = Array[Byte](9),
      format = "pyspark-udf-experimental",
      evalType = "100",
      name = Some("identity"))
    val message = WorkerMessages.init(
      udf = udf,
      dataFormat = UDFWorkerDataFormat.ARROW,
      inputSchema = Array[Byte](4, 5),
      outputSchema = Array[Byte](6),
      timezone = "America/Los_Angeles",
      sessionConf = Map("input_type" -> "struct", "spark.sql.arrow.pyspark.enabled" -> "true"))

    assert(message.getProtocolVersion == 1)
    assert(message.getDataFormat == UDFWorkerDataFormat.ARROW)
    assert(message.getUdf == udf)
    assert(message.getInputSchema.toByteArray.sameElements(Array[Byte](4, 5)))
    assert(message.getOutputSchema.toByteArray.sameElements(Array[Byte](6)))
    assert(message.getTimezone == "America/Los_Angeles")
    assert(message.getTaskContextCount == 0)
    val sessionConf = message.getSessionConfMap
    assert(sessionConf.size == 2)
    assert(sessionConf.get("input_type") == "struct")
    assert(sessionConf.get("spark.sql.arrow.pyspark.enabled") == "true")
  }

  test("dataRequest wraps the batch array without copying") {
    val batch = Array[Byte](1, 2, 3, 4)
    val request = WorkerMessages.dataRequest(batch)
    val wrapped = request.getData.asReadOnlyByteBuffer()
    assert(wrapped.remaining() == 4)
    assert(wrapped.get(0) == 1.toByte)
    assert(wrapped.get(1) == 2.toByte)
    assert(wrapped.get(2) == 3.toByte)
    assert(wrapped.get(3) == 4.toByte)

    batch(1) = 9
    assert(request.getData.asReadOnlyByteBuffer().get(1) == 9.toByte)
  }

  test("data returns a stream of the response payload") {
    val response = DataResponse
      .newBuilder()
      .setData(ByteString.copyFrom(Array[Byte](9, 8, 7)))
      .build()
    val stream = WorkerMessages.data(response)
    assert(stream.readAllBytes.sameElements(Array[Byte](9, 8, 7)))
  }
}
