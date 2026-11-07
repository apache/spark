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

import java.io.InputStream

import scala.jdk.CollectionConverters._

import com.google.protobuf.{ByteString, UnsafeByteOperations}

import org.apache.spark.annotation.Experimental
import org.apache.spark.udf.worker.{DataRequest, DataResponse, Init, UdfPayload}
import org.apache.spark.udf.worker.UDFWorkerDataFormat

/**
 * :: Experimental :: Builds and reads UDF worker messages from plain JVM values.
 *
 * Engine modules relocate protobuf into one package, and the packaged worker runtime relocates it
 * into another. A method descriptor that names a protobuf runtime type is rewritten with the
 * caller's relocation, so it no longer matches the worker message. Those types stay in this
 * object. Signatures expose generated UDF messages and enums, JDK types, and Scala types.
 */
@Experimental
object WorkerMessages {
  def udfPayload(
      payload: Array[Byte],
      format: String,
      evalType: String,
      name: Option[String]): UdfPayload = {
    val builder = UdfPayload
      .newBuilder()
      .setPayload(ByteString.copyFrom(payload))
      .setFormat(format)
      .setEvalType(evalType)
    name.foreach(builder.setName)
    builder.build()
  }

  def init(
      udf: UdfPayload,
      dataFormat: UDFWorkerDataFormat,
      inputSchema: Array[Byte],
      outputSchema: Array[Byte],
      timezone: String,
      sessionConf: Map[String, String]): Init = {
    Init
      .newBuilder()
      .setProtocolVersion(1)
      .setDataFormat(dataFormat)
      .setUdf(udf)
      .setInputSchema(ByteString.copyFrom(inputSchema))
      .setOutputSchema(ByteString.copyFrom(outputSchema))
      .setTimezone(timezone)
      .putAllSessionConf(sessionConf.asJava)
      .build()
  }

  /** Wraps `arrowBatch` without copying. The caller must not mutate the array afterwards. */
  def dataRequest(arrowBatch: Array[Byte]): DataRequest = {
    DataRequest
      .newBuilder()
      .setData(UnsafeByteOperations.unsafeWrap(arrowBatch))
      .build()
  }

  def data(response: DataResponse): InputStream = response.getData.newInput()
}
