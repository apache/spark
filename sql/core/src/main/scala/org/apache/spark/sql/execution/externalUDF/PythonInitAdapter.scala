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

package org.apache.spark.sql.execution.externalUDF

import scala.jdk.CollectionConverters._

import com.google.protobuf.ByteString
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.apache.spark.{BarrierTaskContext, TaskContext}
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.resource.CpuAmount
import org.apache.spark.sql.catalyst.expressions.ExternalUserDefinedFunction
import org.apache.spark.sql.execution.python.ArrowPythonRunner
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.udf.worker.{Init, UdfPayload, UDFWorkerDataFormat, UDFWorkerSpecification}

/**
 * Builds the temporary Python-specific Init message for scalar external UDF execution.
 *
 * The worker specification is deliberately an argument even though this adapter does not inspect
 * it yet. The worker is not present, so the Init contract remains local to the Python adapter
 * until SPARK-59364 replaces it with language-agnostic initialization.
 */
private[externalUDF] object PythonInitAdapter {
  private val PYSPARK_UDF_PAYLOAD_FORMAT = "pyspark-udf-experimental"
  private val INPUT_TYPE_CONF = "input_type"

  private implicit val jsonFormats: Formats = Serialization.formats(NoTypeHints)

  /** Query-invariant Init fields that can be serialized with the partition closure. */
  final case class PreparedInit(template: Init) extends Serializable

  def sessionConf(conf: SQLConf, inputSchema: StructType): Map[String, String] = {
    (ArrowPythonRunner.getPythonRunnerConfMap(conf) - SQLConf.SESSION_LOCAL_TIMEZONE.key) +
      (INPUT_TYPE_CONF -> inputSchema.json)
  }

  def prepare(
      _workerSpec: UDFWorkerSpecification,
      udf: ExternalUserDefinedFunction,
      inputSchema: Array[Byte],
      outputSchema: Array[Byte],
      timeZoneId: String,
      pythonSessionConf: Map[String, String]): PreparedInit = {
    val udfBuilder = UdfPayload.newBuilder()
      .setPayload(ByteString.copyFrom(udf.payload))
      .setFormat(PYSPARK_UDF_PAYLOAD_FORMAT)
      .setEvalType(PythonEvalType.SQL_ARROW_BATCHED_UDF.toString)
    udf.name.foreach(udfBuilder.setName)

    PreparedInit(Init.newBuilder()
      .setProtocolVersion(1)
      .setDataFormat(UDFWorkerDataFormat.ARROW)
      .setUdf(udfBuilder)
      .setInputSchema(ByteString.copyFrom(inputSchema))
      .setOutputSchema(ByteString.copyFrom(outputSchema))
      .setTimezone(timeZoneId)
      .putAllSessionConf(pythonSessionConf.asJava)
      .build())
  }

  def build(prepared: PreparedInit, context: TaskContext): Init = {
    prepared.template.toBuilder
      .putAllTaskContext(taskContextAsMap(context).asJava)
      .build()
  }

  // TODO(SPARK-59364): Share this context mapping with PythonWorkerUtils when Init message
  // construction becomes language-agnostic, while preserving the existing Python wire contract.
  private def taskContextAsMap(context: TaskContext): Map[String, String] = {
    val resources = context.resources().map { case (name, resource) =>
      name -> Map("name" -> resource.name, "addresses" -> resource.addresses)
    }
    Map(
      "isBarrier" -> context.isInstanceOf[BarrierTaskContext].toString,
      "stageId" -> context.stageId().toString,
      "partitionId" -> context.partitionId().toString,
      "attemptNumber" -> context.attemptNumber().toString,
      "taskAttemptId" -> context.taskAttemptId().toString,
      "cpus" -> context.cpuAmount()
        .setScale(0, BigDecimal.RoundingMode.CEILING).intValue.toString,
      "cpuAmount" -> CpuAmount.toDisplayString(context.cpuAmount()),
      "resources" -> Serialization.write(resources),
      "localProperties" -> Serialization.write(context.getLocalProperties.asScala.toMap))
  }
}
