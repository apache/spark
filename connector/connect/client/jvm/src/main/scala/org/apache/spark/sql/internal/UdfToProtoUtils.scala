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
package org.apache.spark.sql.internal

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.google.protobuf.ByteString

import org.apache.spark.SparkException
import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, RowEncoder}
import org.apache.spark.sql.connect.common.DataTypeProtoConverter.toConnectProtoType
import org.apache.spark.sql.connect.common.UdfPacket
import org.apache.spark.sql.encoderFor
import org.apache.spark.sql.expressions.{SparkUserDefinedFunction, UserDefinedAggregator, UserDefinedFunction}
import org.apache.spark.util.{ClosureCleaner, SparkClassUtils, SparkSerDeUtils}

/**
 * Utility for converting a [[UserDefinedFunction]] into a Connect Protobuf message.
 */
private[sql] object UdfToProtoUtils {
  private val LAMBDA_DESERIALIZATION_ERR_MSG: String =
    "cannot assign instance of java.lang.invoke.SerializedLambda to field"

  private def checkDeserializable(bytes: Array[Byte]): Unit = {
    try {
      SparkSerDeUtils.deserialize(bytes, SparkClassUtils.getContextOrSparkClassLoader)
    } catch {
      case e: ClassCastException if e.getMessage.contains(LAMBDA_DESERIALIZATION_ERR_MSG) =>
        throw new SparkException(
          "UDF cannot be executed on a Spark cluster: it cannot be deserialized. " +
            "This is very likely to be caused by the lambda function (the UDF) having a " +
            "self-reference. This is not supported by java serialization.")
      case NonFatal(e) =>
        throw new SparkException(
          "UDF cannot be executed on a Spark cluster: it cannot be deserialized.",
          e)
    }
  }

  private[sql] def toUdfPacketBytes(
      function: AnyRef,
      inputEncoders: Seq[AgnosticEncoder[_]],
      outputEncoder: AgnosticEncoder[_]): ByteString = {
    ClosureCleaner.clean(function, cleanTransitively = true, mutable.Map.empty)
    val bytes = SparkSerDeUtils.serialize(UdfPacket(function, inputEncoders, outputEncoder))
    checkDeserializable(bytes)
    ByteString.copyFrom(bytes)
  }

  /**
   * Convert a [[UserDefinedFunction]] to a [[proto.CommonInlineUserDefinedFunction]].
   */
  private[sql] def toProto(
      udf: UserDefinedFunction,
      arguments: Seq[proto.Expression] = Nil): proto.CommonInlineUserDefinedFunction = {
    val invokeUdf = proto.CommonInlineUserDefinedFunction.newBuilder()
      .setDeterministic(udf.deterministic)
      .addAllArguments(arguments.asJava)
    val protoUdf = invokeUdf.getScalarScalaUdfBuilder
      .setNullable(udf.nullable)
    udf match {
      case f: SparkUserDefinedFunction =>
        val outputEncoder = f.outputEncoder.map(e => encoderFor(e))
          .getOrElse(RowEncoder.encoderForDataType(f.dataType, lenient = false))
        val inputEncoders = f.inputEncoders.map(e => encoderFor(e.get)) // TODO support any?
        inputEncoders.foreach(e => protoUdf.addInputTypes(toConnectProtoType(e.dataType)))
        protoUdf
          .setPayload(toUdfPacketBytes(f, inputEncoders, outputEncoder))
          .setOutputType(toConnectProtoType(outputEncoder.dataType))
          .setAggregate(false)
        f.givenName.foreach(invokeUdf.setFunctionName)
      case f: UserDefinedAggregator[_, _, _] =>
        val outputEncoder = encoderFor(f.aggregator.outputEncoder)
        val inputEncoder = encoderFor(f.inputEncoder)
        protoUdf
          .setPayload(toUdfPacketBytes(f, inputEncoder :: Nil, outputEncoder))
          .addInputTypes(toConnectProtoType(inputEncoder.dataType))
          .setOutputType(toConnectProtoType(outputEncoder.dataType))
          .setAggregate(true)
        f.givenName.foreach(invokeUdf.setFunctionName)
    }
    invokeUdf.build()
  }
}
