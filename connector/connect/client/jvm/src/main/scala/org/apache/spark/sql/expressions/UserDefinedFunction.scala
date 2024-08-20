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
package org.apache.spark.sql.expressions

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe.TypeTag
import scala.util.control.NonFatal

import com.google.protobuf.ByteString

import org.apache.spark.SparkException
import org.apache.spark.connect.proto
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, RowEncoder}
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, UdfPacket}
import org.apache.spark.sql.internal.UserDefinedFunctionLike
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.{ClosureCleaner, SparkClassUtils, SparkSerDeUtils}


/**
 * Holder class for a scala user-defined function and it's input/output encoder(s).
 */
case class ScalaUserDefinedFunction private[sql] (
    // SPARK-43198: Eagerly serialize to prevent the UDF from containing a reference to this class.
    serializedUdfPacket: Array[Byte],
    inputTypes: Seq[proto.DataType],
    outputType: proto.DataType,
    givenName: Option[String],
    override val nullable: Boolean,
    override val deterministic: Boolean,
    aggregate: Boolean)
    extends UserDefinedFunction
    with UserDefinedFunctionLike {

  private[sql] lazy val udf = {
    val scalaUdfBuilder = proto.ScalarScalaUDF
      .newBuilder()
      .setPayload(ByteString.copyFrom(serializedUdfPacket))
      // Send the real inputs and return types to obtain the types without deser the udf bytes.
      .addAllInputTypes(inputTypes.asJava)
      .setOutputType(outputType)
      .setNullable(nullable)
      .setAggregate(aggregate)

    scalaUdfBuilder.build()
  }

  override def withName(name: String): ScalaUserDefinedFunction = copy(givenName = Option(name))

  override def asNonNullable(): ScalaUserDefinedFunction = copy(nullable = false)

  override def asNondeterministic(): ScalaUserDefinedFunction = copy(deterministic = false)

  def toProto: proto.CommonInlineUserDefinedFunction = {
    val builder = proto.CommonInlineUserDefinedFunction.newBuilder()
    builder
      .setDeterministic(deterministic)
      .setScalarScalaUdf(udf)

    givenName.foreach(builder.setFunctionName)
    builder.build()
  }

  override def name: String = givenName.getOrElse("UDF")
}

object ScalaUserDefinedFunction {
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

  private[sql] def apply(
      function: AnyRef,
      returnType: TypeTag[_],
      parameterTypes: TypeTag[_]*): ScalaUserDefinedFunction = {

    ScalaUserDefinedFunction(
      function = function,
      // Input can be a row because the input data schema can be found from the plan.
      inputEncoders =
        parameterTypes.map(tag => ScalaReflection.encoderForWithRowEncoderSupport(tag)),
      // Output cannot be a row as there is no good way to get the return data type.
      outputEncoder = ScalaReflection.encoderFor(returnType))
  }

  private[sql] def apply(
      function: AnyRef,
      inputEncoders: Seq[AgnosticEncoder[_]],
      outputEncoder: AgnosticEncoder[_],
      aggregate: Boolean = false): ScalaUserDefinedFunction = {
    SparkConnectClosureCleaner.clean(function)
    val udfPacketBytes =
      SparkSerDeUtils.serialize(UdfPacket(function, inputEncoders, outputEncoder))
    checkDeserializable(udfPacketBytes)
    ScalaUserDefinedFunction(
      serializedUdfPacket = udfPacketBytes,
      inputTypes = inputEncoders.map(_.dataType).map(DataTypeProtoConverter.toConnectProtoType),
      outputType = DataTypeProtoConverter.toConnectProtoType(outputEncoder.dataType),
      givenName = None,
      nullable = true,
      deterministic = true,
      aggregate = aggregate)
  }

  private[sql] def apply(function: AnyRef, returnType: DataType): ScalaUserDefinedFunction = {
    ScalaUserDefinedFunction(
      function = function,
      inputEncoders = Seq.empty[AgnosticEncoder[_]],
      outputEncoder = RowEncoder.encoderForDataType(returnType, lenient = false))
  }
}

private object SparkConnectClosureCleaner {
  def clean(closure: AnyRef): Unit = {
    ClosureCleaner.clean(closure, cleanTransitively = true, mutable.Map.empty)
  }
}
