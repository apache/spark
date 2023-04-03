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

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag

import com.google.protobuf.ByteString

import org.apache.spark.connect.proto
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, UdfPacket}
import org.apache.spark.util.Utils

/**
 * A user-defined function. To create one, use the `udf` functions in `functions`.
 *
 * As an example:
 * {{{
 *   // Define a UDF that returns true or false based on some numeric score.
 *   val predict = udf((score: Double) => score > 0.5)
 *
 *   // Projects a column that adds a prediction column based on the score column.
 *   df.select( predict(df("score")) )
 * }}}
 *
 * @since 3.4.0
 */
sealed abstract class UserDefinedFunction {

  /**
   * Returns true when the UDF can return a nullable value.
   *
   * @since 3.4.0
   */
  def nullable: Boolean

  /**
   * Returns true iff the UDF is deterministic, i.e. the UDF produces the same output given the
   * same input.
   *
   * @since 3.4.0
   */
  def deterministic: Boolean

  /**
   * Returns an expression that invokes the UDF, using the given arguments.
   *
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def apply(exprs: Column*): Column

  /**
   * Updates UserDefinedFunction with a given name.
   *
   * @since 3.4.0
   */
  def withName(name: String): UserDefinedFunction

  /**
   * Updates UserDefinedFunction to non-nullable.
   *
   * @since 3.4.0
   */
  def asNonNullable(): UserDefinedFunction

  /**
   * Updates UserDefinedFunction to nondeterministic.
   *
   * @since 3.4.0
   */
  def asNondeterministic(): UserDefinedFunction
}

/**
 * Holder class for a scalar user-defined function and it's input/output encoder(s).
 */
case class ScalarUserDefinedFunction(
    function: AnyRef,
    inputEncoders: Seq[AgnosticEncoder[_]],
    outputEncoder: AgnosticEncoder[_],
    name: Option[String],
    override val nullable: Boolean,
    override val deterministic: Boolean)
    extends UserDefinedFunction {

  private[this] lazy val udf = {
    val udfPacketBytes = Utils.serialize(UdfPacket(function, inputEncoders, outputEncoder))
    val scalaUdfBuilder = proto.ScalarScalaUDF
      .newBuilder()
      .setPayload(ByteString.copyFrom(udfPacketBytes))
      // Send the real inputs and return types to obtain the types without deser the udf bytes.
      .addAllInputTypes(
        inputEncoders.map(_.dataType).map(DataTypeProtoConverter.toConnectProtoType).asJava)
      .setOutputType(DataTypeProtoConverter.toConnectProtoType(outputEncoder.dataType))
      .setNullable(nullable)

    scalaUdfBuilder.build()
  }

  @scala.annotation.varargs
  override def apply(exprs: Column*): Column = Column { builder =>
    val udfBuilder = builder.getCommonInlineUserDefinedFunctionBuilder
    udfBuilder
      .setDeterministic(deterministic)
      .setScalarScalaUdf(udf)
      .addAllArguments(exprs.map(_.expr).asJava)

    name.foreach(udfBuilder.setFunctionName)
  }

  override def withName(name: String): ScalarUserDefinedFunction = copy(name = Option(name))

  override def asNonNullable(): ScalarUserDefinedFunction = copy(nullable = false)

  override def asNondeterministic(): ScalarUserDefinedFunction = copy(deterministic = false)
}

object ScalarUserDefinedFunction {
  private[sql] def apply(
      function: AnyRef,
      returnType: TypeTag[_],
      parameterTypes: TypeTag[_]*): ScalarUserDefinedFunction = {

    ScalarUserDefinedFunction(
      function = function,
      inputEncoders = parameterTypes.map(tag => ScalaReflection.encoderFor(tag)),
      outputEncoder = ScalaReflection.encoderFor(returnType))
  }

  private[sql] def apply(
      function: AnyRef,
      inputEncoders: Seq[AgnosticEncoder[_]],
      outputEncoder: AgnosticEncoder[_]): ScalarUserDefinedFunction = {
    ScalarUserDefinedFunction(
      function = function,
      inputEncoders = inputEncoders,
      outputEncoder = outputEncoder,
      name = None,
      nullable = true,
      deterministic = true)
  }
}
