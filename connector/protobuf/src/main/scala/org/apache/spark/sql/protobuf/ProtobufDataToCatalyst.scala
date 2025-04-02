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
package org.apache.spark.sql.protobuf

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.google.protobuf.DynamicMessage
import com.google.protobuf.TypeRegistry

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.util.{FailFastMode, ParseMode, PermissiveMode}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.protobuf.utils.{ProtobufOptions, ProtobufUtils, SchemaConverters}
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}

private[sql] case class ProtobufDataToCatalyst(
    child: Expression,
    messageName: String,
    binaryFileDescriptorSet: Option[Array[Byte]] = None,
    options: Map[String, String] = Map.empty)
    extends UnaryExpression
    with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override lazy val dataType: DataType =
    SchemaConverters.toSqlType(messageDescriptor, protobufOptions).dataType

  override def nullable: Boolean = true

  private lazy val protobufOptions = ProtobufOptions(options)

  @transient private lazy val messageDescriptor =
    ProtobufUtils.buildDescriptor(messageName, binaryFileDescriptorSet)

  @transient private lazy val fieldsNumbers =
    messageDescriptor.getFields.asScala.map(f => f.getNumber).toSet

  @transient private lazy val deserializer = {
    val typeRegistry = binaryFileDescriptorSet match {
      case Some(descBytes) if protobufOptions.convertAnyFieldsToJson =>
        ProtobufUtils.buildTypeRegistry(descBytes) // This loads all the messages in the desc set.
      case None if protobufOptions.convertAnyFieldsToJson =>
        ProtobufUtils.buildTypeRegistry(messageDescriptor) // Loads only connected messages.
      case _ => TypeRegistry.getEmptyTypeRegistry // Default. Json conversion is not enabled.
    }
    new ProtobufDeserializer(
      messageDescriptor,
      dataType,
      typeRegistry = typeRegistry,
      emitDefaultValues = protobufOptions.emitDefaultValues,
      enumsAsInts = protobufOptions.enumsAsInts
    )
  }

  @transient private var result: DynamicMessage = _

  @transient private lazy val parseMode: ParseMode = {
    val mode = protobufOptions.parseMode
    if (mode != PermissiveMode && mode != FailFastMode) {
      throw QueryCompilationErrors.parseModeUnsupportedError(prettyName, mode)
    }
    mode
  }

  private def handleException(e: Throwable): Any = {
    parseMode match {
      case PermissiveMode => null
      case FailFastMode =>
        throw QueryExecutionErrors.malformedProtobufMessageDetectedInMessageParsingError(e)
      case _ =>
        throw QueryCompilationErrors.parseModeUnsupportedError(prettyName, parseMode)
    }
  }

  override def nullSafeEval(input: Any): Any = {
    val binary = input.asInstanceOf[Array[Byte]]
    try {
      result = DynamicMessage.parseFrom(messageDescriptor, binary)
      // If the Java class is available, it is likely more efficient to parse with it than using
      // DynamicMessage. Can consider it in the future if parsing overhead is noticeable.

      result.getUnknownFields.asMap().keySet().asScala.find(fieldsNumbers.contains(_)) match {
        case Some(number) =>
          // Unknown fields contain a field with same number as a known field. Must be due to
          // mismatch of schema between writer and reader here.
          throw QueryCompilationErrors.protobufFieldTypeMismatchError(
            messageDescriptor.getFields.get(number).toString)
        case None =>
      }

      val deserialized = deserializer.deserialize(result)
      assert(
        deserialized.isDefined,
        "Protobuf deserializer cannot return an empty result because filters are not pushed down")
      deserialized.get
    } catch {
      // There could be multiple possible exceptions here, e.g. java.io.IOException,
      // ProtoRuntimeException, ArrayIndexOutOfBoundsException, etc.
      // To make it simple, catch all the exceptions here.
      case NonFatal(e) =>
        handleException(e)
    }
  }

  override def prettyName: String = "from_protobuf"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    nullSafeCodeGen(
      ctx,
      ev,
      eval => {
        val result = ctx.freshName("result")
        val dt = CodeGenerator.boxedType(dataType)
        s"""
        $dt $result = ($dt) $expr.nullSafeEval($eval);
        if ($result == null) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = $result;
        }
      """
      })
  }

  override protected def withNewChildInternal(newChild: Expression): ProtobufDataToCatalyst =
    copy(child = newChild)

  override def equals(that: Any): Boolean = {
    that match {
      case that: ProtobufDataToCatalyst =>
        this.child == that.child &&
        this.messageName == that.messageName &&
        (
          (this.binaryFileDescriptorSet.isEmpty && that.binaryFileDescriptorSet.isEmpty) ||
          (
            this.binaryFileDescriptorSet.nonEmpty && that.binaryFileDescriptorSet.nonEmpty &&
            this.binaryFileDescriptorSet.get.sameElements(that.binaryFileDescriptorSet.get)
          )
        ) &&
        this.options == that.options
      case _ => false
    }
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    while (i < binaryFileDescriptorSet.map(_.length).getOrElse(0)) {
      result = prime * result + binaryFileDescriptorSet.get.apply(i).hashCode
      i += 1
    }
    result = prime * result + child.hashCode
    result = prime * result + messageName.hashCode
    result = prime * result + options.hashCode
    result
  }
}
