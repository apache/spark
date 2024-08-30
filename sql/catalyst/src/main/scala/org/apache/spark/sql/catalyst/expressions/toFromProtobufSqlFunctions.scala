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

package org.apache.spark.sql.catalyst.expressions

import java.io.File
import java.io.FileNotFoundException
import java.nio.file.NoSuchFileException

import scala.util.control.NonFatal

import org.apache.commons.io.FileUtils

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{MapType, NullType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

object ProtobufHelper {
  def readDescriptorFileContent(filePath: String): Array[Byte] = {
    try {
      FileUtils.readFileToByteArray(new File(filePath))
    } catch {
      case ex: FileNotFoundException =>
        throw new RuntimeException(s"Cannot find descriptor file at path: $filePath", ex)
      case ex: NoSuchFileException =>
        throw new RuntimeException(s"Cannot find descriptor file at path: $filePath", ex)
      case NonFatal(ex) =>
        throw new RuntimeException(s"Failed to read the descriptor file: $filePath", ex)
    }
  }
}

/**
 * Converts a binary column of Protobuf format into its corresponding catalyst value.
 * The Protobuf definition is provided through Protobuf <i>descriptor file</i>.
 *
 * @param data
 *   The Catalyst binary input column.
 * @param messageName
 *   The protobuf message name to look for in descriptor file.
 * @param descFilePath
 *   The Protobuf descriptor file. This file is usually created using `protoc` with
 *   `--descriptor_set_out` and `--include_imports` options.
 * @param options
 *   the options to use when performing the conversion.
 * @since 4.0.0
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(data, messageName, descFilePath, options) - Converts a binary Protobuf value into a Catalyst value.
    """,
  examples = """
    Examples:
      > SELECT _FUNC_(s, 'Person', '/path/to/descriptor.desc', map()) IS NULL AS result FROM (SELECT NAMED_STRUCT('name', name, 'id', id) AS s FROM VALUES ('John Doe', 1), (NULL,  2) tab(name, id));
       [false]
  """,
  note = """
    The specified Protobuf schema must match actual schema of the read data, otherwise the behavior
    is undefined: it may fail or return arbitrary result.
    To deserialize the data with a compatible and evolved schema, the expected Protobuf schema can be
    set via the corresponding option.
  """,
  group = "misc_funcs",
  since = "4.0.0"
)
// scalastyle:on line.size.limit
case class FromProtobuf(
    data: Expression,
    messageName: Expression,
    descFilePath: Expression,
    options: Expression) extends QuaternaryExpression with RuntimeReplaceable {
  override def first: Expression = data
  override def second: Expression = messageName
  override def third: Expression = descFilePath
  override def fourth: Expression = options

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression,
      newFourth: Expression): Expression = {
    copy(data = newFirst, messageName = newSecond, descFilePath = newThird, options = newFourth)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    val messageNameCheck = messageName.dataType match {
      case _: StringType if messageName.foldable => None
      case _ =>
        Some(TypeCheckResult.TypeCheckFailure(
          "The second argument of the FROM_PROTOBUF SQL function must be a constant string " +
            "representing the Protobuf message name"))
    }
    val descFilePathCheck = descFilePath.dataType match {
      case _: StringType if descFilePath.foldable => None
      case _ =>
        Some(TypeCheckResult.TypeCheckFailure(
          "The third argument of the FROM_PROTOBUF SQL function must be a constant string " +
            "representing the Protobuf descriptor file path"))
    }
    val optionsCheck = options.dataType match {
      case MapType(StringType, StringType, _) |
           MapType(NullType, NullType, _) |
           _: NullType if options.foldable => None
      case _ =>
        Some(TypeCheckResult.TypeCheckFailure(
          "The fourth argument of the FROM_PROTOBUF SQL function must be a constant map of " +
            "strings to strings containing the options to use for converting the value from " +
            "Protobuf format"))
    }
    messageNameCheck.getOrElse(
      descFilePathCheck.getOrElse(
        optionsCheck.getOrElse(TypeCheckResult.TypeCheckSuccess)
      )
    )
  }

  override lazy val replacement: Expression = {
    val messageNameValue: String = messageName.eval() match {
      case null =>
        throw new IllegalArgumentException("Message name cannot be null")
      case s: UTF8String =>
        s.toString
    }
    val descFilePathValue: Option[Array[Byte]] = descFilePath.eval() match {
      case s: UTF8String => Some(ProtobufHelper.readDescriptorFileContent(s.toString))
      case null => None
    }
    val optionsValue: Map[String, String] = options.eval() match {
      case a: ArrayBasedMapData if a.keyArray.array.nonEmpty =>
        val keys: Array[String] = a.keyArray.array.map(_.toString)
        val values: Array[String] = a.valueArray.array.map(_.toString)
        keys.zip(values).toMap
      case _ => Map.empty
    }
    val constructor = try {
      Utils.classForName(
        "org.apache.spark.sql.protobuf.ProtobufDataToCatalyst").getConstructors().head
    } catch {
      case _: java.lang.ClassNotFoundException =>
        throw QueryCompilationErrors.protobufNotLoadedSqlFunctionsUnusable(
          functionName = "FROM_PROTOBUF")
    }
    val expr = constructor.newInstance(data, messageNameValue, descFilePathValue, optionsValue)
    expr.asInstanceOf[Expression]
  }

  override def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("from_protobuf")
}

/**
 * Converts a Catalyst binary input value into its corresponding Protobuf format result.
 * This is a thin wrapper over the [[CatalystDataToProtobuf]] class to create a SQL function.
 *
 * @param data
 *   The Catalyst binary input column.
 * @param messageName
 *   The Protobuf message name.
 * @param descFilePath
 *   The Protobuf descriptor file path.
 * @param options
 *   The options to use when performing the conversion.
 * @since 4.0.0
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(child, messageName, descFilePath, options) - Converts a Catalyst binary input value into its corresponding
      Protobuf format result.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(s, 'Person', '/path/to/descriptor.desc', map('emitDefaultValues', 'true')) IS NULL FROM (SELECT NULL AS s);
       [true]
  """,
  group = "misc_funcs",
  since = "4.0.0"
)
// scalastyle:on line.size.limit
case class ToProtobuf(
    data: Expression,
    messageName: Expression,
    descFilePath: Expression,
    options: Expression) extends QuaternaryExpression with RuntimeReplaceable {
  override def first: Expression = data
  override def second: Expression = messageName
  override def third: Expression = descFilePath
  override def fourth: Expression = options

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression,
      newFourth: Expression): Expression = {
    copy(data = newFirst, messageName = newSecond, descFilePath = newThird, options = newFourth)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    val messageNameCheck = messageName.dataType match {
      case _: StringType if messageName.foldable => None
      case _ =>
        Some(TypeCheckResult.TypeCheckFailure(
          "The second argument of the TO_PROTOBUF SQL function must be a constant string " +
            "representing the Protobuf message name"))
    }
    val descFilePathCheck = descFilePath.dataType match {
      case _: StringType if descFilePath.foldable => None
      case _ =>
        Some(TypeCheckResult.TypeCheckFailure(
          "The third argument of the TO_PROTOBUF SQL function must be a constant string " +
            "representing the Protobuf descriptor file path"))
    }
    val optionsCheck = options.dataType match {
      case MapType(StringType, StringType, _) |
           MapType(NullType, NullType, _) |
           _: NullType if options.foldable => None
      case _ =>
        Some(TypeCheckResult.TypeCheckFailure(
          "The fourth argument of the TO_PROTOBUF SQL function must be a constant map of " +
            "strings to strings containing the options to use for converting the value to " +
            "Protobuf format"))
    }

    messageNameCheck.getOrElse(
      descFilePathCheck.getOrElse(
        optionsCheck.getOrElse(TypeCheckResult.TypeCheckSuccess)
      )
    )
  }

  override lazy val replacement: Expression = {
    val messageNameValue: String = messageName.eval() match {
      case null =>
        throw new IllegalArgumentException("Message name cannot be null")
      case s: UTF8String =>
        s.toString
    }
    val descFilePathValue: Option[Array[Byte]] = descFilePath.eval() match {
      case s: UTF8String => Some(ProtobufHelper.readDescriptorFileContent(s.toString))
      case null => None
    }
    val optionsValue: Map[String, String] = options.eval() match {
      case a: ArrayBasedMapData if a.keyArray.array.nonEmpty =>
        val keys: Array[String] = a.keyArray.array.map(_.toString)
        val values: Array[String] = a.valueArray.array.map(_.toString)
        keys.zip(values).toMap
      case _ => Map.empty
    }
    val constructor = try {
      Utils.classForName(
        "org.apache.spark.sql.protobuf.CatalystDataToProtobuf").getConstructors().head
    } catch {
      case _: java.lang.ClassNotFoundException =>
        throw QueryCompilationErrors.protobufNotLoadedSqlFunctionsUnusable(
          functionName = "TO_PROTOBUF")
    }
    val expr = constructor.newInstance(data, messageNameValue, descFilePathValue, optionsValue)
    expr.asInstanceOf[Expression]
  }

  override def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("to_protobuf")
}
