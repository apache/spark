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
package org.apache.spark.sql.execution.datasources.v2.ffi

import java.util.Locale

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.JsonNode

import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.types._

/** A function that the manifest of a native package lists in `functions`. */
sealed trait NativeFunctionEntry {
  def name: String
  def kind: String
}

/**
 * A function of a native package, of a kind that Spark supports. The library of the package
 * implements it with the C interface in `org/apache/spark/sql/datasource/spark_udf.h`.
 * {{{
 *   {
 *     "udfAbiVersion": 1,
 *     "functions": [
 *       {"name": "add_one", "kind": "scalar", "arguments": ["bigint"], "returns": "bigint"},
 *       {"name": "random_id", "kind": "scalar", "arguments": [], "returns": "string",
 *        "deterministic": false}
 *     ],
 *     "libraries": {"linux-x86_64": "linux-x86_64/libmy_functions.so"}
 *   }
 * }}}
 *
 * @param name the name of the function, which the library gets in `create`
 * @param kind the kind of the function, [[NativeFunctionSpec.SCALAR]] in version 1
 * @param arguments the types of the arguments, to which Spark casts the arguments of a call
 * @param returnType the type of the result, which is always nullable
 * @param deterministic whether the function returns the same result for the same arguments
 */
case class NativeFunctionSpec(
    name: String,
    kind: String,
    arguments: Seq[DataType],
    returnType: DataType,
    deterministic: Boolean) extends NativeFunctionEntry

/**
 * A function of a kind that this version of Spark does not know, which a later version of the
 * interface may add, or of a version of the interface that Spark does not support: only its name
 * and its kind are read, so that the rest of the package stays usable, and registering it fails.
 */
case class UnsupportedNativeFunction(name: String, kind: String) extends NativeFunctionEntry

object NativeFunctionSpec {
  /** The kind of a scalar function, SPARK_UDF_SCALAR in the header. */
  val SCALAR = "scalar"

  private val Kinds = Map(SCALAR -> NativeUdfLibrary.SCALAR)

  /** The value of the kind in `SparkUdfCreateInfo`. */
  def kindCode(kind: String): Int = Kinds(kind)

  def supportedKinds: Seq[String] = Kinds.keys.toSeq.sorted

  /**
   * Reads the version of the native function interface, `udfAbiVersion`, and the functions of a
   * manifest, or returns why they are invalid. A manifest without functions has no version: Spark
   * ignores `udfAbiVersion` then, as it ignores the fields that it does not know. Of a version
   * that Spark does not support, only the names and the kinds of the functions are read, since
   * the version may change the other fields: Spark reports the version when a session adds the
   * package or registers one of its functions.
   */
  private[ffi] def parse(
      root: JsonNode): Either[String, (Option[Int], Seq[NativeFunctionEntry])] = {
    val functions = root.get("functions")
    if (functions == null || functions.isNull) {
      return Right((None, Nil))
    }
    if (!functions.isArray) {
      return Left("The field 'functions' of the manifest must be an array of functions.")
    }
    if (functions.isEmpty) {
      return Right((None, Nil))
    }
    val version = root.get("udfAbiVersion")
    if (version == null || !version.isInt || version.asInt() < 1) {
      return Left("A manifest with functions must have a positive integer field " +
        "'udfAbiVersion'.")
    }
    val unsupportedVersion = version.asInt() > NativeUdfLibrary.ABI_VERSION
    val parsed = functions.elements().asScala.map(parseFunction(_, unsupportedVersion)).toSeq
    parsed.collectFirst { case Left(reason) => reason } match {
      case Some(reason) => Left(reason)
      case None =>
        val entries = parsed.collect { case Right(entry) => entry }
        // The names are looked up case-insensitively, as the names of data sources.
        entries.groupBy(_.name.toLowerCase(Locale.ROOT)).values.find(_.size > 1) match {
          case Some(duplicates) =>
            Left(s"The manifest lists the function ${duplicates.head.name} more than once.")
          case None => Right((Some(version.asInt()), entries))
        }
    }
  }

  private def parseFunction(
      function: JsonNode,
      unsupportedVersion: Boolean): Either[String, NativeFunctionEntry] = {
    if (!function.isObject) {
      return Left("Each function of the manifest must be a JSON object.")
    }
    val name = function.get("name")
    if (name == null || !name.isTextual || name.asText().isEmpty ||
        name.asText().contains('\u0000')) {
      return Left("Each function of the manifest must have a field 'name' with a non-empty name " +
        "without the character U+0000.")
    }
    val functionName = name.asText()
    def invalid(reason: String) = Left(s"The function $functionName of the manifest $reason")
    val kind = function.get("kind")
    if (kind == null || !kind.isTextual) {
      return invalid("must have a field 'kind'.")
    }
    if (unsupportedVersion || !Kinds.contains(kind.asText())) {
      return Right(UnsupportedNativeFunction(functionName, kind.asText()))
    }
    val arguments = function.get("arguments")
    if (arguments == null || !arguments.isArray ||
        !arguments.elements().asScala.forall(_.isTextual)) {
      return invalid("must have a field 'arguments' with an array of the types of its arguments.")
    }
    val returns = function.get("returns")
    if (returns == null || !returns.isTextual) {
      return invalid("must have a field 'returns' with the type of its result.")
    }
    val deterministic = function.get("deterministic")
    if (deterministic != null && !deterministic.isBoolean) {
      return invalid("must have a boolean in its field 'deterministic'.")
    }
    val types = arguments.elements().asScala.zipWithIndex.map { case (ddl, i) =>
      parseType(ddl.asText(), s"the type of its argument ${i + 1}")
    }.toSeq :+ parseType(returns.asText(), "the type of its result")
    types.collectFirst { case Left(reason) => reason } match {
      case Some(reason) => invalid(reason)
      case None =>
        val parsed = types.collect { case Right(dataType) => dataType }
        Right(NativeFunctionSpec(functionName, kind.asText(), parsed.init, parsed.last,
          Option(deterministic).forall(_.asBoolean())))
    }
  }

  /** Parses `what`, a type of the manifest, and checks that it is a type of version 1. */
  private def parseType(ddl: String, what: String): Either[String, DataType] = {
    val dataType = try {
      CatalystSqlParser.parseDataType(ddl)
    } catch {
      case e: ParseException => return Left(s"has $ddl as $what, which is not a valid type: " +
        e.getMessage)
    }
    unsupportedType(dataType) match {
      case Some(unsupported) =>
        Left(s"has ${dataType.sql} as $what, which contains the type ${unsupported.sql}: " +
          "native functions do not support it.")
      case None if hasNulInFieldName(dataType) =>
        Left(s"has ${NativeArrow.printable(dataType.sql)} as $what, with a field whose name " +
          "contains the character U+0000, which Spark cannot pass to a library.")
      case None => Right(dataType)
    }
  }

  /**
   * The first type in `dataType` that is not a type of version 1 of the interface, which
   * `spark_udf.h` lists with their Arrow types. A string of a fixed or a maximum length is not
   * one: Arrow does not carry the length.
   */
  private def unsupportedType(dataType: DataType): Option[DataType] = dataType match {
    case _: CharType | _: VarcharType => Some(dataType)
    case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
         _: DecimalType | _: StringType | BinaryType | DateType | TimestampType |
         TimestampNTZType => None
    case ArrayType(elementType, _) => unsupportedType(elementType)
    case MapType(keyType, valueType, _) =>
      unsupportedType(keyType).orElse(unsupportedType(valueType))
    case StructType(fields) =>
      fields.iterator.flatMap(f => unsupportedType(f.dataType)).nextOption()
    case other => Some(other)
  }

  private def hasNulInFieldName(dataType: DataType): Boolean = dataType.existsRecursively {
    case StructType(fields) => fields.exists(_.name.contains('\u0000'))
    case _ => false
  }
}
