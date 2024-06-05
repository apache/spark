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

package org.apache.spark.sql

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{EmptyRow, ExpectsInputTypes, Expression, Literal}
import org.apache.spark.sql.internal.types.{AbstractArrayType, StringTypeAnyCollation, StringTypeBinaryLcase}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, AnyTimestampType, ArrayType, BinaryType, BooleanType, DatetimeType, Decimal, DecimalType, IntegerType, LongType, NumericType, StringType, TypeCollection}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

class CollationExpressionWalkerSuite extends SparkFunSuite with SharedSparkSession {
  test("SPARK-48280: Expression Walker for Testing") {
    // This test does following:
    // 1) Take all expressions
    // 2) Filter out ones that have at least one argument of StringType
    // 3) Use reflection to create an instance of the expression using first constructor
    //    (test other as well).
    // 4) Check if the expression is of type ExpectsInputTypes (should make this a bit broader)
    // 5) Run eval against literals with strings under:
    //    a) UTF8_BINARY, "dummy string" as input.
    //    b) UTF8_BINARY_LCASE, "DuMmY sTrInG" as input.
    // 6) Check if both expressions throw an exception.
    // 7) If no exception, check if the result is the same.
    // 8) There is a list of allowed expressions that can differ (e.g. hex)
    def hasStringType(inputType: AbstractDataType): Boolean = {
      inputType match {
        case _: StringType | StringTypeAnyCollation | StringTypeBinaryLcase | AnyDataType =>
          true
        case ArrayType => true
        case ArrayType(elementType, _) => hasStringType(elementType)
        case AbstractArrayType(elementType) => hasStringType(elementType)
        case TypeCollection(typeCollection) =>
          typeCollection.exists(hasStringType)
        case _ => false
      }
    }

    val funInfos = spark.sessionState.functionRegistry.listFunction().map { funcId =>
      spark.sessionState.catalog.lookupFunctionInfo(funcId)
    }.filter(funInfo => {
      // make sure that there is a constructor.
      val cl = Utils.classForName(funInfo.getClassName)
      !cl.getConstructors.isEmpty
    }).filter(funInfo => {
      val className = funInfo.getClassName
      val cl = Utils.classForName(funInfo.getClassName)
      // dummy instance
      // Take first constructor.
      val headConstructor = cl.getConstructors.head

      val params = headConstructor.getParameters.map(p => p.getType)
      val allExpressions = params.forall(p => p.isAssignableFrom(classOf[Expression]) ||
        p.isAssignableFrom(classOf[Seq[Expression]]) ||
        p.isAssignableFrom(classOf[Option[Expression]]))

      if (!allExpressions) {
        false
      } else {
        val args = params.map {
          case e if e.isAssignableFrom(classOf[Expression]) => Literal.create("1")
          case se if se.isAssignableFrom(classOf[Seq[Expression]]) =>
            Seq(Literal.create("1"), Literal.create("2"))
          case oe if oe.isAssignableFrom(classOf[Option[Expression]]) => None
        }
        // Find all expressions that have string as input
        try {
          val expr = headConstructor.newInstance(args: _*)
          expr match {
            case types: ExpectsInputTypes =>
              val inputTypes = types.inputTypes
              // check if this is a collection...
              inputTypes.exists(hasStringType)
          }
        } catch {
          case _: Throwable => false
        }
      }
    }).toArray

    // Helper methods for generating data.
    sealed trait CollationType
    case object Utf8Binary extends CollationType
    case object Utf8BinaryLcase extends CollationType

    def generateSingleEntry(
                             inputType: AbstractDataType,
                             collationType: CollationType): Expression =
      inputType match {
        // Try to make this a bit more random.
        case AnyTimestampType => Literal("2009-07-30 12:58:59")
        case BinaryType => Literal(new Array[Byte](5))
        case BooleanType => Literal(true)
        case _: DatetimeType => Literal(1L)
        case _: DecimalType => Literal(new Decimal)
        case IntegerType | NumericType => Literal(1)
        case LongType => Literal(1L)
        case _: StringType | StringTypeAnyCollation | StringTypeBinaryLcase | AnyDataType =>
          collationType match {
            case Utf8Binary =>
              Literal.create("dummy string", StringType("UTF8_BINARY"))
            case Utf8BinaryLcase =>
              Literal.create("DuMmY sTrInG", StringType("UTF8_BINARY_LCASE"))
          }
        case TypeCollection(typeCollection) =>
          val strTypes = typeCollection.filter(hasStringType)
          if (strTypes.isEmpty) {
            // Take first type
            generateSingleEntry(typeCollection.head, collationType)
          } else {
            // Take first string type
            generateSingleEntry(strTypes.head, collationType)
          }
        case AbstractArrayType(elementType) =>
          generateSingleEntry(elementType, collationType).map(
            lit => Literal.create(Seq(lit.asInstanceOf[Literal].value), ArrayType(lit.dataType))
          ).head
        case ArrayType(elementType, _) =>
          generateSingleEntry(elementType, collationType).map(
            lit => Literal.create(Seq(lit.asInstanceOf[Literal].value), ArrayType(lit.dataType))
          ).head
        case ArrayType =>
          generateSingleEntry(StringTypeAnyCollation, collationType).map(
            lit => Literal.create(Seq(lit.asInstanceOf[Literal].value), ArrayType(lit.dataType))
          ).head
      }

    def generateData(
                      inputTypes: Seq[AbstractDataType],
                      collationType: CollationType): Seq[Expression] = {
      inputTypes.map(generateSingleEntry(_, collationType))
    }

    val toSkip = List(
      "get_json_object",
      "map_zip_with",
      "printf",
      "transform_keys",
      "concat_ws",
      "format_string",
      "session_window",
      "transform_values",
      "arrays_zip",
      "hex" // this is fine
    )

    for (f <- funInfos.filter(f => !toSkip.contains(f.getName))) {
      val cl = Utils.classForName(f.getClassName)
      val headConstructor = cl.getConstructors.head
      val params = headConstructor.getParameters.map(p => p.getType)
      val paramCount = params.length
      val args = params.map {
        case e if e.isAssignableFrom(classOf[Expression]) => Literal.create("1")
        case se if se.isAssignableFrom(classOf[Seq[Expression]]) =>
          Seq(Literal.create("1"))
        case oe if oe.isAssignableFrom(classOf[Option[Expression]]) => None
      }
      val expr = headConstructor.newInstance(args: _*).asInstanceOf[ExpectsInputTypes]
      val inputTypes = expr.inputTypes

      var nones = Array.fill(0)(None)
      if (paramCount > inputTypes.length) {
        nones = Array.fill(paramCount - inputTypes.length)(None)
      }

      val inputDataUtf8Binary = generateData(inputTypes.take(paramCount), Utf8Binary) ++ nones
      val instanceUtf8Binary =
        headConstructor.newInstance(inputDataUtf8Binary: _*).asInstanceOf[Expression]

      val inputDataLcase = generateData(inputTypes.take(paramCount), Utf8BinaryLcase) ++ nones
      val instanceLcase = headConstructor.newInstance(inputDataLcase: _*).asInstanceOf[Expression]

      val exceptionUtfBinary = {
        try {
          instanceUtf8Binary.eval(EmptyRow)
          None
        } catch {
          case e: Throwable => Some(e)
        }
      }

      val exceptionLcase = {
        try {
          instanceLcase.eval(EmptyRow)
          None
        } catch {
          case e: Throwable => Some(e)
        }
      }

      // Check that both cases either throw or pass
      assert(exceptionUtfBinary.isDefined == exceptionLcase.isDefined)

      if (exceptionUtfBinary.isEmpty) {
        val resUtf8Binary = instanceUtf8Binary.eval(EmptyRow)
        val resUtf8Lcase = instanceLcase.eval(EmptyRow)

        val dt = instanceLcase.dataType

        dt match {
          case _: StringType if resUtf8Lcase != null && resUtf8Lcase != null =>
            assert(resUtf8Binary.isInstanceOf[UTF8String])
            assert(resUtf8Lcase.isInstanceOf[UTF8String])
            // scalastyle:off caselocale
            assert(resUtf8Binary.asInstanceOf[UTF8String].toLowerCase.binaryEquals(
              resUtf8Lcase.asInstanceOf[UTF8String].toLowerCase))
          // scalastyle:on caselocale
          case _ => resUtf8Lcase === resUtf8Binary
        }
      }
      else {
        assert(exceptionUtfBinary.get.getClass == exceptionLcase.get.getClass)
      }
    }
  }
}
