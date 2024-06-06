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
import org.apache.spark.sql.catalyst.expressions.{EmptyRow, EvalMode, ExpectsInputTypes, Expression, GenericInternalRow, Literal}
import org.apache.spark.sql.internal.types.{AbstractArrayType, StringTypeAnyCollation, StringTypeBinaryLcase}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, AnyTimestampType, ArrayType, BinaryType, BooleanType, DataType, DatetimeType, Decimal, DecimalType, IntegerType, LongType, MapType, NumericType, StringType, StructType, TypeCollection}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 *  This suite is introduced in order to test a bulk of expressions and functionalities related to
 *  collations
 */
class CollationExpressionWalkerSuite extends SparkFunSuite with SharedSparkSession {

  // Trait to distinguish different cases for generation
  sealed trait CollationType

  case object Utf8Binary extends CollationType

  case object Utf8BinaryLcase extends CollationType

  /**
   * Helper function to generate all necesary parameters
   *
   * @param inputEntry - List of all input entries that need to be generated
   * @param collationType - Flag defining collation type to use
   * @return
   */
  def generateData(
      inputEntry: Seq[Any],
      collationType: CollationType): Seq[Any] = {
    inputEntry.map(generateSingleEntry(_, collationType))
  }

  /**
   * Helper function to generate single entry of data.
   * @param inputEntry - Single input entry that requires generation
   * @param collationType - Flag defining collation type to use
   * @return
   */
  def generateSingleEntry(
      inputEntry: Any,
      collationType: CollationType): Any =
    inputEntry match {
      case e: Class[_] if e.isAssignableFrom(classOf[Expression]) => Literal.create("1")
      case se: Class[_] if se.isAssignableFrom(classOf[Seq[Expression]]) =>
        Seq(Literal.create("1"), Literal.create("2"))
      case oe: Class[_] if oe.isAssignableFrom(classOf[Option[Expression]]) => None
      case b: Class[_] if b.isAssignableFrom(classOf[Boolean]) => false
      case dt: Class[_] if dt.isAssignableFrom(classOf[DataType]) => StringType
      case st: Class[_] if st.isAssignableFrom(classOf[StructType]) => StructType
      case em: Class[_] if em.isAssignableFrom(classOf[EvalMode.Value]) => EvalMode.LEGACY
      case m: Class[_] if m.isAssignableFrom(classOf[Map[_, _]]) => Map.empty
      case c: Class[_] if c.isAssignableFrom(classOf[Char]) => '\\'
      case i: Class[_] if i.isAssignableFrom(classOf[Int]) => 0
      case l: Class[_] if l.isAssignableFrom(classOf[Long]) => 0
      case adt: AbstractDataType => generateLiterals(adt, collationType)
      case Nil => Seq()
      case (head: AbstractDataType) :: rest => generateData(head :: rest, collationType)
    }

  /**
   * Helper function to generate single literal from the given type.
   *
   * @param inputType    - Single input literal type that requires generation
   * @param collationType - Flag defining collation type to use
   * @return
   */
  def generateLiterals(
      inputType: AbstractDataType,
      collationType: CollationType): Expression =
    inputType match {
      // TODO: Try to make this a bit more random.
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
          generateLiterals(typeCollection.head, collationType)
        } else {
          // Take first string type
          generateLiterals(strTypes.head, collationType)
        }
      case AbstractArrayType(elementType) =>
        generateLiterals(elementType, collationType).map(
          lit => Literal.create(Seq(lit.asInstanceOf[Literal].value), ArrayType(lit.dataType))
        ).head
      case ArrayType(elementType, _) =>
        generateLiterals(elementType, collationType).map(
          lit => Literal.create(Seq(lit.asInstanceOf[Literal].value), ArrayType(lit.dataType))
        ).head
      case ArrayType =>
        generateLiterals(StringTypeAnyCollation, collationType).map(
          lit => Literal.create(Seq(lit.asInstanceOf[Literal].value), ArrayType(lit.dataType))
        ).head
      case MapType =>
        val key = generateLiterals(StringTypeAnyCollation, collationType)
        val value = generateLiterals(StringTypeAnyCollation, collationType)
        Literal.create(Map(key -> value))
      case MapType(keyType, valueType, _) =>
        val key = generateLiterals(keyType, collationType)
        val value = generateLiterals(valueType, collationType)
        Literal.create(Map(key -> value))
      case StructType =>
        Literal.create((generateLiterals(StringTypeAnyCollation, collationType),
          generateLiterals(StringTypeAnyCollation, collationType)))
      case StructType(fields) =>
        Literal.create(new GenericInternalRow(
          fields.map(f => generateLiterals(f.dataType, collationType).asInstanceOf[Any])),
          StructType(fields))
    }

  /**
   * Helper function to extract types of relevance
   * @param inputType
   * @return
   */
  def hasStringType(inputType: AbstractDataType): Boolean = {
    inputType match {
      case _: StringType | StringTypeAnyCollation | StringTypeBinaryLcase | AnyDataType =>
        true
      case ArrayType => true
      case MapType => true
      case MapType(keyType, valueType, _) => hasStringType(keyType) || hasStringType(valueType)
      case ArrayType(elementType, _) => hasStringType(elementType)
      case AbstractArrayType(elementType) => hasStringType(elementType)
      case TypeCollection(typeCollection) =>
        typeCollection.exists(hasStringType)
      case StructType => true
      case StructType(fields) => fields.exists(sf => hasStringType(sf.dataType))
      case _ => false
    }
  }

  def replaceExpressions(inputTypes: Seq[AbstractDataType], params: Seq[Class[_]]): Seq[Any] = {
    (inputTypes, params) match {
      case (Nil, mparams) => mparams
      case (_, Nil) => Nil
      case (minputTypes, mparams) if mparams.head.isAssignableFrom(classOf[Expression]) =>
        minputTypes.head +: replaceExpressions(inputTypes.tail, mparams.tail)
      case (minputTypes, mparams) =>
        mparams.head +: replaceExpressions(minputTypes.tail, mparams.tail)
    }
  }

  test("SPARK-48280: Expression Walker for Test") {
    // This test does following:
    // 1) Take all expressions
    // 2) Find the ones that have at least one argument of StringType
    // 3) Use reflection to create an instance of the expression using first constructor
    //    (test other as well).
    // 4) Check if the expression is of type ExpectsInputTypes (should make this a bit broader)
    // 5) Run eval against literals with strings under:
    //    a) UTF8_BINARY, "dummy string" as input.
    //    b) UTF8_BINARY_LCASE, "DuMmY sTrInG" as input.
    // 6) Check if both expressions throw an exception.
    // 7) If no exception, check if the result is the same.
    // 8) There is a list of allowed expressions that can differ (e.g. hex)
    var expressionCounter = 0
    var expectsExpressionCounter = 0;
    val funInfos = spark.sessionState.functionRegistry.listFunction().map { funcId =>
      spark.sessionState.catalog.lookupFunctionInfo(funcId)
    }.filter(funInfo => {
      // make sure that there is a constructor.
      val cl = Utils.classForName(funInfo.getClassName)
      !cl.getConstructors.isEmpty
    }).filter(funInfo => {
      expressionCounter = expressionCounter + 1
      val cl = Utils.classForName(funInfo.getClassName)
      // dummy instance
      // Take first constructor.
      val headConstructor = cl.getConstructors.head

      val params = headConstructor.getParameters.map(p => p.getType)

      val args = generateData(params.toSeq, Utf8Binary)
      // Find all expressions that have string as input
      try {
        val expr = headConstructor.newInstance(args: _*)
        expr match {
          case types: ExpectsInputTypes =>
            expectsExpressionCounter = expectsExpressionCounter + 1
            val inputTypes = types.inputTypes
            inputTypes.exists(hasStringType)
        }
      } catch {
        case _: Throwable => false
      }
    }).toArray

    val toSkip = List(
      "parse_url", // Parse URL is using wrong concepts, not related to ExpectsInputTypes
      "hex" // this is fine
    )
    // scalastyle:off println
    println("Total number of expression: " + expressionCounter)
    println("Total number of expression that expect input: " + expectsExpressionCounter)
    println("Number of extracted expressions of relevance: " + (funInfos.length - toSkip.length))
    // scalastyle:on println
    for (f <- funInfos.filter(f => !toSkip.contains(f.getName))) {
      // scalastyle:off println
      println(f.getName)
      val cl = Utils.classForName(f.getClassName)
      val headConstructor = cl.getConstructors.head
      val params = headConstructor.getParameters.map(p => p.getType)
      val args = generateData(params.toSeq, Utf8Binary)
      val expr = headConstructor.newInstance(args: _*).asInstanceOf[ExpectsInputTypes]
      val inputTypes = expr.inputTypes

      val inputDataUtf8Binary =
        generateData(replaceExpressions(inputTypes, params.toSeq), Utf8Binary)
      val instanceUtf8Binary =
        headConstructor.newInstance(inputDataUtf8Binary: _*).asInstanceOf[Expression]

      val inputDataLcase =
        generateData(replaceExpressions(inputTypes, params.toSeq), Utf8BinaryLcase)
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
