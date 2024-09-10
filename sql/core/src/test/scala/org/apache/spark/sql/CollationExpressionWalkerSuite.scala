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

import java.sql.Timestamp

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.variant.ParseJson
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.internal.types._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 *  This suite is introduced in order to test a bulk of expressions and functionalities related to
 *  collations
 */
class CollationExpressionWalkerSuite extends SparkFunSuite with SharedSparkSession {

  // Trait to distinguish different cases for generation
  sealed trait CollationType

  case object Utf8Binary extends CollationType

  case object Utf8Lcase extends CollationType

  /**
   * Helper function to generate all necessary parameters
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
   * Helper function to generate single entry of data as a string.
   * @param inputEntry - Single input entry that requires generation
   * @param collationType - Flag defining collation type to use
   * @return
   */
  def generateDataAsStrings(
      inputEntry: Seq[AbstractDataType],
      collationType: CollationType): Seq[Any] = {
    inputEntry.map(generateInputAsString(_, collationType))
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
      case e: Class[_] if e.isAssignableFrom(classOf[Expression]) =>
        generateLiterals(StringTypeAnyCollation, collationType)
      case se: Class[_] if se.isAssignableFrom(classOf[Seq[Expression]]) =>
        CreateArray(Seq(generateLiterals(StringTypeAnyCollation, collationType),
          generateLiterals(StringTypeAnyCollation, collationType)))
      case oe: Class[_] if oe.isAssignableFrom(classOf[Option[Any]]) => None
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
      case AnyTimestampType => Literal(Timestamp.valueOf("2009-07-30 12:58:59"))
      case BinaryType => collationType match {
        case Utf8Binary =>
          Literal.create("dummy string".getBytes)
        case Utf8Lcase =>
          Literal.create("DuMmY sTrInG".getBytes)
      }
      case BooleanType => Literal(true)
      case _: DatetimeType => Literal(Timestamp.valueOf("2009-07-30 12:58:59"))
      case _: DecimalType => Literal((new Decimal).set(5))
      case _: DoubleType => Literal(5.0)
      case IntegerType | NumericType | IntegralType => Literal(5)
      case LongType => Literal(5L)
      case NullType => Literal(null)
      case _: StringType | AnyDataType | _: AbstractStringType =>
        collationType match {
          case Utf8Binary =>
            Literal.create("dummy string", StringType("UTF8_BINARY"))
          case Utf8Lcase =>
            Literal.create("DuMmY sTrInG", StringType("UTF8_LCASE"))
        }
      case VariantType => collationType match {
        case Utf8Binary =>
          ParseJson(Literal.create("{}", StringType("UTF8_BINARY")))
        case Utf8Lcase =>
          ParseJson(Literal.create("{}", StringType("UTF8_LCASE")))
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
        CreateNamedStruct(
          Seq(Literal("start"), generateLiterals(StringTypeAnyCollation, collationType),
          Literal("end"), generateLiterals(StringTypeAnyCollation, collationType)))
    }

  /**
   * Helper function to generate single input as a string from the given type.
   *
   * @param inputType     - Single input type that requires generation
   * @param collationType - Flag defining collation type to use
   * @return
   */
  def generateInputAsString(
      inputType: AbstractDataType,
      collationType: CollationType): String =
    inputType match {
      // TODO: Try to make this a bit more random.
      case AnyTimestampType => "TIMESTAMP'2009-07-30 12:58:59'"
      case BinaryType =>
        collationType match {
          case Utf8Binary => "Cast('dummy string' collate utf8_binary as BINARY)"
          case Utf8Lcase => "Cast('DuMmY sTrInG' collate utf8_lcase as BINARY)"
        }
      case BooleanType => "True"
      case _: DatetimeType => "date'2016-04-08'"
      case _: DecimalType => "5.0"
      case _: DoubleType => "5.0"
      case IntegerType | NumericType | IntegralType => "5"
      case LongType => "5L"
      case _: StringType | AnyDataType | _: AbstractStringType =>
        collationType match {
          case Utf8Binary => "'dummy string' COLLATE UTF8_BINARY"
          case Utf8Lcase => "'DuMmY sTrInG' COLLATE UTF8_LCASE"
        }
      case NullType => "null"
      case VariantType => s"parse_json('{}')"
      case TypeCollection(typeCollection) =>
        val strTypes = typeCollection.filter(hasStringType)
        if (strTypes.isEmpty) {
          // Take first type
          generateInputAsString(typeCollection.head, collationType)
        } else {
          // Take first string type
          generateInputAsString(strTypes.head, collationType)
        }
      case AbstractArrayType(elementType) =>
        "array(" + generateInputAsString(elementType, collationType) + ")"
      case ArrayType(elementType, _) =>
        "array(" + generateInputAsString(elementType, collationType) + ")"
      case ArrayType =>
        "array(" + generateInputAsString(StringTypeAnyCollation, collationType) + ")"
      case MapType =>
        "map(" + generateInputAsString(StringTypeAnyCollation, collationType) + ", " +
          generateInputAsString(StringTypeAnyCollation, collationType) + ")"
      case MapType(keyType, valueType, _) =>
        "map(" + generateInputAsString(keyType, collationType) + ", " +
          generateInputAsString(valueType, collationType) + ")"
      case StructType =>
        "named_struct( 'start', " + generateInputAsString(StringTypeAnyCollation, collationType) +
          ", 'end', " + generateInputAsString(StringTypeAnyCollation, collationType) + ")"
      case StructType(fields) =>
        "named_struct(" + fields.map(f => "'" + f.name + "', " +
          generateInputAsString(f.dataType, collationType)).mkString(", ") + ")"
    }

  /**
   * Helper function to generate single input type as string from the given type.
   *
   * @param inputType     - Single input type that requires generation
   * @param collationType - Flag defining collation type to use
   * @return
   */
  def generateInputTypeAsStrings(
      inputType: AbstractDataType,
      collationType: CollationType): String =
    inputType match {
      case AnyTimestampType => "TIMESTAMP"
      case BinaryType => "BINARY"
      case BooleanType => "BOOLEAN"
      case _: DatetimeType => "DATE"
      case _: DecimalType => "DECIMAL(2, 1)"
      case _: DoubleType => "DOUBLE"
      case IntegerType | NumericType | IntegralType => "INT"
      case LongType => "BIGINT"
      case _: StringType | AnyDataType | _: AbstractStringType =>
        collationType match {
          case Utf8Binary => "STRING"
          case Utf8Lcase => "STRING COLLATE UTF8_LCASE"
        }
      case VariantType => "VARIANT"
      case TypeCollection(typeCollection) =>
        val strTypes = typeCollection.filter(hasStringType)
        if (strTypes.isEmpty) {
          // Take first type
          generateInputTypeAsStrings(typeCollection.head, collationType)
        } else {
          // Take first string type
          generateInputTypeAsStrings(strTypes.head, collationType)
        }
      case AbstractArrayType(elementType) =>
        "array<" + generateInputTypeAsStrings(elementType, collationType) + ">"
      case ArrayType(elementType, _) =>
        "array<" + generateInputTypeAsStrings(elementType, collationType) + ">"
      case ArrayType =>
        "array<" + generateInputTypeAsStrings(StringTypeAnyCollation, collationType) + ">"
      case MapType =>
        "map<" + generateInputTypeAsStrings(StringTypeAnyCollation, collationType) + ", " +
          generateInputTypeAsStrings(StringTypeAnyCollation, collationType) + ">"
      case MapType(keyType, valueType, _) =>
        "map<" + generateInputTypeAsStrings(keyType, collationType) + ", " +
          generateInputTypeAsStrings(valueType, collationType) + ">"
      case StructType =>
        "struct<start:" + generateInputTypeAsStrings(StringTypeAnyCollation, collationType) +
          ", end:" +
          generateInputTypeAsStrings(StringTypeAnyCollation, collationType) + ">"
      case StructType(fields) =>
        "named_struct<" + fields.map(f => "'" + f.name + "', " +
          generateInputTypeAsStrings(f.dataType, collationType)).mkString(", ") + ">"
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

  /**
   * Helper function to replace expected parameters with expected input types.
   * @param inputTypes - Input types generated by ExpectsInputType.inputTypes
   * @param params - Parameters that are read from expression info
   * @return
   */
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

  /**
   * Helper method to extract relevant expressions that can be walked over.
   * @return
   */
  def extractRelevantExpressions(): (Array[ExpressionInfo], List[String]) = {
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
      val headConstructor = cl.getConstructors
        .zip(cl.getConstructors.map(c => c.getParameters.length)).minBy(a => a._2)._1

      val params = headConstructor.getParameters.map(p => p.getType)

      val args = generateData(params.toSeq, Utf8Binary)
      // Find all expressions that have string as input
      try {
        val expr = headConstructor.newInstance(args: _*)
        expr match {
          case expTypes: ExpectsInputTypes =>
            expectsExpressionCounter = expectsExpressionCounter + 1
            val inputTypes = expTypes.inputTypes
            inputTypes.exists(it => hasStringType(it)) ||
              (inputTypes.nonEmpty && hasStringType(expTypes.dataType))
        }
      } catch {
        case _: Throwable => false
      }
    }).toArray

    val toSkip = List(
      "parse_url", // Parse URL cannot be generalized with ExpectInputTypes
      "collation", // Expected to return different collation names
      // Different inputs affect conversion
      "hex",
      "md5",
      "sha1",
      "unbase64",
      "base64",
      "sha2",
      "sha",
      "crc32",
      "ascii"
    )

    logInfo("Total number of expression: " + expressionCounter)
    logInfo("Total number of expression that expect input: " + expectsExpressionCounter)
    logInfo("Number of extracted expressions of relevance: " + (funInfos.length - toSkip.length))

    (funInfos, toSkip)
  }

  /**
   * Helper function to generate string of an expression suitable for execution.
   * @param expr - Expression that needs to be converted
   * @param collationType - Defines explicit collation to use
   * @return
   */
  def transformExpressionToString(expr: ExpectsInputTypes, collationType: CollationType): String = {
    if (expr.isInstanceOf[BinaryOperator]) {
      "col0 " + expr.asInstanceOf[BinaryOperator].symbol + " col1"
    } else {
      if (expr.inputTypes.size == 1) {
        expr.prettyName + "(col0)"
      }
      else {
        expr.prettyName + "(col0, " +
          expr.inputTypes.tail.map(generateInputAsString(_, collationType)).mkString(", ") + ")"
      }
    }
  }

  /**
   * Helper function to generate input data for the dataframe.
   * @param inputTypes - Column types that need to be generated
   * @param collationType - Defines explicit collation to use
   * @return
   */
  def generateTableData(
      inputTypes: Seq[AbstractDataType],
      collationType: CollationType): DataFrame = {
    val tblName = collationType match {
      case Utf8Binary => "tbl"
      case Utf8Lcase => "tbl_lcase"
    }

    sql(s"CREATE TABLE $tblName (" +
      inputTypes.zipWithIndex
        .map(it => "col" +
          it._2.toString + " " +
          generateInputTypeAsStrings(it._1, collationType)).mkString(", ") +
      ") USING PARQUET")

    sql(s"INSERT INTO $tblName VALUES (" +
      inputTypes.map(generateInputAsString(_, collationType)).mkString(", ") +
      ")")

    sql(s"SELECT * FROM $tblName")
  }

  /**
   * This test does following:
   * 1) Extract relevant expressions
   * 2) Run evaluation on expressions with different inputs
   * 3) Check if both expressions throw an exception
   * 4) If no exception, check if the result is the same
   * 5) Otherwise, check if exceptions are the same
   */
  test("SPARK-48280: Expression Walker for expression evaluation") {
    val (funInfos, toSkip) = extractRelevantExpressions()

    for (f <- funInfos.filter(f => !toSkip.contains(f.getName))) {
      val cl = Utils.classForName(f.getClassName)
      val headConstructor = cl.getConstructors
        .zip(cl.getConstructors.map(c => c.getParameters.length)).minBy(a => a._2)._1
      val params = headConstructor.getParameters.map(p => p.getType)
      val args = generateData(params.toSeq, Utf8Binary)
      val expr = headConstructor.newInstance(args: _*).asInstanceOf[ExpectsInputTypes]
      val inputTypes = expr.inputTypes

      val inputDataUtf8Binary =
        generateData(
          replaceExpressions(inputTypes, headConstructor.getParameters.map(p => p.getType).toSeq),
          Utf8Binary
        )
      val instanceUtf8Binary =
        headConstructor.newInstance(inputDataUtf8Binary: _*).asInstanceOf[Expression]
      val inputDataLcase =
        generateData(
          replaceExpressions(inputTypes, headConstructor.getParameters.map(p => p.getType).toSeq),
          Utf8Lcase
        )
      val instanceLcase = headConstructor.newInstance(inputDataLcase: _*).asInstanceOf[Expression]

      val exceptionUtfBinary = {
        try {
          scala.util.Right(instanceUtf8Binary match {
            case replaceable: RuntimeReplaceable =>
              replaceable.replacement.eval(EmptyRow)
            case _ =>
              instanceUtf8Binary.eval(EmptyRow)
          })
        } catch {
          case e: Throwable => scala.util.Left(e)
        }
      }

      val exceptionLcase = {
        try {
          scala.util.Right(instanceLcase match {
            case replaceable: RuntimeReplaceable =>
              replaceable.replacement.eval(EmptyRow)
            case _ =>
              instanceLcase.eval(EmptyRow)
          })
        } catch {
          case e: Throwable => scala.util.Left(e)
        }
      }

      // Check that both cases either throw or pass
      assert(exceptionUtfBinary.isRight == exceptionLcase.isRight)

      if (exceptionUtfBinary.isRight) {
        val resUtf8Binary = exceptionUtfBinary.getOrElse(null)
        val resUtf8Lcase = exceptionLcase.getOrElse(null)

        val dt = instanceLcase.dataType

        dt match {
          case st if resUtf8Lcase != null && resUtf8Lcase != null && hasStringType(st) =>
            // scalastyle:off caselocale
            assert(resUtf8Binary.toString.toLowerCase === resUtf8Lcase.toString.toLowerCase)
            // scalastyle:on caselocale
          case _ =>
            assert(resUtf8Lcase === resUtf8Binary)
        }
      }
      else {
        assert(exceptionUtfBinary.getOrElse(new Exception()).getClass
          == exceptionLcase.getOrElse(new Exception()).getClass)
      }
    }
  }

  /**
   * This test does following:
   * 1) Extract relevant expressions
   * 2) Run dataframe select on expressions with different inputs
   * 3) Check if both expressions throw an exception
   * 4) If no exception, check if the result is the same
   * 5) Otherwise, check if exceptions are the same
   */
  test("SPARK-48280: Expression Walker for codeGen generation") {
    val (funInfos, toSkip) = extractRelevantExpressions()

    for (f <- funInfos.filter(f => !toSkip.contains(f.getName))) {
      val cl = Utils.classForName(f.getClassName)
      val headConstructor = cl.getConstructors
        .zip(cl.getConstructors.map(c => c.getParameters.length)).minBy(a => a._2)._1
      val params = headConstructor.getParameters.map(p => p.getType)
      val args = generateData(params.toSeq, Utf8Binary)
      val expr = headConstructor.newInstance(args: _*).asInstanceOf[ExpectsInputTypes]

      withTable("tbl", "tbl_lcase") {

        val utf8_df = generateTableData(expr.inputTypes.take(2), Utf8Binary)
        val utf8_lcase_df = generateTableData(expr.inputTypes.take(2), Utf8Lcase)

        val utf8BinaryResult = try {
          val df = utf8_df.selectExpr(transformExpressionToString(expr, Utf8Binary))
          df.getRows(1, 0)
          scala.util.Right(df)
        } catch {
          case e: Throwable => scala.util.Left(e)
        }
        val utf8LcaseResult = try {
          val df = utf8_lcase_df.selectExpr(transformExpressionToString(expr, Utf8Lcase))
          df.getRows(1, 0)
          scala.util.Right(df)
        } catch {
          case e: Throwable => scala.util.Left(e)
        }

        assert(utf8BinaryResult.isLeft === utf8LcaseResult.isLeft)

        if (utf8BinaryResult.isRight) {
          val utf8BinaryResultChecked = utf8BinaryResult.getOrElse(null)
          val utf8LcaseResultChecked = utf8LcaseResult.getOrElse(null)

          val dt = utf8BinaryResultChecked.schema.fields.head.dataType

          dt match {
            case st if utf8BinaryResultChecked != null && utf8LcaseResultChecked != null &&
              hasStringType(st) =>
              // scalastyle:off caselocale
              assert(utf8BinaryResultChecked.getRows(1, 0).map(_.map(_.toLowerCase))(1) ===
                utf8LcaseResultChecked.getRows(1, 0).map(_.map(_.toLowerCase))(1))
              // scalastyle:on caselocale
            case _ =>
              assert(utf8BinaryResultChecked.getRows(1, 0)(1) ===
                utf8LcaseResultChecked.getRows(1, 0)(1))
          }
        }
        else {
          assert(utf8BinaryResult.getOrElse(new Exception()).getClass
            == utf8LcaseResult.getOrElse(new Exception()).getClass)
        }
      }
    }
  }

  /**
   * This test does following:
   * 1) Extract all expressions
   * 2) Run example queries for different session level default  collations
   * 3) Check if both expressions throw an exception
   * 4) If no exception, check if the result is the same
   * 5) Otherwise, check if exceptions are the same
   */
  test("SPARK-48280: Expression Walker for SQL query examples") {
    val funInfos = spark.sessionState.functionRegistry.listFunction().map { funcId =>
      spark.sessionState.catalog.lookupFunctionInfo(funcId)
    }

    // If expression is expected to return different results, it needs to be skipped
    val toSkip = List(
      // need to skip as these give timestamp/time related output
      "current_timestamp",
      "unix_timestamp",
      "localtimestamp",
      "now",
      // need to skip as plans differ in STRING <-> STRING COLLATE UTF8_LCASE
      "current_timezone",
      "schema_of_variant",
      // need to skip as result is expected to differ
      "collation",
      "contains",
      "aes_encrypt",
      "translate",
      "replace",
      "grouping",
      "grouping_id",
      "reflect",
      "try_reflect",
      "java_method",
      "hash",
      "xxhash64",
      // need to skip as these are random functions
      "rand",
      "random",
      "randn",
      "uuid",
      "shuffle",
      // other functions which are not yet supported
      "to_avro",
      "from_avro",
      "to_protobuf",
      "from_protobuf"
    )

    for (funInfo <- funInfos.filter(f => !toSkip.contains(f.getName))) {
      for (query <- "> .*;".r.findAllIn(funInfo.getExamples).map(s => s.substring(2))) {
        try {
          val resultUTF8 = sql(query)
          withSQLConf(SqlApiConf.DEFAULT_COLLATION -> "UTF8_LCASE") {
            val resultUTF8Lcase = sql(query)
            assert(resultUTF8.collect() === resultUTF8Lcase.collect())
          }
        } catch {
          case e: SparkRuntimeException => assert(e.getErrorClass == "USER_RAISED_EXCEPTION")
          case other: Throwable => throw other
        }
      }
    }
  }
}
