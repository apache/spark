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

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.catalyst.expressions._
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
      case AnyTimestampType => Literal("2009-07-30 12:58:59")
      case BinaryType => collationType match {
        case Utf8Binary =>
          Literal.create("dummy string".getBytes)
        case Utf8BinaryLcase =>
          Literal.create("DuMmY sTrInG".getBytes)
      }
      case BooleanType => Literal(true)
      case _: DatetimeType => Literal(0L)
      case _: DecimalType => Literal(new Decimal)
      case IntegerType | NumericType => Literal(0)
      case LongType => Literal(0L)
      case _: StringType | AnyDataType | _: AbstractStringType =>
        collationType match {
          case Utf8Binary =>
            Literal.create("dummy string", StringType("UTF8_BINARY"))
          case Utf8BinaryLcase =>
            Literal.create("DuMmY sTrInG", StringType("UTF8_LCASE"))
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

  def generateInputAsString(
      inputType: AbstractDataType,
      collationType: CollationType): String =
    inputType match {
      // TODO: Try to make this a bit more random.
      case AnyTimestampType => "TIMESTAMP'2009-07-30 12:58:59'"
      case BinaryType =>
        collationType match {
          case Utf8Binary => "Cast('dummy string' collate utf8_binary as BINARY)"
          case Utf8BinaryLcase => "Cast('DuMmY sTrInG' collate utf8_lcase as BINARY)"
        }
      case BooleanType => "True"
      case _: DatetimeType => "date'2016-04-08'"
      case _: DecimalType => "0.0"
      case IntegerType | NumericType => "0"
      case LongType => "0"
      case _: StringType | AnyDataType | _: AbstractStringType =>
        collationType match {
          case Utf8Binary => "'dummy string' COLLATE UTF8_BINARY"
          case Utf8BinaryLcase => "'DuMmY sTrInG' COLLATE UTF8_LCASE"
        }
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

  def generateInputTypeAsStrings(
      inputType: AbstractDataType,
      collationType: CollationType): String =
    inputType match {
      case AnyTimestampType => "TIMESTAMP"
      case BinaryType => "BINARY"
      case BooleanType => "BOOLEAN"
      case _: DatetimeType => "DATE"
      case _: DecimalType => "DECIMAL(2, 1)"
      case IntegerType | NumericType => "INT"
      case LongType => "BIGINT"
      case _: StringType | AnyDataType | _: AbstractStringType =>
        collationType match {
          case Utf8Binary => "STRING"
          case Utf8BinaryLcase => "STRING COLLATE UTF8_LCASE"
        }
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
            inputTypes.exists(it => hasStringType(it) || it.isInstanceOf[BinaryType])
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

  test("SPARK-48280: Expression Walker for expression evaluation") {
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

    val (funInfos, toSkip) = extractRelevantExpressions()

    for (f <- funInfos.filter(f => !toSkip.contains(f.getName))) {
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
          instanceUtf8Binary match {
            case replaceable: RuntimeReplaceable =>
              replaceable.replacement.eval(EmptyRow)
            case _ =>
              instanceUtf8Binary.eval(EmptyRow)
          }
          None
        } catch {
          case e: Throwable => Some(e)
        }
      }

      val exceptionLcase = {
        try {
          instanceLcase match {
            case replaceable: RuntimeReplaceable =>
              replaceable.replacement.eval(EmptyRow)
            case _ =>
              instanceLcase.eval(EmptyRow)
          }
          None
        } catch {
          case e: Throwable => Some(e)
        }
      }

      // Check that both cases either throw or pass
      assert(exceptionUtfBinary.isDefined == exceptionLcase.isDefined)

      if (exceptionUtfBinary.isEmpty) {
        val resUtf8Binary = instanceUtf8Binary match {
          case replaceable: RuntimeReplaceable =>
            replaceable.replacement.eval(EmptyRow)
          case _ =>
            instanceUtf8Binary.eval(EmptyRow)
        }
        val resUtf8Lcase = instanceLcase match {
          case replaceable: RuntimeReplaceable =>
            replaceable.replacement.eval(EmptyRow)
          case _ =>
            instanceLcase.eval(EmptyRow)
        }

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
        assert(exceptionUtfBinary.get.getClass == exceptionLcase.get.getClass)
      }
    }
  }

  test("SPARK-48280: Expression Walker for codeGen generation") {

    var (funInfos, toSkip) = extractRelevantExpressions()
    toSkip = toSkip ++ List(
      // Problem caught with other tests already
      "map_from_arrays",
      // These expressions are not called as functions
      "lead",
      "nth_value",
      "session_window",
      // Unexpected to fail
      "to_xml"
    )
    for (f <- funInfos.filter(f => !toSkip.contains(f.getName))) {
      val cl = Utils.classForName(f.getClassName)
      val headConstructor = cl.getConstructors.head
      val params = headConstructor.getParameters.map(p => p.getType)
      val args = generateData(params.toSeq, Utf8Binary)
      val expr = headConstructor.newInstance(args: _*).asInstanceOf[ExpectsInputTypes]
      val inputTypes = expr.inputTypes

      withTable("tbl", "tbl_lcase") {
        sql("CREATE TABLE tbl (" +
          inputTypes.zipWithIndex
            .map(it => "col" +
              it._2.toString + " " +
              generateInputTypeAsStrings(it._1, Utf8Binary)).mkString(", ") +
          ") USING PARQUET")
        sql("INSERT INTO tbl VALUES (" +
          inputTypes.map(generateInputAsString(_, Utf8Binary)).mkString(", ") +
          ")")

        sql("CREATE TABLE tbl_lcase (" +
          inputTypes.zipWithIndex
            .map(it => "col" +
              it._2.toString + " " +
              generateInputTypeAsStrings(it._1, Utf8BinaryLcase)).mkString(", ") +
          ") USING PARQUET")
        sql("INSERT INTO tbl_lcase VALUES (" +
          inputTypes.map(generateInputAsString(_, Utf8BinaryLcase)).mkString(", ") +
          ")")

        val utf8BinaryResult = try {
          if (expr.isInstanceOf[BinaryComparison]) {
            sql("SELECT " + "(col0 " + f.getName + "col1) FROM tbl")
          } else {
            if (inputTypes.size == 1) {
              sql("SELECT " + f.getName + "(col0) FROM tbl")
            }
            else {
              sql("SELECT " + f.getName + "(col0, " +
                inputTypes.tail.map(generateInputAsString(_, Utf8Binary)).mkString(", ") +
                ") FROM tbl")
            }
          }.getRows(1, 0)
          None
        } catch {
          case e: Throwable => Some(e)
        }
        val utf8BinaryLcaseResult = try {
          if (expr.isInstanceOf[BinaryComparison]) {
            sql("SELECT " + "(col0 " + f.getName + "col1) FROM tbl_lcase")
          } else {
            if (inputTypes.size == 1) {
              sql("SELECT " + f.getName + "(col0) FROM tbl_lcase")
            }
            else {
              sql("SELECT " + f.getName + "(col0, " +
                inputTypes.tail.map(generateInputAsString(_, Utf8BinaryLcase)).mkString(", ") +
                ") FROM tbl_lcase")
            }
          }.getRows(1, 0)
          None
        } catch {
          case e: Throwable => Some(e)
        }

        assert(utf8BinaryResult.isDefined === utf8BinaryLcaseResult.isDefined)

        if (utf8BinaryResult.isEmpty) {
          val utf8BinaryResult =
            if (expr.isInstanceOf[BinaryComparison]) {
              sql("SELECT " + "(col0 " + f.getName + "col1) FROM tbl")
            } else {
              if (inputTypes.size == 1) {
                sql("SELECT " + f.getName + "(col0) FROM tbl")
              }
              else {
                sql("SELECT " + f.getName + "(col0, " +
                  inputTypes.tail.map(generateInputAsString(_, Utf8Binary)).mkString(", ") +
                  ") FROM tbl")
              }
            }
          val utf8BinaryLcaseResult =
            if (expr.isInstanceOf[BinaryComparison]) {
              sql("SELECT " + "(col0 " + f.getName + "col1) FROM tbl_lcase")
            } else {
              if (inputTypes.size == 1) {
                sql("SELECT " + f.getName + "(col0) FROM tbl_lcase")
              }
              else {
                sql("SELECT " + f.getName + "(col0, " +
                  inputTypes.tail.map(generateInputAsString(_, Utf8BinaryLcase)).mkString(", ") +
                  ") FROM tbl_lcase")
              }
            }

          val dt = utf8BinaryResult.schema.fields.head.dataType

          dt match {
            case st if utf8BinaryResult != null && utf8BinaryLcaseResult != null &&
              hasStringType(st) =>
              // scalastyle:off caselocale
              assert(utf8BinaryResult.getRows(1, 0).map(_.map(_.toLowerCase)) ===
                utf8BinaryLcaseResult.getRows(1, 0).map(_.map(_.toLowerCase)))
              // scalastyle:on caselocale
            case _ =>
              // scalastyle:off caselocale
              assert(utf8BinaryResult.getRows(1, 0)(1) ===
              utf8BinaryLcaseResult.getRows(1, 0)(1))
              // scalastyle:on caselocale
          }
        }
        else {
          assert(utf8BinaryResult.get.getClass == utf8BinaryResult.get.getClass)
        }
      }
    }
  }

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
      // need to skip as plans differ in STRING <-> STRING COLLATE UTF8_BINARY_LCASE
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
      // need to skip as these are random functions
      "rand",
      "random",
      "randn",
      "uuid",
      "shuffle",
      // other functions which are not yet supported
      "date_sub",
      "date_add",
      "dateadd",
      "window",
      "window_time",
      "session_window",
      "reflect",
      "try_reflect",
      "levenshtein",
      "java_method"
    )

    for (funInfo <- funInfos.filter(f => !toSkip.contains(f.getName))) {
      println("checking - " + funInfo.getName)
      for (m <- "> .*;".r.findAllIn(funInfo.getExamples)) {
        try {
          val resultUTF8 = sql(m.substring(2))
          withSQLConf(SqlApiConf.DEFAULT_COLLATION -> "UTF8_BINARY_LCASE") {
            val resultUTF8Lcase = sql(m.substring(2))
            assert(resultUTF8.collect() === resultUTF8Lcase.collect())
          }
        }
        catch {
          case e: SparkRuntimeException => assert(e.getErrorClass == "USER_RAISED_EXCEPTION")
          case other: Throwable => other
        }
      }
    }
  }
}
