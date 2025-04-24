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

import java.time.ZoneOffset

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.{ArrayData, CharVarcharCodegenUtils, DateFormatter, FractionTimeFormatter, IntervalStringStyles, IntervalUtils, MapData, SparkStringUtils, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.IntervalStringStyles.ANSI_STYLE
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.BinaryOutputStyle
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.UTF8StringBuilder
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.ArrayImplicits._

trait ToStringBase { self: UnaryExpression with TimeZoneAwareExpression =>

  private lazy val dateFormatter = DateFormatter()
  private lazy val timeFormatter = new FractionTimeFormatter()
  private lazy val timestampFormatter = TimestampFormatter.getFractionFormatter(zoneId)
  private lazy val timestampNTZFormatter = TimestampFormatter.getFractionFormatter(ZoneOffset.UTC)

  // The brackets that are used in casting structs and maps to strings
  protected def leftBracket: String
  protected def rightBracket: String

  // The string value to use to represent null elements in array/struct/map.
  protected def nullString: String

  protected def useDecimalPlainString: Boolean

  protected val binaryFormatter: BinaryFormatter = UTF8String.fromBytes

  // Makes the function accept Any type input by doing `asInstanceOf[T]`.
  @inline private def acceptAny[T](func: T => UTF8String): Any => UTF8String =
    i => func(i.asInstanceOf[T])

  // Returns a function to convert a value to pretty string. The function assumes input is not null.
  protected final def castToString(
      from: DataType, to: StringConstraint = NoConstraint): Any => UTF8String =
    to match {
      case FixedLength(length) =>
        s => CharVarcharCodegenUtils.charTypeWriteSideCheck(castToString(from)(s), length)
      case MaxLength(length) =>
        s => CharVarcharCodegenUtils.varcharTypeWriteSideCheck(castToString(from)(s), length)
      case NoConstraint => castToString(from)
    }

  private def castToString(from: DataType): Any => UTF8String = from match {
    case CalendarIntervalType =>
      acceptAny[CalendarInterval](i => UTF8String.fromString(i.toString))
    case BinaryType => acceptAny[Array[Byte]](binaryFormatter.apply)
    case DateType =>
      acceptAny[Int](d => UTF8String.fromString(dateFormatter.format(d)))
    case TimestampType =>
      acceptAny[Long](t => UTF8String.fromString(timestampFormatter.format(t)))
    case TimestampNTZType =>
      acceptAny[Long](t => UTF8String.fromString(timestampNTZFormatter.format(t)))
    case _: TimeType =>
      acceptAny[Long](t => UTF8String.fromString(timeFormatter.format(t)))
    case ArrayType(et, _) =>
      acceptAny[ArrayData](array => {
        val builder = new UTF8StringBuilder
        builder.append("[")
        if (array.numElements() > 0) {
          val toUTF8String = castToString(et)
          if (array.isNullAt(0)) {
            if (nullString.nonEmpty) builder.append(nullString)
          } else {
            builder.append(toUTF8String(array.get(0, et)))
          }
          var i = 1
          while (i < array.numElements()) {
            builder.append(",")
            if (array.isNullAt(i)) {
              if (nullString.nonEmpty) builder.append(" " + nullString)
            } else {
              builder.append(" ")
              builder.append(toUTF8String(array.get(i, et)))
            }
            i += 1
          }
        }
        builder.append("]")
        builder.build()
      })
    case MapType(kt, vt, _) =>
      acceptAny[MapData](map => {
        val builder = new UTF8StringBuilder
        builder.append(leftBracket)
        if (map.numElements() > 0) {
          val keyArray = map.keyArray()
          val valueArray = map.valueArray()
          val keyToUTF8String = castToString(kt)
          val valueToUTF8String = castToString(vt)
          builder.append(keyToUTF8String(keyArray.get(0, kt)))
          builder.append(" ->")
          if (valueArray.isNullAt(0)) {
            if (nullString.nonEmpty) builder.append(" " + nullString)
          } else {
            builder.append(" ")
            builder.append(valueToUTF8String(valueArray.get(0, vt)))
          }
          var i = 1
          while (i < map.numElements()) {
            builder.append(", ")
            builder.append(keyToUTF8String(keyArray.get(i, kt)))
            builder.append(" ->")
            if (valueArray.isNullAt(i)) {
              if (nullString.nonEmpty) builder.append(" " + nullString)
            } else {
              builder.append(" ")
              builder.append(valueToUTF8String(valueArray.get(i, vt)))
            }
            i += 1
          }
        }
        builder.append(rightBracket)
        builder.build()
      })
    case StructType(fields) =>
      acceptAny[InternalRow](row => {
        val builder = new UTF8StringBuilder
        builder.append(leftBracket)
        if (row.numFields > 0) {
          val st = fields.map(_.dataType)
          val toUTF8StringFuncs = st.map(castToString)
          if (row.isNullAt(0)) {
            if (nullString.nonEmpty) builder.append(nullString)
          } else {
            builder.append(toUTF8StringFuncs(0)(row.get(0, st(0))))
          }
          var i = 1
          while (i < row.numFields) {
            builder.append(",")
            if (row.isNullAt(i)) {
              if (nullString.nonEmpty) builder.append(" " + nullString)
            } else {
              builder.append(" ")
              builder.append(toUTF8StringFuncs(i)(row.get(i, st(i))))
            }
            i += 1
          }
        }
        builder.append(rightBracket)
        builder.build()
      })
    case pudt: PythonUserDefinedType => castToString(pudt.sqlType)
    case udt: UserDefinedType[_] =>
      o => UTF8String.fromString(udt.deserialize(o).toString)
    case YearMonthIntervalType(startField, endField) =>
      acceptAny[Int](i => UTF8String.fromString(
        IntervalUtils.toYearMonthIntervalString(i, ANSI_STYLE, startField, endField)))
    case DayTimeIntervalType(startField, endField) =>
      acceptAny[Long](i => UTF8String.fromString(
        IntervalUtils.toDayTimeIntervalString(i, ANSI_STYLE, startField, endField)))
    case _: DecimalType if useDecimalPlainString =>
      acceptAny[Decimal](d => UTF8String.fromString(d.toPlainString))
    case _: StringType => acceptAny[UTF8String](identity[UTF8String])
    case _ => o => UTF8String.fromString(o.toString)
  }

  // Returns a function to generate code to convert a value to pretty string. It assumes the input
  // is not null.
  protected final def castToStringCode(
      from: DataType,
      ctx: CodegenContext,
      to: StringConstraint = NoConstraint): (ExprValue, ExprValue) => Block =
    (c, evPrim) => {
      val tmpVar = ctx.freshVariable("tmp", classOf[UTF8String])
      val castToString = castToStringCode(from, ctx)(c, tmpVar)
      val maintainConstraint = to match {
        case FixedLength(length) =>
          code"""$evPrim = org.apache.spark.sql.catalyst.util.CharVarcharCodegenUtils
                .charTypeWriteSideCheck($tmpVar, $length);""".stripMargin
        case MaxLength(length) =>
          code"""$evPrim = org.apache.spark.sql.catalyst.util.CharVarcharCodegenUtils
                .varcharTypeWriteSideCheck($tmpVar, $length);""".stripMargin
        case NoConstraint => code"$evPrim = $tmpVar;"
      }
      code"""
            UTF8String $tmpVar;
            $castToString
            $maintainConstraint
          """
    }

  @scala.annotation.tailrec
  private def castToStringCode(
      from: DataType, ctx: CodegenContext): (ExprValue, ExprValue) => Block = {
    from match {
      case BinaryType =>
        val bf = JavaCode.global(
          ctx.addReferenceObj("binaryFormatter", binaryFormatter),
          classOf[BinaryFormatter])
        (c, evPrim) => code"$evPrim = $bf.apply($c);"
      case DateType =>
        val df = JavaCode.global(
          ctx.addReferenceObj("dateFormatter", dateFormatter),
          dateFormatter.getClass)
        (c, evPrim) => code"$evPrim = UTF8String.fromString($df.format($c));"
      case TimestampType =>
        val tf = JavaCode.global(
          ctx.addReferenceObj("timestampFormatter", timestampFormatter),
          timestampFormatter.getClass)
        (c, evPrim) => code"$evPrim = UTF8String.fromString($tf.format($c));"
      case TimestampNTZType =>
        val tf = JavaCode.global(
          ctx.addReferenceObj("timestampNTZFormatter", timestampNTZFormatter),
          timestampNTZFormatter.getClass)
        (c, evPrim) => code"$evPrim = UTF8String.fromString($tf.format($c));"
      case _: TimeType =>
        val tf = JavaCode.global(
          ctx.addReferenceObj("timeFormatter", timeFormatter),
          timeFormatter.getClass)
        (c, evPrim) => code"$evPrim = UTF8String.fromString($tf.format($c));"
      case CalendarIntervalType =>
        (c, evPrim) => code"$evPrim = UTF8String.fromString($c.toString());"
      case ArrayType(et, _) =>
        (c, evPrim) => {
          val buffer = ctx.freshVariable("buffer", classOf[UTF8StringBuilder])
          val bufferClass = JavaCode.javaType(classOf[UTF8StringBuilder])
          val writeArrayElemCode = writeArrayToStringBuilder(et, c, buffer, ctx)
          code"""
             |$bufferClass $buffer = new $bufferClass();
             |$writeArrayElemCode;
             |$evPrim = $buffer.build();
           """.stripMargin
        }
      case MapType(kt, vt, _) =>
        (c, evPrim) => {
          val buffer = ctx.freshVariable("buffer", classOf[UTF8StringBuilder])
          val bufferClass = JavaCode.javaType(classOf[UTF8StringBuilder])
          val writeMapElemCode = writeMapToStringBuilder(kt, vt, c, buffer, ctx)
          code"""
             |$bufferClass $buffer = new $bufferClass();
             |$writeMapElemCode;
             |$evPrim = $buffer.build();
           """.stripMargin
        }
      case StructType(fields) =>
        (c, evPrim) => {
          val row = ctx.freshVariable("row", classOf[InternalRow])
          val buffer = ctx.freshVariable("buffer", classOf[UTF8StringBuilder])
          val bufferClass = JavaCode.javaType(classOf[UTF8StringBuilder])
          val writeStructCode =
            writeStructToStringBuilder(fields.map(_.dataType).toImmutableArraySeq, row, buffer, ctx)
          code"""
             |InternalRow $row = $c;
             |$bufferClass $buffer = new $bufferClass();
             |$writeStructCode
             |$evPrim = $buffer.build();
           """.stripMargin
        }
      case pudt: PythonUserDefinedType => castToStringCode(pudt.sqlType, ctx)
      case udt: UserDefinedType[_] =>
        val udtRef = JavaCode.global(ctx.addReferenceObj("udt", udt), udt.sqlType)
        (c, evPrim) =>
          code"$evPrim = UTF8String.fromString($udtRef.deserialize($c).toString());"
      case i: YearMonthIntervalType =>
        val iu = IntervalUtils.getClass.getName.stripSuffix("$")
        val iss = IntervalStringStyles.getClass.getName.stripSuffix("$")
        val style = s"$iss$$.MODULE$$.ANSI_STYLE()"
        (c, evPrim) =>
          // scalastyle:off line.size.limit
          code"$evPrim = UTF8String.fromString($iu.toYearMonthIntervalString($c, $style, (byte)${i.startField}, (byte)${i.endField}));"
          // scalastyle:on line.size.limit
      case i: DayTimeIntervalType =>
        val iu = IntervalUtils.getClass.getName.stripSuffix("$")
        val iss = IntervalStringStyles.getClass.getName.stripSuffix("$")
        val style = s"$iss$$.MODULE$$.ANSI_STYLE()"
        (c, evPrim) =>
          // scalastyle:off line.size.limit
          code"$evPrim = UTF8String.fromString($iu.toDayTimeIntervalString($c, $style, (byte)${i.startField}, (byte)${i.endField}));"
          // scalastyle:on line.size.limit
      // In ANSI mode, Spark always use plain string representation on casting Decimal values
      // as strings. Otherwise, the casting is using `BigDecimal.toString` which may use scientific
      // notation if an exponent is needed.
      case _: DecimalType if useDecimalPlainString =>
        (c, evPrim) => code"$evPrim = UTF8String.fromString($c.toPlainString());"
      case _: StringType =>
        (c, evPrim) => code"$evPrim = $c;"
      case _ =>
        (c, evPrim) => code"$evPrim = UTF8String.fromString(String.valueOf($c));"
    }
  }

  private def appendNull(buffer: ExprValue, isFirstElement: Boolean): Block = {
    if (nullString.isEmpty) {
      EmptyBlock
    } else if (isFirstElement) {
      code"""$buffer.append("$nullString");"""
    } else {
      code"""$buffer.append(" $nullString");"""
    }
  }

  private def writeArrayToStringBuilder(
      et: DataType,
      array: ExprValue,
      buffer: ExprValue,
      ctx: CodegenContext): Block = {
    val elementToStringCode = castToStringCode(et, ctx)
    val funcName = ctx.freshName("elementToString")
    val element = JavaCode.variable("element", et)
    val elementStr = JavaCode.variable("elementStr", StringType)
    val elementToStringFunc = inline"${ctx.addNewFunction(funcName,
      s"""
         |private UTF8String $funcName(${CodeGenerator.javaType(et)} $element) {
         |  UTF8String $elementStr = null;
         |  ${elementToStringCode(element, elementStr)}
         |  return elementStr;
         |}
       """.stripMargin)}"

    val loopIndex = ctx.freshVariable("loopIndex", IntegerType)
    code"""
       |$buffer.append("[");
       |if ($array.numElements() > 0) {
       |  if ($array.isNullAt(0)) {
       |    ${appendNull(buffer, isFirstElement = true)}
       |  } else {
       |    $buffer.append($elementToStringFunc(${CodeGenerator.getValue(array, et, "0")}));
       |  }
       |  for (int $loopIndex = 1; $loopIndex < $array.numElements(); $loopIndex++) {
       |    $buffer.append(",");
       |    if ($array.isNullAt($loopIndex)) {
       |      ${appendNull(buffer, isFirstElement = false)}
       |    } else {
       |      $buffer.append(" ");
       |      $buffer.append($elementToStringFunc(${CodeGenerator.getValue(array, et, loopIndex)}));
       |    }
       |  }
       |}
       |$buffer.append("]");
     """.stripMargin
  }

  private def writeMapToStringBuilder(
      kt: DataType,
      vt: DataType,
      map: ExprValue,
      buffer: ExprValue,
      ctx: CodegenContext): Block = {

    def dataToStringFunc(func: String, dataType: DataType) = {
      val funcName = ctx.freshName(func)
      val dataToStringCode = castToStringCode(dataType, ctx)
      val data = JavaCode.variable("data", dataType)
      val dataStr = JavaCode.variable("dataStr", StringType)
      val functionCall = ctx.addNewFunction(funcName,
        s"""
           |private UTF8String $funcName(${CodeGenerator.javaType(dataType)} $data) {
           |  UTF8String $dataStr = null;
           |  ${dataToStringCode(data, dataStr)}
           |  return dataStr;
           |}
         """.stripMargin)
      inline"$functionCall"
    }

    val keyToStringFunc = dataToStringFunc("keyToString", kt)
    val valueToStringFunc = dataToStringFunc("valueToString", vt)
    val loopIndex = ctx.freshVariable("loopIndex", IntegerType)
    val mapKeyArray = JavaCode.expression(s"$map.keyArray()", classOf[ArrayData])
    val mapValueArray = JavaCode.expression(s"$map.valueArray()", classOf[ArrayData])
    val getMapFirstKey = CodeGenerator.getValue(mapKeyArray, kt, JavaCode.literal("0", IntegerType))
    val getMapFirstValue = CodeGenerator.getValue(mapValueArray, vt,
      JavaCode.literal("0", IntegerType))
    val getMapKeyArray = CodeGenerator.getValue(mapKeyArray, kt, loopIndex)
    val getMapValueArray = CodeGenerator.getValue(mapValueArray, vt, loopIndex)
    code"""
       |$buffer.append("$leftBracket");
       |if ($map.numElements() > 0) {
       |  $buffer.append($keyToStringFunc($getMapFirstKey));
       |  $buffer.append(" ->");
       |  if ($map.valueArray().isNullAt(0)) {
       |    ${appendNull(buffer, isFirstElement = false)}
       |  } else {
       |    $buffer.append(" ");
       |    $buffer.append($valueToStringFunc($getMapFirstValue));
       |  }
       |  for (int $loopIndex = 1; $loopIndex < $map.numElements(); $loopIndex++) {
       |    $buffer.append(", ");
       |    $buffer.append($keyToStringFunc($getMapKeyArray));
       |    $buffer.append(" ->");
       |    if ($map.valueArray().isNullAt($loopIndex)) {
       |      ${appendNull(buffer, isFirstElement = false)}
       |    } else {
       |      $buffer.append(" ");
       |      $buffer.append($valueToStringFunc($getMapValueArray));
       |    }
       |  }
       |}
       |$buffer.append("$rightBracket");
     """.stripMargin
  }

  private def writeStructToStringBuilder(
      st: Seq[DataType],
      row: ExprValue,
      buffer: ExprValue,
      ctx: CodegenContext): Block = {
    val structToStringCode = st.zipWithIndex.map { case (ft, i) =>
      val fieldToStringCode = castToStringCode(ft, ctx)
      val field = ctx.freshVariable("field", ft)
      val fieldStr = ctx.freshVariable("fieldStr", StringType)
      val javaType = JavaCode.javaType(ft)
      code"""
         |${if (i != 0) code"""$buffer.append(",");""" else EmptyBlock}
         |if ($row.isNullAt($i)) {
         |  ${appendNull(buffer, isFirstElement = i == 0)}
         |} else {
         |  ${if (i != 0) code"""$buffer.append(" ");""" else EmptyBlock}
         |
         |  // Append $i field into the string buffer
         |  $javaType $field = ${CodeGenerator.getValue(row, ft, s"$i")};
         |  UTF8String $fieldStr = null;
         |  ${fieldToStringCode(field, fieldStr)}
         |  $buffer.append($fieldStr);
         |}
       """.stripMargin
    }

    val writeStructCode = ctx.splitExpressions(
      expressions = structToStringCode.map(_.code),
      funcName = "fieldToString",
      arguments = ("InternalRow", row.code) ::
        (classOf[UTF8StringBuilder].getName, buffer.code) :: Nil)

    code"""
       |$buffer.append("$leftBracket");
       |$writeStructCode
       |$buffer.append("$rightBracket");
     """.stripMargin
  }
}

object ToStringBase {
  def getBinaryFormatter: BinaryFormatter = {
    val style = SQLConf.get.getConf(SQLConf.BINARY_OUTPUT_STYLE)
    style match {
      case Some(BinaryOutputStyle.UTF8) =>
        (array: Array[Byte]) => UTF8String.fromBytes(array)
      case Some(BinaryOutputStyle.BASIC) =>
        (array: Array[Byte]) => UTF8String.fromString(array.mkString("[", ", ", "]"))
      case Some(BinaryOutputStyle.BASE64) =>
        (array: Array[Byte]) =>
          UTF8String.fromString(java.util.Base64.getEncoder.withoutPadding().encodeToString(array))
      case Some(BinaryOutputStyle.HEX) =>
        (array: Array[Byte]) => Hex.hex(array)
      case _ =>
        (array: Array[Byte]) => UTF8String.fromString(SparkStringUtils.getHexString(array))
    }
  }
}

trait BinaryFormatter extends (Array[Byte] => UTF8String) with Serializable

