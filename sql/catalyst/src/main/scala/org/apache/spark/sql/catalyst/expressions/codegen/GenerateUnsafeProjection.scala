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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/**
 * Generates a [[Projection]] that returns an [[UnsafeRow]].
 *
 * It generates the code for all the expressions, compute the total length for all the columns
 * (can be accessed via variables), and then copy the data into a scratch buffer space in the
 * form of UnsafeRow (the scratch buffer will grow as needed).
 *
 * Note: The returned UnsafeRow will be pointed to a scratch buffer inside the projection.
 */
object GenerateUnsafeProjection extends CodeGenerator[Seq[Expression], UnsafeProjection] {

  private val StringWriter = classOf[UnsafeRowWriters.UTF8StringWriter].getName
  private val BinaryWriter = classOf[UnsafeRowWriters.BinaryWriter].getName
  private val IntervalWriter = classOf[UnsafeRowWriters.IntervalWriter].getName
  private val StructWriter = classOf[UnsafeRowWriters.StructWriter].getName
  private val CompactDecimalWriter = classOf[UnsafeRowWriters.CompactDecimalWriter].getName
  private val DecimalWriter = classOf[UnsafeRowWriters.DecimalWriter].getName
  private val ArrayWriter = classOf[UnsafeRowWriters.ArrayWriter].getName
  private val MapWriter = classOf[UnsafeRowWriters.MapWriter].getName

  /** Returns true iff we support this data type. */
  def canSupport(dataType: DataType): Boolean = dataType match {
    case NullType => true
    case t: AtomicType => true
    case _: CalendarIntervalType => true
    case t: StructType => t.toSeq.forall(field => canSupport(field.dataType))
    case t: ArrayType if canSupport(t.elementType) => true
    case MapType(kt, vt, _) if canSupport(kt) && canSupport(vt) => true
    case _ => false
  }

  def genAdditionalSize(dt: DataType, ev: GeneratedExpressionCode): String = dt match {
    case t: DecimalType if t.precision > Decimal.MAX_LONG_DIGITS =>
      s"$DecimalWriter.getSize(${ev.primitive})"
    case StringType =>
      s"${ev.isNull} ? 0 : $StringWriter.getSize(${ev.primitive})"
    case BinaryType =>
      s"${ev.isNull} ? 0 : $BinaryWriter.getSize(${ev.primitive})"
    case CalendarIntervalType =>
      s"${ev.isNull} ? 0 : 16"
    case _: StructType =>
      s"${ev.isNull} ? 0 : $StructWriter.getSize(${ev.primitive})"
    case _: ArrayType =>
      s"${ev.isNull} ? 0 : $ArrayWriter.getSize(${ev.primitive})"
    case _: MapType =>
      s"${ev.isNull} ? 0 : $MapWriter.getSize(${ev.primitive})"
    case _ => ""
  }

  def genFieldWriter(
      ctx: CodeGenContext,
      fieldType: DataType,
      ev: GeneratedExpressionCode,
      target: String,
      index: Int,
      cursor: String): String = fieldType match {
    case _ if ctx.isPrimitiveType(fieldType) =>
      s"${ctx.setColumn(target, fieldType, index, ev.primitive)}"
    case t: DecimalType if t.precision <= Decimal.MAX_LONG_DIGITS =>
      s"""
       // make sure Decimal object has the same scale as DecimalType
       if (${ev.primitive}.changePrecision(${t.precision}, ${t.scale})) {
         $CompactDecimalWriter.write($target, $index, $cursor, ${ev.primitive});
       } else {
         $target.setNullAt($index);
       }
       """
    case t: DecimalType if t.precision > Decimal.MAX_LONG_DIGITS =>
      s"""
       // make sure Decimal object has the same scale as DecimalType
       if (${ev.primitive}.changePrecision(${t.precision}, ${t.scale})) {
         $cursor += $DecimalWriter.write($target, $index, $cursor, ${ev.primitive});
       } else {
         $cursor += $DecimalWriter.write($target, $index, $cursor, null);
       }
       """
    case StringType =>
      s"$cursor += $StringWriter.write($target, $index, $cursor, ${ev.primitive})"
    case BinaryType =>
      s"$cursor += $BinaryWriter.write($target, $index, $cursor, ${ev.primitive})"
    case CalendarIntervalType =>
      s"$cursor += $IntervalWriter.write($target, $index, $cursor, ${ev.primitive})"
    case _: StructType =>
      s"$cursor += $StructWriter.write($target, $index, $cursor, ${ev.primitive})"
    case _: ArrayType =>
      s"$cursor += $ArrayWriter.write($target, $index, $cursor, ${ev.primitive})"
    case _: MapType =>
      s"$cursor += $MapWriter.write($target, $index, $cursor, ${ev.primitive})"
    case NullType => ""
    case _ =>
      throw new UnsupportedOperationException(s"Not supported DataType: $fieldType")
  }

  /**
   * Generates the Java code to convert a struct (backed by InternalRow) to UnsafeRow.
   *
   * @param ctx code generation context
   * @param inputs could be the codes for expressions or input struct fields.
   * @param inputTypes types of the inputs
   */
  private def createCodeForStruct(
      ctx: CodeGenContext,
      row: String,
      inputs: Seq[GeneratedExpressionCode],
      inputTypes: Seq[DataType]): GeneratedExpressionCode = {

    val fixedSize = 8 * inputTypes.length + UnsafeRow.calculateBitSetWidthInBytes(inputTypes.length)

    val output = ctx.freshName("convertedStruct")
    ctx.addMutableState("UnsafeRow", output, s"this.$output = new UnsafeRow();")
    val buffer = ctx.freshName("buffer")
    ctx.addMutableState("byte[]", buffer, s"this.$buffer = new byte[$fixedSize];")
    val cursor = ctx.freshName("cursor")
    ctx.addMutableState("int", cursor, s"this.$cursor = 0;")
    val tmp = ctx.freshName("tmpBuffer")

    val convertedFields = inputTypes.zip(inputs).zipWithIndex.map { case ((dt, input), i) =>
      val ev = createConvertCode(ctx, input, dt)
      val growBuffer = if (!UnsafeRow.isFixedLength(dt)) {
        val numBytes = ctx.freshName("numBytes")
        s"""
          int $numBytes = $cursor + (${genAdditionalSize(dt, ev)});
          if ($buffer.length < $numBytes) {
            // This will not happen frequently, because the buffer is re-used.
            byte[] $tmp = new byte[$numBytes * 2];
            Platform.copyMemory($buffer, Platform.BYTE_ARRAY_OFFSET,
              $tmp, Platform.BYTE_ARRAY_OFFSET, $buffer.length);
            $buffer = $tmp;
          }
          $output.pointTo($buffer, Platform.BYTE_ARRAY_OFFSET, ${inputTypes.length}, $numBytes);
         """
      } else {
        ""
      }
      val update = dt match {
        case dt: DecimalType if dt.precision > Decimal.MAX_LONG_DIGITS =>
          // Can't call setNullAt() for DecimalType
          s"""
          if (${ev.isNull}) {
            $cursor += $DecimalWriter.write($output, $i, $cursor, null);
          } else {
            ${genFieldWriter(ctx, dt, ev, output, i, cursor)};
          }
        """
        case _ =>
          s"""
          if (${ev.isNull}) {
            $output.setNullAt($i);
          } else {
            ${genFieldWriter(ctx, dt, ev, output, i, cursor)};
          }
        """
      }
      s"""
        ${ev.code}
        $growBuffer
        $update
      """
    }

    val code = s"""
      $cursor = $fixedSize;
      $output.pointTo($buffer, Platform.BYTE_ARRAY_OFFSET, ${inputTypes.length}, $cursor);
      ${ctx.splitExpressions(row, convertedFields)}
      """
    GeneratedExpressionCode(code, "false", output)
  }

  private def getWriter(dt: DataType) = dt match {
    case StringType => classOf[UnsafeWriters.UTF8StringWriter].getName
    case BinaryType => classOf[UnsafeWriters.BinaryWriter].getName
    case CalendarIntervalType => classOf[UnsafeWriters.IntervalWriter].getName
    case _: StructType => classOf[UnsafeWriters.StructWriter].getName
    case _: ArrayType => classOf[UnsafeWriters.ArrayWriter].getName
    case _: MapType => classOf[UnsafeWriters.MapWriter].getName
    case _: DecimalType => classOf[UnsafeWriters.DecimalWriter].getName
  }

  private def createCodeForArray(
      ctx: CodeGenContext,
      input: GeneratedExpressionCode,
      elementType: DataType): GeneratedExpressionCode = {
    val output = ctx.freshName("convertedArray")
    ctx.addMutableState("UnsafeArrayData", output, s"$output = new UnsafeArrayData();")
    val buffer = ctx.freshName("buffer")
    ctx.addMutableState("byte[]", buffer, s"$buffer = new byte[64];")
    val outputIsNull = ctx.freshName("isNull")
    val tmp = ctx.freshName("tmp")
    val numElements = ctx.freshName("numElements")
    val fixedSize = ctx.freshName("fixedSize")
    val numBytes = ctx.freshName("numBytes")
    val elements = ctx.freshName("elements")
    val cursor = ctx.freshName("cursor")
    val index = ctx.freshName("index")

    val element = GeneratedExpressionCode(
      code = "",
      isNull = s"$tmp.isNullAt($index)",
      primitive = s"${ctx.getValue(tmp, elementType, index)}"
    )
    val convertedElement: GeneratedExpressionCode = createConvertCode(ctx, element, elementType)

    // go through the input array to calculate how many bytes we need.
    val calculateNumBytes = elementType match {
      case _ if ctx.isPrimitiveType(elementType) =>
        // Should we do word align?
        val elementSize = elementType.defaultSize
        s"""
          $numBytes += $elementSize * $numElements;
        """
      case t: DecimalType if t.precision <= Decimal.MAX_LONG_DIGITS =>
        s"""
          $numBytes += 8 * $numElements;
        """
      case _ =>
        val writer = getWriter(elementType)
        val elementSize = s"$writer.getSize($elements[$index])"
        // TODO(davies): avoid the copy
        val unsafeType = elementType match {
          case _: StructType => "UnsafeRow"
          case _: ArrayType => "UnsafeArrayData"
          case _: MapType => "UnsafeMapData"
          case _ => ctx.javaType(elementType)
        }
        val copy = elementType match {
          // We reuse the buffer during conversion, need copy it before process next element.
          case _: StructType | _: ArrayType | _: MapType => ".copy()"
          case _ => ""
        }

        val newElements = if (elementType == BinaryType) {
          s"new byte[$numElements][]"
        } else {
          s"new $unsafeType[$numElements]"
        }
        s"""
          final $unsafeType[] $elements = $newElements;
          for (int $index = 0; $index < $numElements; $index++) {
            ${convertedElement.code}
            if (!${convertedElement.isNull}) {
              $elements[$index] = ${convertedElement.primitive}$copy;
              $numBytes += $elementSize;
            }
          }
        """
    }

    val writeElement = elementType match {
      case _ if ctx.isPrimitiveType(elementType) =>
        // Should we do word align?
        val elementSize = elementType.defaultSize
        s"""
          Platform.put${ctx.primitiveTypeName(elementType)}(
            $buffer,
            Platform.BYTE_ARRAY_OFFSET + $cursor,
            ${convertedElement.primitive});
          $cursor += $elementSize;
        """
      case t: DecimalType if t.precision <= Decimal.MAX_LONG_DIGITS =>
        s"""
          Platform.putLong(
            $buffer,
            Platform.BYTE_ARRAY_OFFSET + $cursor,
            ${convertedElement.primitive}.toUnscaledLong());
          $cursor += 8;
        """
      case _ =>
        val writer = getWriter(elementType)
        s"""
          $cursor += $writer.write(
            $buffer,
            Platform.BYTE_ARRAY_OFFSET + $cursor,
            $elements[$index]);
        """
    }

    val checkNull = elementType match {
      case _ if ctx.isPrimitiveType(elementType) => s"${convertedElement.isNull}"
      case t: DecimalType => s"$elements[$index] == null" +
        s" || !$elements[$index].changePrecision(${t.precision}, ${t.scale})"
      case _ => s"$elements[$index] == null"
    }

    val code = s"""
      ${input.code}
      final boolean $outputIsNull = ${input.isNull};
      if (!$outputIsNull) {
        final ArrayData $tmp = ${input.primitive};
        if ($tmp instanceof UnsafeArrayData) {
          $output = (UnsafeArrayData) $tmp;
        } else {
          final int $numElements = $tmp.numElements();
          final int $fixedSize = 4 * $numElements;
          int $numBytes = $fixedSize;

          $calculateNumBytes

          if ($numBytes > $buffer.length) {
            $buffer = new byte[$numBytes];
          }

          int $cursor = $fixedSize;
          for (int $index = 0; $index < $numElements; $index++) {
            if ($checkNull) {
              // If element is null, write the negative value address into offset region.
              Platform.putInt($buffer, Platform.BYTE_ARRAY_OFFSET + 4 * $index, -$cursor);
            } else {
              Platform.putInt($buffer, Platform.BYTE_ARRAY_OFFSET + 4 * $index, $cursor);
              $writeElement
            }
          }

          $output.pointTo(
            $buffer,
            Platform.BYTE_ARRAY_OFFSET,
            $numElements,
            $numBytes);
        }
      }
      """
    GeneratedExpressionCode(code, outputIsNull, output)
  }

  private def createCodeForMap(
      ctx: CodeGenContext,
      input: GeneratedExpressionCode,
      keyType: DataType,
      valueType: DataType): GeneratedExpressionCode = {
    val output = ctx.freshName("convertedMap")
    val outputIsNull = ctx.freshName("isNull")
    val tmp = ctx.freshName("tmp")

    val keyArray = GeneratedExpressionCode(
      code = "",
      isNull = "false",
      primitive = s"$tmp.keyArray()"
    )
    val valueArray = GeneratedExpressionCode(
      code = "",
      isNull = "false",
      primitive = s"$tmp.valueArray()"
    )
    val convertedKeys: GeneratedExpressionCode = createCodeForArray(ctx, keyArray, keyType)
    val convertedValues: GeneratedExpressionCode = createCodeForArray(ctx, valueArray, valueType)

    val code = s"""
      ${input.code}
      final boolean $outputIsNull = ${input.isNull};
      UnsafeMapData $output = null;
      if (!$outputIsNull) {
        final MapData $tmp = ${input.primitive};
        if ($tmp instanceof UnsafeMapData) {
          $output = (UnsafeMapData) $tmp;
        } else {
          ${convertedKeys.code}
          ${convertedValues.code}
          $output = new UnsafeMapData(${convertedKeys.primitive}, ${convertedValues.primitive});
        }
      }
      """
    GeneratedExpressionCode(code, outputIsNull, output)
  }

  /**
   * Generates the java code to convert a data to its unsafe version.
   */
  private def createConvertCode(
      ctx: CodeGenContext,
      input: GeneratedExpressionCode,
      dataType: DataType): GeneratedExpressionCode = dataType match {
    case t: StructType =>
      val output = ctx.freshName("convertedStruct")
      val outputIsNull = ctx.freshName("isNull")
      val tmp = ctx.freshName("tmp")
      val fieldTypes = t.fields.map(_.dataType)
      val fieldEvals = fieldTypes.zipWithIndex.map { case (dt, i) =>
        val getFieldCode = ctx.getValue(tmp, dt, i.toString)
        val fieldIsNull = s"$tmp.isNullAt($i)"
        GeneratedExpressionCode("", fieldIsNull, getFieldCode)
      }
      val converter = createCodeForStruct(ctx, tmp, fieldEvals, fieldTypes)
      val code = s"""
        ${input.code}
         UnsafeRow $output = null;
         final boolean $outputIsNull = ${input.isNull};
         if (!$outputIsNull) {
           final InternalRow $tmp = ${input.primitive};
           if ($tmp instanceof UnsafeRow) {
             $output = (UnsafeRow) $tmp;
           } else {
             ${converter.code}
             $output = ${converter.primitive};
           }
         }
        """
      GeneratedExpressionCode(code, outputIsNull, output)

    case ArrayType(elementType, _) => createCodeForArray(ctx, input, elementType)

    case MapType(kt, vt, _) => createCodeForMap(ctx, input, kt, vt)

    case _ => input
  }

  def createCode(ctx: CodeGenContext, expressions: Seq[Expression]): GeneratedExpressionCode = {
    val exprEvals = expressions.map(e => e.gen(ctx))
    val exprTypes = expressions.map(_.dataType)
    createCodeForStruct(ctx, "i", exprEvals, exprTypes)
  }

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  protected def create(expressions: Seq[Expression]): UnsafeProjection = {
    val ctx = newCodeGenContext()

    val eval = createCode(ctx, expressions)

    val code = s"""
      public Object generate($exprType[] exprs) {
        return new SpecificUnsafeProjection(exprs);
      }

      class SpecificUnsafeProjection extends ${classOf[UnsafeProjection].getName} {

        private $exprType[] expressions;

        ${declareMutableStates(ctx)}
        ${declareAddedFunctions(ctx)}

        public SpecificUnsafeProjection($exprType[] expressions) {
          this.expressions = expressions;
          ${initMutableStates(ctx)}
        }

        // Scala.Function1 need this
        public Object apply(Object row) {
          return apply((InternalRow) row);
        }

        public UnsafeRow apply(InternalRow i) {
          ${eval.code}
          return ${eval.primitive};
        }
      }
      """

    logDebug(s"code for ${expressions.mkString(",")}:\n${CodeFormatter.format(code)}")

    val c = compile(code)
    c.generate(ctx.references.toArray).asInstanceOf[UnsafeProjection]
  }
}
