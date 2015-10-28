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

  /** Returns true iff we support this data type. */
  def canSupport(dataType: DataType): Boolean = dataType match {
    case NullType => true
    case t: AtomicType => true
    case _: CalendarIntervalType => true
    case t: StructType => t.toSeq.forall(field => canSupport(field.dataType))
    case t: ArrayType if canSupport(t.elementType) => true
    case MapType(kt, vt, _) if canSupport(kt) && canSupport(vt) => true
    case dt: OpenHashSetUDT => false  // it's not a standard UDT
    case udt: UserDefinedType[_] => canSupport(udt.sqlType)
    case _ => false
  }

  private val rowWriterClass = classOf[UnsafeRowWriter].getName
  private val arrayWriterClass = classOf[UnsafeArrayWriter].getName

  // TODO: if the nullability of field is correct, we can use it to save null check.
  private def writeStructToBuffer(
      ctx: CodeGenContext,
      input: String,
      fieldTypes: Seq[DataType],
      bufferHolder: String): String = {
    val fieldEvals = fieldTypes.zipWithIndex.map { case (dt, i) =>
      val fieldName = ctx.freshName("fieldName")
      val code = s"final ${ctx.javaType(dt)} $fieldName = ${ctx.getValue(input, dt, i.toString)};"
      val isNull = s"$input.isNullAt($i)"
      GeneratedExpressionCode(code, isNull, fieldName)
    }

    s"""
      if ($input instanceof UnsafeRow) {
        ${writeUnsafeData(ctx, s"((UnsafeRow) $input)", bufferHolder)}
      } else {
        ${writeExpressionsToBuffer(ctx, input, fieldEvals, fieldTypes, bufferHolder)}
      }
    """
  }

  private def writeExpressionsToBuffer(
      ctx: CodeGenContext,
      row: String,
      inputs: Seq[GeneratedExpressionCode],
      inputTypes: Seq[DataType],
      bufferHolder: String): String = {
    val rowWriter = ctx.freshName("rowWriter")
    ctx.addMutableState(rowWriterClass, rowWriter, s"this.$rowWriter = new $rowWriterClass();")

    val writeFields = inputs.zip(inputTypes).zipWithIndex.map {
      case ((input, dataType), index) =>
        val dt = dataType match {
          case udt: UserDefinedType[_] => udt.sqlType
          case other => other
        }
        val tmpCursor = ctx.freshName("tmpCursor")

        val setNull = dt match {
          case t: DecimalType if t.precision > Decimal.MAX_LONG_DIGITS =>
            // Can't call setNullAt() for DecimalType with precision larger than 18.
            s"$rowWriter.write($index, (Decimal) null, ${t.precision}, ${t.scale});"
          case _ => s"$rowWriter.setNullAt($index);"
        }

        val writeField = dt match {
          case t: StructType =>
            s"""
              // Remember the current cursor so that we can calculate how many bytes are
              // written later.
              final int $tmpCursor = $bufferHolder.cursor;
              ${writeStructToBuffer(ctx, input.value, t.map(_.dataType), bufferHolder)}
              $rowWriter.setOffsetAndSize($index, $tmpCursor, $bufferHolder.cursor - $tmpCursor);
            """

          case a @ ArrayType(et, _) =>
            s"""
              // Remember the current cursor so that we can calculate how many bytes are
              // written later.
              final int $tmpCursor = $bufferHolder.cursor;
              ${writeArrayToBuffer(ctx, input.value, et, bufferHolder)}
              $rowWriter.setOffsetAndSize($index, $tmpCursor, $bufferHolder.cursor - $tmpCursor);
              $rowWriter.alignToWords($bufferHolder.cursor - $tmpCursor);
            """

          case m @ MapType(kt, vt, _) =>
            s"""
              // Remember the current cursor so that we can calculate how many bytes are
              // written later.
              final int $tmpCursor = $bufferHolder.cursor;
              ${writeMapToBuffer(ctx, input.value, kt, vt, bufferHolder)}
              $rowWriter.setOffsetAndSize($index, $tmpCursor, $bufferHolder.cursor - $tmpCursor);
              $rowWriter.alignToWords($bufferHolder.cursor - $tmpCursor);
            """

          case _ if ctx.isPrimitiveType(dt) =>
            s"""
              $rowWriter.write($index, ${input.value});
            """

          case t: DecimalType =>
            s"$rowWriter.write($index, ${input.value}, ${t.precision}, ${t.scale});"

          case NullType => ""

          case _ => s"$rowWriter.write($index, ${input.value});"
        }

        s"""
          ${input.code}
          if (${input.isNull}) {
            ${setNull.trim}
          } else {
            ${writeField.trim}
          }
        """
    }

    s"""
      $rowWriter.initialize($bufferHolder, ${inputs.length});
      ${ctx.splitExpressions(row, writeFields)}
    """.trim
  }

  // TODO: if the nullability of array element is correct, we can use it to save null check.
  private def writeArrayToBuffer(
      ctx: CodeGenContext,
      input: String,
      elementType: DataType,
      bufferHolder: String): String = {
    val arrayWriter = ctx.freshName("arrayWriter")
    ctx.addMutableState(arrayWriterClass, arrayWriter,
      s"this.$arrayWriter = new $arrayWriterClass();")
    val numElements = ctx.freshName("numElements")
    val index = ctx.freshName("index")
    val element = ctx.freshName("element")

    val et = elementType match {
      case udt: UserDefinedType[_] => udt.sqlType
      case other => other
    }

    val jt = ctx.javaType(et)

    val fixedElementSize = et match {
      case t: DecimalType if t.precision <= Decimal.MAX_LONG_DIGITS => 8
      case _ if ctx.isPrimitiveType(jt) => et.defaultSize
      case _ => 0
    }

    val writeElement = et match {
      case t: StructType =>
        s"""
          $arrayWriter.setOffset($index);
          ${writeStructToBuffer(ctx, element, t.map(_.dataType), bufferHolder)}
        """

      case a @ ArrayType(et, _) =>
        s"""
          $arrayWriter.setOffset($index);
          ${writeArrayToBuffer(ctx, element, et, bufferHolder)}
        """

      case m @ MapType(kt, vt, _) =>
        s"""
          $arrayWriter.setOffset($index);
          ${writeMapToBuffer(ctx, element, kt, vt, bufferHolder)}
        """

      case t: DecimalType =>
        s"$arrayWriter.write($index, $element, ${t.precision}, ${t.scale});"

      case NullType => ""

      case _ => s"$arrayWriter.write($index, $element);"
    }

    s"""
      if ($input instanceof UnsafeArrayData) {
        ${writeUnsafeData(ctx, s"((UnsafeArrayData) $input)", bufferHolder)}
      } else {
        final int $numElements = $input.numElements();
        $arrayWriter.initialize($bufferHolder, $numElements, $fixedElementSize);

        for (int $index = 0; $index < $numElements; $index++) {
          if ($input.isNullAt($index)) {
            $arrayWriter.setNullAt($index);
          } else {
            final $jt $element = ${ctx.getValue(input, et, index)};
            $writeElement
          }
        }
      }
    """
  }

  // TODO: if the nullability of value element is correct, we can use it to save null check.
  private def writeMapToBuffer(
      ctx: CodeGenContext,
      input: String,
      keyType: DataType,
      valueType: DataType,
      bufferHolder: String): String = {
    val keys = ctx.freshName("keys")
    val values = ctx.freshName("values")
    val tmpCursor = ctx.freshName("tmpCursor")


    // Writes out unsafe map according to the format described in `UnsafeMapData`.
    s"""
      if ($input instanceof UnsafeMapData) {
        ${writeUnsafeData(ctx, s"((UnsafeMapData) $input)", bufferHolder)}
      } else {
        final ArrayData $keys = $input.keyArray();
        final ArrayData $values = $input.valueArray();

        // preserve 4 bytes to write the key array numBytes later.
        $bufferHolder.grow(4);
        $bufferHolder.cursor += 4;

        // Remember the current cursor so that we can write numBytes of key array later.
        final int $tmpCursor = $bufferHolder.cursor;

        ${writeArrayToBuffer(ctx, keys, keyType, bufferHolder)}
        // Write the numBytes of key array into the first 4 bytes.
        Platform.putInt($bufferHolder.buffer, $tmpCursor - 4, $bufferHolder.cursor - $tmpCursor);

        ${writeArrayToBuffer(ctx, values, valueType, bufferHolder)}
      }
    """
  }

  /**
   * If the input is already in unsafe format, we don't need to go through all elements/fields,
   * we can directly write it.
   */
  private def writeUnsafeData(ctx: CodeGenContext, input: String, bufferHolder: String) = {
    val sizeInBytes = ctx.freshName("sizeInBytes")
    s"""
      final int $sizeInBytes = $input.getSizeInBytes();
      // grow the global buffer before writing data.
      $bufferHolder.grow($sizeInBytes);
      $input.writeToMemory($bufferHolder.buffer, $bufferHolder.cursor);
      $bufferHolder.cursor += $sizeInBytes;
    """
  }

  def createCode(ctx: CodeGenContext, expressions: Seq[Expression],
      useSubexprElimination: Boolean = false): GeneratedExpressionCode = {
    val exprEvals = ctx.generateExpressions(expressions, useSubexprElimination)
    val exprTypes = expressions.map(_.dataType)

    val result = ctx.freshName("result")
    ctx.addMutableState("UnsafeRow", result, s"this.$result = new UnsafeRow();")
    val bufferHolder = ctx.freshName("bufferHolder")
    val holderClass = classOf[BufferHolder].getName
    ctx.addMutableState(holderClass, bufferHolder, s"this.$bufferHolder = new $holderClass();")

    // Reset the isLoaded flag for each row.
    val subexprReset = ctx.subExprEliminationStates.map(s => {
     s"${s.isLoaded} = false;"
    }).mkString("\n")

    val code =
      s"""
        $bufferHolder.reset();
        $subexprReset
        ${writeExpressionsToBuffer(ctx, ctx.INPUT_ROW, exprEvals, exprTypes, bufferHolder)}

        $result.pointTo($bufferHolder.buffer, ${expressions.length}, $bufferHolder.totalSize());
      """
    GeneratedExpressionCode(code, "false", result)
  }

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  protected def create(expressions: Seq[Expression]): UnsafeProjection = {
    val ctx = newCodeGenContext()

    val eval = createCode(ctx, expressions, true)

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

        public UnsafeRow apply(InternalRow ${ctx.INPUT_ROW}) {
          ${eval.code.trim}
          return ${eval.value};
        }
      }
      """

    logDebug(s"code for ${expressions.mkString(",")}:\n${CodeFormatter.format(code)}")

    val c = compile(code)
    c.generate(ctx.references.toArray).asInstanceOf[UnsafeProjection]
  }
}
