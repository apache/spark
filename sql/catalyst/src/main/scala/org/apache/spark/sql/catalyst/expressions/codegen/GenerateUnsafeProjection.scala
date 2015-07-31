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

  /** Returns true iff we support this data type. */
  def canSupport(dataType: DataType): Boolean = dataType match {
    case t: AtomicType if !t.isInstanceOf[DecimalType] => true
    case _: CalendarIntervalType => true
    case t: StructType => t.toSeq.forall(field => canSupport(field.dataType))
    case NullType => true
    case t: DecimalType => true
    case _ => false
  }

  def genAdditionalSize(dt: DataType, ev: GeneratedExpressionCode): String = dt match {
    case t: DecimalType if t.precision > Decimal.MAX_LONG_DIGITS =>
      s" + (${ev.isNull} ? 0 : $DecimalWriter.getSize(${ev.primitive}))"
    case StringType =>
      s" + (${ev.isNull} ? 0 : $StringWriter.getSize(${ev.primitive}))"
    case BinaryType =>
      s" + (${ev.isNull} ? 0 : $BinaryWriter.getSize(${ev.primitive}))"
    case CalendarIntervalType =>
      s" + (${ev.isNull} ? 0 : 16)"
    case _: StructType =>
      s" + (${ev.isNull} ? 0 : $StructWriter.getSize(${ev.primitive}))"
    case _ => ""
  }

  def genFieldWriter(
      ctx: CodeGenContext,
      fieldType: DataType,
      ev: GeneratedExpressionCode,
      primitive: String,
      index: Int,
      cursor: String): String = fieldType match {
    case _ if ctx.isPrimitiveType(fieldType) =>
      s"${ctx.setColumn(primitive, fieldType, index, ev.primitive)}"
    case t: DecimalType if t.precision <= Decimal.MAX_LONG_DIGITS =>
      s"""
       // make sure Decimal object has the same scale as DecimalType
       if (${ev.primitive}.changePrecision(${t.precision}, ${t.scale})) {
         $CompactDecimalWriter.write($primitive, $index, $cursor, ${ev.primitive});
       } else {
         $primitive.setNullAt($index);
       }
       """
    case t: DecimalType if t.precision > Decimal.MAX_LONG_DIGITS =>
      s"""
       // make sure Decimal object has the same scale as DecimalType
       if (${ev.primitive}.changePrecision(${t.precision}, ${t.scale})) {
         $cursor += $DecimalWriter.write($primitive, $index, $cursor, ${ev.primitive});
       } else {
         $primitive.setNullAt($index);
       }
       """
    case StringType =>
      s"$cursor += $StringWriter.write($primitive, $index, $cursor, ${ev.primitive})"
    case BinaryType =>
      s"$cursor += $BinaryWriter.write($primitive, $index, $cursor, ${ev.primitive})"
    case CalendarIntervalType =>
      s"$cursor += $IntervalWriter.write($primitive, $index, $cursor, ${ev.primitive})"
    case t: StructType =>
      s"$cursor += $StructWriter.write($primitive, $index, $cursor, ${ev.primitive})"
    case NullType => ""
    case _ =>
      throw new UnsupportedOperationException(s"Not supported DataType: $fieldType")
  }

  /**
   * Generates the code to create an [[UnsafeRow]] object based on the input expressions.
   * @param ctx context for code generation
   * @param ev specifies the name of the variable for the output [[UnsafeRow]] object
   * @param expressions input expressions
   * @return generated code to put the expression output into an [[UnsafeRow]]
   */
  def createCode(ctx: CodeGenContext, ev: GeneratedExpressionCode, expressions: Seq[Expression])
    : String = {

    val ret = ev.primitive
    ctx.addMutableState("UnsafeRow", ret, s"$ret = new UnsafeRow();")
    val buffer = ctx.freshName("buffer")
    ctx.addMutableState("byte[]", buffer, s"$buffer = new byte[64];")
    val cursor = ctx.freshName("cursor")
    val numBytes = ctx.freshName("numBytes")

    val exprs = expressions.map { e => e.dataType match {
      case st: StructType => createCodeForStruct(ctx, e.gen(ctx), st)
      case _ => e.gen(ctx)
    }}
    val allExprs = exprs.map(_.code).mkString("\n")

    val fixedSize = 8 * exprs.length + UnsafeRow.calculateBitSetWidthInBytes(exprs.length)
    val additionalSize = expressions.zipWithIndex.map {
      case (e, i) => genAdditionalSize(e.dataType, exprs(i))
    }.mkString("")

    val writers = expressions.zipWithIndex.map { case (e, i) =>
      val update = genFieldWriter(ctx, e.dataType, exprs(i), ret, i, cursor)
      s"""if (${exprs(i).isNull}) {
            $ret.setNullAt($i);
          } else {
            $update;
          }"""
    }.mkString("\n          ")

    s"""
      $allExprs
      int $numBytes = $fixedSize $additionalSize;
      if ($numBytes > $buffer.length) {
        $buffer = new byte[$numBytes];
      }

      $ret.pointTo(
        $buffer,
        org.apache.spark.unsafe.PlatformDependent.BYTE_ARRAY_OFFSET,
        ${expressions.size},
        $numBytes);
      int $cursor = $fixedSize;

      $writers
      boolean ${ev.isNull} = false;
     """
  }

  /**
   * Generates the Java code to convert a struct (backed by InternalRow) to UnsafeRow.
   *
   * This function also handles nested structs by recursively generating the code to do conversion.
   *
   * @param ctx code generation context
   * @param input the input struct, identified by a [[GeneratedExpressionCode]]
   * @param schema schema of the struct field
   */
  // TODO: refactor createCode and this function to reduce code duplication.
  private def createCodeForStruct(
      ctx: CodeGenContext,
      input: GeneratedExpressionCode,
      schema: StructType): GeneratedExpressionCode = {

    val isNull = input.isNull
    val primitive = ctx.freshName("structConvert")
    ctx.addMutableState("UnsafeRow", primitive, s"$primitive = new UnsafeRow();")
    val buffer = ctx.freshName("buffer")
    ctx.addMutableState("byte[]", buffer, s"$buffer = new byte[64];")
    val cursor = ctx.freshName("cursor")

    val exprs: Seq[GeneratedExpressionCode] = schema.map(_.dataType).zipWithIndex.map {
      case (dt, i) => dt match {
        case st: StructType =>
          val nestedStructEv = GeneratedExpressionCode(
            code = "",
            isNull = s"${input.primitive}.isNullAt($i)",
            primitive = s"${ctx.getValue(input.primitive, dt, i.toString)}"
          )
          createCodeForStruct(ctx, nestedStructEv, st)
        case _ =>
          GeneratedExpressionCode(
            code = "",
            isNull = s"${input.primitive}.isNullAt($i)",
            primitive = s"${ctx.getValue(input.primitive, dt, i.toString)}"
          )
        }
    }
    val allExprs = exprs.map(_.code).mkString("\n")

    val fixedSize = 8 * exprs.length + UnsafeRow.calculateBitSetWidthInBytes(exprs.length)
    val additionalSize = schema.toSeq.map(_.dataType).zip(exprs).map { case (dt, ev) =>
      genAdditionalSize(dt, ev)
    }.mkString("")

    val writers = schema.toSeq.map(_.dataType).zip(exprs).zipWithIndex.map { case ((dt, ev), i) =>
      val update = genFieldWriter(ctx, dt, ev, primitive, i, cursor)
      s"""
          if (${exprs(i).isNull}) {
            $primitive.setNullAt($i);
          } else {
            $update;
          }
        """
    }.mkString("\n          ")

    // Note that we add a shortcut here for performance: if the input is already an UnsafeRow,
    // just copy the bytes directly into our buffer space without running any conversion.
    // We also had to use a hack to introduce a "tmp" variable, to avoid the Java compiler from
    // complaining that a GenericMutableRow (generated by expressions) cannot be cast to UnsafeRow.
    val tmp = ctx.freshName("tmp")
    val numBytes = ctx.freshName("numBytes")
    val code = s"""
       |${input.code}
       |if (!${input.isNull}) {
       |  Object $tmp = (Object) ${input.primitive};
       |  if ($tmp instanceof UnsafeRow) {
       |    $primitive = (UnsafeRow) $tmp;
       |  } else {
       |    $allExprs
       |
       |    int $numBytes = $fixedSize $additionalSize;
       |    if ($numBytes > $buffer.length) {
       |      $buffer = new byte[$numBytes];
       |    }
       |
       |    $primitive.pointTo(
       |      $buffer,
       |      org.apache.spark.unsafe.PlatformDependent.BYTE_ARRAY_OFFSET,
       |      ${exprs.size},
       |      $numBytes);
       |    int $cursor = $fixedSize;
       |
       |    $writers
       |  }
       |}
     """.stripMargin

    GeneratedExpressionCode(code, isNull, primitive)
  }

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  protected def create(expressions: Seq[Expression]): UnsafeProjection = {
    val ctx = newCodeGenContext()

    val isNull = ctx.freshName("retIsNull")
    val primitive = ctx.freshName("retValue")
    val eval = GeneratedExpressionCode("", isNull, primitive)
    eval.code = createCode(ctx, eval, expressions)

    val code = s"""
      public Object generate($exprType[] exprs) {
        return new SpecificProjection(exprs);
      }

      class SpecificProjection extends ${classOf[UnsafeProjection].getName} {

        private $exprType[] expressions;

        ${declareMutableStates(ctx)}

        public SpecificProjection($exprType[] expressions) {
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
