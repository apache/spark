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

  /** Returns true iff we support this data type. */
  def canSupport(dataType: DataType): Boolean = dataType match {
    case t: AtomicType if !t.isInstanceOf[DecimalType] => true
    case _: IntervalType => true
    case NullType => true
    case _ => false
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
    val bufferTerm = ctx.freshName("buffer")
    ctx.addMutableState("byte[]", bufferTerm, s"$bufferTerm = new byte[64];")
    val cursorTerm = ctx.freshName("cursor")
    val numBytesTerm = ctx.freshName("numBytes")

    val exprs = expressions.map(_.gen(ctx))
    val allExprs = exprs.map(_.code).mkString("\n")
    val fixedSize = 8 * exprs.length + UnsafeRow.calculateBitSetWidthInBytes(exprs.length)

    val additionalSize = expressions.zipWithIndex.map { case (e, i) =>
      e.dataType match {
        case StringType =>
          s" + (${exprs(i).isNull} ? 0 : $StringWriter.getSize(${exprs(i).primitive}))"
        case BinaryType =>
          s" + (${exprs(i).isNull} ? 0 : $BinaryWriter.getSize(${exprs(i).primitive}))"
        case IntervalType =>
          s" + (${exprs(i).isNull} ? 0 : 16)"
        case _ => ""
      }
    }.mkString("")

    val writers = expressions.zipWithIndex.map { case (e, i) =>
      val update = e.dataType match {
        case dt if ctx.isPrimitiveType(dt) =>
          s"${ctx.setColumn(ret, dt, i, exprs(i).primitive)}"
        case StringType =>
          s"$cursorTerm += $StringWriter.write($ret, $i, $cursorTerm, ${exprs(i).primitive})"
        case BinaryType =>
          s"$cursorTerm += $BinaryWriter.write($ret, $i, $cursorTerm, ${exprs(i).primitive})"
        case IntervalType =>
          s"$cursorTerm += $IntervalWriter.write($ret, $i, $cursorTerm, ${exprs(i).primitive})"
        case NullType => ""
        case _ =>
          throw new UnsupportedOperationException(s"Not supported DataType: ${e.dataType}")
      }
      s"""if (${exprs(i).isNull}) {
            $ret.setNullAt($i);
          } else {
            $update;
          }"""
    }.mkString("\n          ")

    s"""
      $allExprs
      int $numBytesTerm = $fixedSize $additionalSize;
      if ($numBytesTerm > $bufferTerm.length) {
        $bufferTerm = new byte[$numBytesTerm];
      }

      $ret.pointTo(
        $bufferTerm,
        org.apache.spark.unsafe.PlatformDependent.BYTE_ARRAY_OFFSET,
        ${expressions.size},
        $numBytesTerm);
      int $cursorTerm = $fixedSize;


      $writers
      boolean ${ev.isNull} = false;
     """
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
      private $exprType[] expressions;

      public Object generate($exprType[] expr) {
        this.expressions = expr;
        return new SpecificProjection();
      }

      class SpecificProjection extends ${classOf[UnsafeProjection].getName} {

        ${declareMutableStates(ctx)}

        public SpecificProjection() {
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

    logDebug(s"code for ${expressions.mkString(",")}:\n$code")

    val c = compile(code)
    c.generate(ctx.references.toArray).asInstanceOf[UnsafeProjection]
  }
}
