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
import org.apache.spark.sql.types.{BinaryType, StringType}


/**
 * Generates a [[Projection]] that returns an [[UnsafeRow]].
 *
 * Note: The returned UnsafeRow will be pointed to a scratch buffer inside the projection.
 */
object GenerateUnsafeProjection extends CodeGenerator[Seq[Expression], UnsafeProjection] {

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  protected def create(expressions: Seq[Expression]): UnsafeProjection = {
    val ctx = newCodeGenContext()
    val codes = expressions.map(_.gen(ctx))
    val all_exprs = codes.map(_.code).mkString("\n")
    val fixedSize = 8 * codes.length + UnsafeRow.calculateBitSetWidthInBytes(codes.length)

    val additionalSize = expressions.zipWithIndex.map { case (e, i) =>
      e.dataType match {
        case StringType =>
          s" + (${codes(i).isNull} ? 0 : stringWriter.getSize(${codes(i).primitive}))"
        case BinaryType =>
          s" + (${codes(i).isNull} ? 0 : binaryWriter.getSize(${codes(i).primitive}))"
        case _ => ""
      }
    }.mkString("")

    val writers = expressions.zipWithIndex.map { case (e, i) =>
      val update = e.dataType match {
        case dt if ctx.isPrimitiveType(dt) =>
          s"${ctx.setColumn("target", dt, i, codes(i).primitive)}"
        case StringType =>
          s"cursor += stringWriter.write(target, ${codes(i).primitive}, $i, cursor)"
        case BinaryType =>
          s"cursor += binaryWriter.write(target, ${codes(i).primitive}, $i, cursor)"
        case _ =>
          throw new Exception(s"Not supported DataType: $dt")
      }
      s"""if (${codes(i).isNull}) {
            target.setNullAt($i);
          } else {
            $update;
          }"""
    }.mkString("\n          ")

    val code = s"""
    public Object generate($exprType[] expr) {
      return new SpecificProjection();
    }

    class SpecificProjection extends ${classOf[UnsafeProjection].getName} {

      private final org.apache.spark.sql.catalyst.expressions.StringUnsafeColumnWriter
        stringWriter = new org.apache.spark.sql.catalyst.expressions.StringUnsafeColumnWriter();
      private final org.apache.spark.sql.catalyst.expressions.BinaryUnsafeColumnWriter
        binaryWriter = new org.apache.spark.sql.catalyst.expressions.BinaryUnsafeColumnWriter();
      private final org.apache.spark.sql.catalyst.expressions.ObjectUnsafeColumnWriter
        objectWriter = new org.apache.spark.sql.catalyst.expressions.ObjectUnsafeColumnWriter();

      private UnsafeRow target = new UnsafeRow();
      private byte[] buffer = new byte[64];

      public SpecificProjection() {}

      public Object apply(Object row) {
        return apply((InternalRow) i);
      }

      public UnsafeRow apply(InternalRow i) {
        ${all_exprs}

        int numBytes = $fixedSize $additionalSize;
        if (numBytes > buffer.length) {
          buffer = new byte[numBytes];
        }
        target.pointTo(buffer, org.apache.spark.unsafe.PlatformDependent.BYTE_ARRAY_OFFSET,
          ${expressions.size}, numBytes, null);
        int cursor = $fixedSize;
        $writers
        return target;
      }
    }
    """

    logDebug(s"code for ${expressions.mkString(",")}:\n$code")

    val c = compile(code)
    c.generate(null).asInstanceOf[UnsafeProjection]
  }
}
