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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.ObjectPool
import org.apache.spark.sql.types.{StructType, DataType, BinaryType, StringType}


/**
 * A projection that returns an UnsafeRow.
 */
abstract class UnsafeProjection extends BaseProjection {
  /**
   * Returns an UnsafeRow from InternalRow.
   */
  def apply(row: InternalRow): UnsafeRow

  /**
   * Change the ObjectPool that is used inside projection. Once this is called,
   * the pool will not be cleared for each call `apply`.
   */
  def setPool(pool: ObjectPool)
}

/**
 * Generates byte code that convert a InternalRow to an [[UnsafeRow]].
 */
object GenerateUnsafeProjection extends CodeGenerator[Seq[Expression], UnsafeProjection] {

  def generate(schema: StructType): UnsafeProjection = generate(schema.fields.map(_.dataType))

  def generate(fields: Seq[DataType]): UnsafeProjection =
    generate(fields.zipWithIndex.map(x => new BoundReference(x._2, x._1, true)))

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  protected def create(expressions: Seq[Expression]): UnsafeProjection = {
    val ctx = newCodeGenContext()
    val codes = expressions.map(_.gen(ctx))
    val all_exprs = codes.map(_.code).mkString("\n")

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
          s"objectWriter.write(target, ${codes(i).primitive}, $i, cursor)"
      }
      s"""if (${codes(i).isNull}) {
            target.setNullAt($i);
          } else {
            $update;
          }"""
    }.mkString("\n          ")
    val fixedSize = (8 * expressions.length) + UnsafeRow.calculateBitSetWidthInBytes(expressions.length)

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

      private boolean externalPool = false;
      private org.apache.spark.sql.catalyst.util.ObjectPool pool =
        new org.apache.spark.sql.catalyst.util.ObjectPool(10);
      private UnsafeRow unsafeRow = new UnsafeRow(pool);
      private byte[] buffer = new byte[64];

      public SpecificProjection() {}

      public void setPool(org.apache.spark.sql.catalyst.util.ObjectPool pool) {
        this.pool = pool;
        this.externalPool = true;
      }

      public Object apply(Object i) {
        return
      }

      public UnsafeRow apply(InternalRow i) {
        ${all_exprs}

        if (!externalPool) {
          pool.clear();
        }
        int numBytes = $fixedSize $additionalSize;
        if (numBytes > buffer.length) {
          buffer = new byte[numBytes];
        }
        unsafeRow.pointTo(buffer, PlatformDependents.BYTE_ARRAY_OFFSET, ${expressions.size}, numBytes);
        int cursor = $fixedSize;
        $writers
        return unsafeRow;
      }
    }
    """

    println(s"code for ${expressions.mkString(",")}:\n$code")

    val c = compile(code)
    c.generate(null).asInstanceOf[UnsafeProjection]
  }
}
