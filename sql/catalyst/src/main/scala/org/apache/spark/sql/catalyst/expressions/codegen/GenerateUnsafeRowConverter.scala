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
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}

/**
 * Generates byte code that convert a InternalRow to an [[UnsafeRow]].
 */
object GenerateUnsafeRowConverter extends CodeGenerator[Array[DataType], UnsafeRowConverter] {

  protected def canonicalize(in: Array[DataType]): Array[DataType] = in

  protected def bind(in: Array[DataType], inputSchema: Seq[Attribute]): Array[DataType] = in

  protected def create(fields: Array[DataType]): UnsafeRowConverter = {
    val ctx = newCodeGenContext()
    val additionalSize = fields.zipWithIndex.map { case (dt, i) =>
      dt match {
        case StringType => s" + (row.isNullAt($i) ? 0 : stringWriter.getSize(row, $i))"
        case BinaryType => s" + (row.isNullAt($i) ? 0 : binaryWriter.getSize(row, $i))"
        case _ => ""
      }
    }.mkString("")

    val writers = fields.zipWithIndex.map { case (dt, i) =>
      val update = dt match {
        case dt if ctx.isPrimitiveType(dt) =>
          s"${ctx.setColumn("target", dt, i, ctx.getColumn("row", dt, i))}"
        case StringType =>
          s"cursor += stringWriter.write(row, target, $i, cursor)"
        case BinaryType =>
          s"cursor += binaryWriter.write(row, target, $i, cursor)"
        case _ =>
          s"objectWriter.write(row, target, $i, cursor)"
      }
      s"""if (row.isNullAt($i)) {
            target.setNullAt($i);
          } else {
            $update;
          }"""
    }.mkString("\n          ")
    val fixedSize = (8 * fields.length) + UnsafeRow.calculateBitSetWidthInBytes(fields.length)

    val code = s"""
      public Object generate($exprType[] expr) {
        return new SpecificProjection();
      }

      class SpecificProjection extends ${classOf[UnsafeRowConverter].getName} {

        private final org.apache.spark.sql.catalyst.expressions.StringUnsafeColumnWriter
          stringWriter = new org.apache.spark.sql.catalyst.expressions.StringUnsafeColumnWriter();
        private final org.apache.spark.sql.catalyst.expressions.BinaryUnsafeColumnWriter
          binaryWriter = new org.apache.spark.sql.catalyst.expressions.BinaryUnsafeColumnWriter();
        private final org.apache.spark.sql.catalyst.expressions.ObjectUnsafeColumnWriter
          objectWriter = new org.apache.spark.sql.catalyst.expressions.ObjectUnsafeColumnWriter();

        public SpecificProjection() {}

        public int numFields() {
          return ${fields.size};
        }

        public int getSizeRequirement(InternalRow row) {
          return $fixedSize $additionalSize;
        }

        public int writeRow(InternalRow row, UnsafeRow target) {
          int cursor = $fixedSize;
          $writers
          return cursor;
        }
      }
    """

    logDebug(s"code for ${fields.mkString(",")}:\n$code")

    val c = compile(code)
    c.generate(null).asInstanceOf[UnsafeRowConverter]
  }
}
