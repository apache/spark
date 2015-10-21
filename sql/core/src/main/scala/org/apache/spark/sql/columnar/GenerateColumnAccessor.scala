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

package org.apache.spark.sql.columnar

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeFormatter, CodeGenerator}
import org.apache.spark.sql.types._

/**
 * An Iterator to walk throught the InternalRows from a CachedBatch
 */
abstract class ColumnarIterator extends Iterator[InternalRow] {
  def initialize(input: Iterator[CachedBatch], mutableRow: MutableRow, columnTypes: Array[DataType],
    columnIndexes: Array[Int]): Unit
}

/**
 * Generates bytecode for an [[ColumnarIterator]] for columnar cache.
 */
object GenerateColumnAccessor extends CodeGenerator[Seq[DataType], ColumnarIterator] with Logging {

  protected def canonicalize(in: Seq[DataType]): Seq[DataType] = in
  protected def bind(in: Seq[DataType], inputSchema: Seq[Attribute]): Seq[DataType] = in

  protected def create(columnTypes: Seq[DataType]): ColumnarIterator = {
    val ctx = newCodeGenContext()
    val (initializeAccessors, extractors) = columnTypes.zipWithIndex.map { case (dt, index) =>
      val accessorName = ctx.freshName("accessor")
      val accessorCls = dt match {
        case NullType => classOf[NullColumnAccessor].getName
        case BooleanType => classOf[BooleanColumnAccessor].getName
        case ByteType => classOf[ByteColumnAccessor].getName
        case ShortType => classOf[ShortColumnAccessor].getName
        case IntegerType | DateType => classOf[IntColumnAccessor].getName
        case LongType | TimestampType => classOf[LongColumnAccessor].getName
        case FloatType => classOf[FloatColumnAccessor].getName
        case DoubleType => classOf[DoubleColumnAccessor].getName
        case StringType => classOf[StringColumnAccessor].getName
        case BinaryType => classOf[BinaryColumnAccessor].getName
        case dt: DecimalType if dt.precision <= Decimal.MAX_LONG_DIGITS =>
          classOf[CompactDecimalColumnAccessor].getName
        case dt: DecimalType => classOf[DecimalColumnAccessor].getName
        case struct: StructType => classOf[StructColumnAccessor].getName
        case array: ArrayType => classOf[ArrayColumnAccessor].getName
        case t: MapType => classOf[MapColumnAccessor].getName
      }
      ctx.addMutableState(accessorCls, accessorName, s"$accessorName = null;")

      val createCode = dt match {
        case t if ctx.isPrimitiveType(dt) =>
          s"$accessorName = new $accessorCls(ByteBuffer.wrap(buffers[$index]).order(nativeOrder));"
        case NullType | StringType | BinaryType =>
          s"$accessorName = new $accessorCls(ByteBuffer.wrap(buffers[$index]).order(nativeOrder));"
        case other =>
          s"""$accessorName = new $accessorCls(ByteBuffer.wrap(buffers[$index]).order(nativeOrder),
             (${dt.getClass.getName}) columnTypes[$index]);"""
      }

      val extract = s"$accessorName.extractTo(mutableRow, $index);"

      (createCode, extract)
    }.unzip

    val code = s"""
      import java.nio.ByteBuffer;
      import java.nio.ByteOrder;

      public SpecificColumnarIterator generate($exprType[] expr) {
        return new SpecificColumnarIterator();
      }

      class SpecificColumnarIterator extends ${classOf[ColumnarIterator].getName} {

        private ByteOrder nativeOrder = null;
        private byte[][] buffers = null;

        private int currentRow = 0;
        private int numRowsInBatch = 0;

        private scala.collection.Iterator input = null;
        private MutableRow mutableRow = null;
        private ${classOf[DataType].getName}[] columnTypes = null;
        private int[] columnIndexes = null;

        ${declareMutableStates(ctx)}

        public SpecificColumnarIterator() {
          this.nativeOrder = ByteOrder.nativeOrder();
          this.buffers = new byte[${columnTypes.length}][];

          ${initMutableStates(ctx)}
        }

        public void initialize(scala.collection.Iterator input, MutableRow mutableRow,
                               ${classOf[DataType].getName}[] columnTypes, int[] columnIndexes) {
          this.input = input;
          this.mutableRow = mutableRow;
          this.columnTypes = columnTypes;
          this.columnIndexes = columnIndexes;
        }

        public boolean hasNext() {
          if (currentRow < numRowsInBatch) {
            return true;
          }
          if (!input.hasNext()) {
            return false;
          }

          ${classOf[CachedBatch].getName} batch = (${classOf[CachedBatch].getName}) input.next();
          currentRow = 0;
          numRowsInBatch = batch.numRows();
          for (int i = 0; i < columnIndexes.length; i ++) {
            buffers[i] = batch.buffers()[columnIndexes[i]];
          }
          ${initializeAccessors.mkString("\n")}

          return hasNext();
        }

        public InternalRow next() {
          ${extractors.mkString("\n")}
          currentRow += 1;
          return mutableRow;
        }
      }"""

    logDebug(s"Generated ColumnarIterator: ${CodeFormatter.format(code)}")

    compile(code).generate(ctx.references.toArray).asInstanceOf[ColumnarIterator]
  }
}
