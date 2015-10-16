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


abstract class ColumnarIterator extends Iterator[InternalRow] {
  def initialize(mutableRow: MutableRow, columnTypes: Array[DataType],
    input: Iterator[CachedBatch], bufferIndex: Array[Int]): Unit
}

/**
 * Generates bytecode for an [[Ordering]] of rows for a given set of expressions.
 */
object GenerateColumnAccessor extends CodeGenerator[Seq[DataType], ColumnarIterator] with Logging {

  protected def canonicalize(in: Seq[DataType]): Seq[DataType] = in
  protected def bind(in: Seq[DataType], inputSchema: Seq[Attribute]): Seq[DataType] = in

  protected def create(columnTypes: Seq[DataType]): ColumnarIterator = {
    val ctx = newCodeGenContext()
    val (creaters, accesses) = columnTypes.zipWithIndex.map { case (dataType, index) =>
      val accessorName = ctx.freshName("accessor")
      val dt = dataType match {
        case udt: UserDefinedType[_] => udt.sqlType
        case other => other
      }
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
          s"$accessorName = new $accessorCls(ByteBuffer.wrap(buffers[$index]).order(order));"
        case NullType | StringType | BinaryType =>
          s"$accessorName = new $accessorCls(ByteBuffer.wrap(buffers[$index]).order(order));"
        case other =>
          s"""$accessorName = new $accessorCls(ByteBuffer.wrap(buffers[$index]).order(order),
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

        private final ByteOrder order = null;
        private int currentRow = 0;
        private int totalRows = 0;
        private byte[][] buffers = null;
        private MutableRow mutableRow = null;
        private scala.collection.Iterator input = null;
        private ${classOf[DataType].getName}[] columnTypes = null;
        private int[] bufferIndex = null;

        ${declareMutableStates(ctx)}

        public SpecificColumnarIterator() {
          order = ByteOrder.nativeOrder();
        }

        public void initialize(MutableRow mutableRow, ${classOf[DataType].getName}[] columnTypes,
                               scala.collection.Iterator input, int[] bufferIndex) {
          ${initMutableStates(ctx)}

          this.mutableRow = mutableRow;
          this.columnTypes = columnTypes;
          this.input = input;
          this.bufferIndex = bufferIndex;
          this.buffers = new byte[bufferIndex.length][];
        }

        @Override
        public boolean hasNext() {
          if (currentRow < totalRows) {
            return true;
          }
          if (!input.hasNext()) {
            return false;
          }

          ${classOf[CachedBatch].getName} batch = (${classOf[CachedBatch].getName}) input.next();
          currentRow = 0;
          totalRows = batch.count();
          for (int i=0; i<bufferIndex.length; i++) {
            buffers[i] = batch.buffers()[bufferIndex[i]];
          }
          ${creaters.mkString("\n")}

          return hasNext();
        }

        @Override
        public InternalRow next() {
          ${accesses.mkString("\n")}
          currentRow += 1;
          return mutableRow;
        }
      }"""

    logDebug(s"Generated ColumnarIterator: ${CodeFormatter.format(code)}")

    compile(code).generate(ctx.references.toArray).asInstanceOf[ColumnarIterator]
  }
}
