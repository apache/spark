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

package org.apache.spark.sql.execution.columnar

import scala.collection.mutable

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodeGenerator, UnsafeRowWriter}
import org.apache.spark.sql.types._

/**
 * An Iterator to walk through the InternalRows from a CachedBatch
 */
abstract class ColumnarIterator extends Iterator[InternalRow] {
  def initialize(input: Iterator[CachedBatch], columnTypes: Array[DataType],
    columnIndexes: Array[Int]): Unit
}

/**
 * An helper class to update the fields of UnsafeRow, used by ColumnAccessor
 *
 * WARNING: These setter MUST be called in increasing order of ordinals.
 */
class MutableUnsafeRow(val writer: UnsafeRowWriter) extends GenericMutableRow(null) {

  override def isNullAt(i: Int): Boolean = writer.isNullAt(i)
  override def setNullAt(i: Int): Unit = writer.setNullAt(i)

  override def setBoolean(i: Int, v: Boolean): Unit = writer.write(i, v)
  override def setByte(i: Int, v: Byte): Unit = writer.write(i, v)
  override def setShort(i: Int, v: Short): Unit = writer.write(i, v)
  override def setInt(i: Int, v: Int): Unit = writer.write(i, v)
  override def setLong(i: Int, v: Long): Unit = writer.write(i, v)
  override def setFloat(i: Int, v: Float): Unit = writer.write(i, v)
  override def setDouble(i: Int, v: Double): Unit = writer.write(i, v)

  // the writer will be used directly to avoid creating wrapper objects
  override def setDecimal(i: Int, v: Decimal, precision: Int): Unit =
    throw new UnsupportedOperationException
  override def update(i: Int, v: Any): Unit = throw new UnsupportedOperationException

  // all other methods inherited from GenericMutableRow are not need
}

/**
 * Generates bytecode for an [[ColumnarIterator]] for columnar cache.
 */
object GenerateColumnAccessor extends CodeGenerator[Seq[DataType], ColumnarIterator] with Logging {

  protected def canonicalize(in: Seq[DataType]): Seq[DataType] = in
  protected def bind(in: Seq[DataType], inputSchema: Seq[Attribute]): Seq[DataType] = in

  protected def create(columnTypes: Seq[DataType]): ColumnarIterator = {
    val ctx = newCodeGenContext()
    val numFields = columnTypes.size
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
      ctx.addMutableState(accessorCls, accessorName, "")

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
      val patch = dt match {
        case DecimalType.Fixed(p, s) if p > Decimal.MAX_LONG_DIGITS =>
          // For large Decimal, it should have 16 bytes for future update even it's null now.
          s"""
            if (mutableRow.isNullAt($index)) {
              rowWriter.write($index, (Decimal) null, $p, $s);
            }
           """
        case other => ""
      }
      (createCode, extract + patch)
    }.unzip

    /*
     * 200 = 6000 bytes / 30 (up to 30 bytes per one call))
     * the maximum byte code size to be compiled for HotSpot is 8000.
     * We should keep less than 8000
     */
    val numberOfStatementsThreshold = 200
    val (initializerAccessorCalls, extractorCalls) =
      if (initializeAccessors.length <= numberOfStatementsThreshold) {
        (initializeAccessors.mkString("\n"), extractors.mkString("\n"))
      } else {
        val groupedAccessorsItr = initializeAccessors.grouped(numberOfStatementsThreshold)
        val groupedExtractorsItr = extractors.grouped(numberOfStatementsThreshold)
        var groupedAccessorsLength = 0
        groupedAccessorsItr.zipWithIndex.foreach { case (body, i) =>
          groupedAccessorsLength += 1
          val funcName = s"accessors$i"
          val funcCode = s"""
             |private void $funcName() {
             |  ${body.mkString("\n")}
             |}
           """.stripMargin
          ctx.addNewFunction(funcName, funcCode)
        }
        groupedExtractorsItr.zipWithIndex.foreach { case (body, i) =>
          val funcName = s"extractors$i"
          val funcCode = s"""
             |private void $funcName() {
             |  ${body.mkString("\n")}
             |}
           """.stripMargin
          ctx.addNewFunction(funcName, funcCode)
        }
        ((0 to groupedAccessorsLength - 1).map { i => s"accessors$i();" }.mkString("\n"),
         (0 to groupedAccessorsLength - 1).map { i => s"extractors$i();" }.mkString("\n"))
      }

    val codeBody = s"""
      import java.nio.ByteBuffer;
      import java.nio.ByteOrder;
      import scala.collection.Iterator;
      import org.apache.spark.sql.types.DataType;
      import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
      import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
      import org.apache.spark.sql.execution.columnar.MutableUnsafeRow;

      public SpecificColumnarIterator generate($exprType[] expr) {
        return new SpecificColumnarIterator();
      }

      class SpecificColumnarIterator extends ${classOf[ColumnarIterator].getName} {

        private ByteOrder nativeOrder = null;
        private byte[][] buffers = null;
        private UnsafeRow unsafeRow = new UnsafeRow();
        private BufferHolder bufferHolder = new BufferHolder();
        private UnsafeRowWriter rowWriter = new UnsafeRowWriter();
        private MutableUnsafeRow mutableRow = null;

        private int currentRow = 0;
        private int numRowsInBatch = 0;

        private scala.collection.Iterator input = null;
        private DataType[] columnTypes = null;
        private int[] columnIndexes = null;

        ${declareMutableStates(ctx)}

        public SpecificColumnarIterator() {
          this.nativeOrder = ByteOrder.nativeOrder();
          this.buffers = new byte[${columnTypes.length}][];
          this.mutableRow = new MutableUnsafeRow(rowWriter);
        }

        public void initialize(Iterator input, DataType[] columnTypes, int[] columnIndexes) {
          this.input = input;
          this.columnTypes = columnTypes;
          this.columnIndexes = columnIndexes;
        }

        ${declareAddedFunctions(ctx)}

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
          ${initializerAccessorCalls}

          return hasNext();
        }

        public InternalRow next() {
          currentRow += 1;
          bufferHolder.reset();
          rowWriter.initialize(bufferHolder, $numFields);
          ${extractorCalls}
          unsafeRow.pointTo(bufferHolder.buffer, $numFields, bufferHolder.totalSize());
          return unsafeRow;
        }
      }"""

    val code = new CodeAndComment(codeBody, ctx.getPlaceHolderToComments())
    logDebug(s"Generated ColumnarIterator: ${CodeFormatter.format(code)}")

    compile(code).generate(ctx.references.toArray).asInstanceOf[ColumnarIterator]
  }
}
