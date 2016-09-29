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

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.execution.vectorized.ColumnarBatch
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

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
class MutableUnsafeRow(val writer: UnsafeRowWriter) extends BaseGenericInternalRow {
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
  override protected def genericGet(ordinal: Int): Any = throw new UnsupportedOperationException
  override def numFields: Int = throw new UnsupportedOperationException
  override def copy(): InternalRow = throw new UnsupportedOperationException

  def setDecimal(i: Int, v: Decimal, precision: Int, scale: Int): Unit =
    writer.write(i, v, precision, scale)
  def setUTF8String(i: Int, s: UTF8String): Unit = writer.write(i, s)
  def setBinary(i: Int, b: Array[Byte]): Unit = writer.write(i, b)
  def setArray(i: Int, a: ArrayData): Unit = {
    val u = a.asInstanceOf[UnsafeArrayData]
    val base = u.getBaseObject.asInstanceOf[Array[Byte]]
    val offset = u.getBaseOffset - Platform.BYTE_ARRAY_OFFSET
    if (offset > Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("Cannot write this array as it's too big.")
    }
    val size = u.getSizeInBytes
    writer.write(i, base, offset.toInt, size)
  }
  def setMap(i: Int, m: MapData): Unit = {
    val u = m.asInstanceOf[UnsafeMapData]
    val base = u.getBaseObject.asInstanceOf[Array[Byte]]
    val offset = u.getBaseOffset - Platform.BYTE_ARRAY_OFFSET
    if (offset > Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("Cannot write this array as it's too big.")
    }
    val size = u.getSizeInBytes
    writer.write(i, base, offset.toInt, size)
  }
  def setStruct(i: Int, r: MutableRow): Unit = {
    val u = r.asInstanceOf[UnsafeRow]
    val base = u.getBaseObject.asInstanceOf[Array[Byte]]
    val offset = u.getBaseOffset - Platform.BYTE_ARRAY_OFFSET
    if (offset > Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("Cannot write this array as it's too big.")
    }
    val size = u.getSizeInBytes
    writer.write(i, base, offset.toInt, size)
  }
}

/**
 * Generates bytecode for a [[ColumnarIterator]] for columnar cache.
 */
class GenerateColumnAccessor(conf: SparkConf)
    extends CodeGenerator[Seq[DataType], ColumnarIterator] with Logging {

  protected def canonicalize(in: Seq[DataType]): Seq[DataType] = in
  protected def bind(in: Seq[DataType], inputSchema: Seq[Attribute]): Seq[DataType] = in

  protected def create(columnTypes: Seq[DataType]): ColumnarIterator = {
    if (conf != null) {
      return createItrForCacheColumnarBatch(conf, columnTypes)
    }
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

      public SpecificColumnarIterator generate(Object[] references) {
        return new SpecificColumnarIterator();
      }

      class SpecificColumnarIterator extends ${classOf[ColumnarIterator].getName} {

        private ByteOrder nativeOrder = null;
        private byte[][] buffers = null;
        private UnsafeRow unsafeRow = new UnsafeRow($numFields);
        private BufferHolder bufferHolder = new BufferHolder(unsafeRow);
        private UnsafeRowWriter rowWriter = new UnsafeRowWriter(bufferHolder, $numFields);
        private MutableUnsafeRow mutableRow = null;

        private int currentRow = 0;
        private int numRowsInBatch = 0;

        private scala.collection.Iterator input = null;
        private DataType[] columnTypes = null;
        private int[] columnIndexes = null;

        ${ctx.declareMutableStates()}

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

        ${ctx.declareAddedFunctions()}

        public boolean hasNext() {
          if (currentRow < numRowsInBatch) {
            return true;
          }
          if (!input.hasNext()) {
            return false;
          }

          ${classOf[CachedBatchBytes].getName} batch =
            (${classOf[CachedBatchBytes].getName}) input.next();
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
          rowWriter.zeroOutNullBytes();
          ${extractorCalls}
          unsafeRow.setTotalSize(bufferHolder.totalSize());
          return unsafeRow;
        }
      }"""

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"Generated ColumnarIteratorForCachedBatchBytes:\n${CodeFormatter.format(code)}")

    CodeGenerator.compile(code).generate(Array.empty).asInstanceOf[ColumnarIterator]
  }

  protected def createItrForCacheColumnarBatch(conf: SparkConf, columnTypes: Seq[DataType])
      : ColumnarIterator = {
    val ctx = newCodeGenContext()
    val numFields = columnTypes.size
    val confVar = ctx.addReferenceObj("conf", conf, classOf[SparkConf].getName)

    val setters = ctx.splitExpressions(
      "row",
      columnTypes.zipWithIndex.map { case (dt, index) =>
        val setter = dt match {
          case NullType =>
            s"if (colInstances[$index].isNullAt(rowIdx)) { mutableRow.setNullAt($index); }\n"
          case BooleanType => s"setBoolean($index, colInstances[$index].getBoolean(rowIdx))"
          case ByteType => s"setByte($index, colInstances[$index].getByte(rowIdx))"
          case ShortType => s"setShort($index, colInstances[$index].getShort(rowIdx))"
          case IntegerType | DateType => s"setInt($index, colInstances[$index].getInt(rowIdx))"
          case LongType | TimestampType => s"setLong($index, colInstances[$index].getLong(rowIdx))"
          case FloatType => s"setFloat($index, colInstances[$index].getFloat(rowIdx))"
          case DoubleType => s"setDouble($index, colInstances[$index].getDouble(rowIdx))"
          case dt: DecimalType if dt.precision <= Decimal.MAX_INT_DIGITS =>
            s"setLong($index, (long)colInstances[$index].getInt(rowIdx))"
          case dt: DecimalType if dt.precision <= Decimal.MAX_LONG_DIGITS =>
            s"setLong($index, colInstances[$index].getLong(rowIdx))"
          case dt: DecimalType =>
            val p = dt.precision
            val s = dt.scale
            s"setDecimal($index, colInstances[$index].getDecimal(rowIdx, $p, $s), $p, $s)"
          case StringType => s"setUTF8String($index, colInstances[$index].getUTF8String(rowIdx))"
          case BinaryType => s"setBinary($index, colInstances[$index].getBinary(rowIdx))"
          case array: ArrayType => s"setArray($index, colInstances[$index].getArray(rowIdx))"
          case t: MapType => s"setMap($index, colInstances[$index].getMap(rowIdx))"
          case struct: StructType =>
            val s = struct.fields.length
            s"setStruct($index, colInstances[$index].getStruct(rowIdx, $s))"
        }

        dt match {
          case NullType => setter
          case dt: DecimalType if dt.precision > Decimal.MAX_LONG_DIGITS =>
            s"""
            if (colInstances[$index].isNullAt(rowIdx)) {
              mutableRow.setDecimal($index, null, ${dt.precision}, ${dt.scale});
            } else {
              mutableRow.$setter;
            }
           """
          case _ =>
            s"""
            if (colInstances[$index].isNullAt(rowIdx)) {
              mutableRow.setNullAt($index);
            } else {
              mutableRow.$setter;
            }
           """
        }
      }
    )

    val codeBody = s"""
      import scala.collection.Iterator;
      import org.apache.spark.sql.types.DataType;
      import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
      import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
      import org.apache.spark.sql.execution.columnar.MutableUnsafeRow;
      import org.apache.spark.sql.execution.vectorized.ColumnVector;
      import org.apache.spark.sql.execution.vectorized.OnHeapUnsafeColumnVector;

      public SpecificColumnarIterator generate(Object[] references) {
        return new SpecificColumnarIterator(references);
      }

      class SpecificColumnarIterator extends ${classOf[ColumnarIterator].getName} {
        private ColumnVector[] colInstances;
        private UnsafeRow unsafeRow = new UnsafeRow($numFields);
        private BufferHolder bufferHolder = new BufferHolder(unsafeRow);
        private UnsafeRowWriter rowWriter = new UnsafeRowWriter(bufferHolder, $numFields);
        private MutableUnsafeRow mutableRow = null;

        private int rowIdx = 0;
        private int numRowsInBatch = 0;

        private scala.collection.Iterator input = null;
        private DataType[] columnTypes = null;
        private int[] columnIndexes = null;

        ${ctx.declareMutableStates()}

        public SpecificColumnarIterator(Object[] references) {
          ${ctx.initMutableStates()}
          this.mutableRow = new MutableUnsafeRow(rowWriter);
        }

        public void initialize(Iterator input, DataType[] columnTypes, int[] columnIndexes) {
          this.input = input;
          this.columnTypes = columnTypes;
          this.columnIndexes = columnIndexes;
        }

        ${ctx.declareAddedFunctions()}

        public boolean hasNext() {
          if (rowIdx < numRowsInBatch) {
            return true;
          }
          if (!input.hasNext()) {
            return false;
          }

          ${classOf[CachedColumnarBatch].getName} cachedBatch =
            (${classOf[CachedColumnarBatch].getName}) input.next();
          ${classOf[ColumnarBatch].getName} batch = cachedBatch.columnarBatch();
          rowIdx = 0;
          numRowsInBatch = cachedBatch.getNumRows();
          colInstances = new ColumnVector[columnIndexes.length];
          for (int i = 0; i < columnIndexes.length; i ++) {
            colInstances[i] = batch.column(columnIndexes[i]);
            ((OnHeapUnsafeColumnVector)colInstances[i]).decompress($confVar);
          }

          return hasNext();
        }

        public InternalRow next() {
          bufferHolder.reset();
          rowWriter.zeroOutNullBytes();
          InternalRow row = null;
          ${setters}
          unsafeRow.setTotalSize(bufferHolder.totalSize());
          rowIdx += 1;
          return unsafeRow;
        }
      }"""

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"Generated ColumnarIteratorForCachedColumnarBatch:\n${CodeFormatter.format(code)}")

    CodeGenerator.compile(code).generate(ctx.references.toArray).asInstanceOf[ColumnarIterator]
  }

}
