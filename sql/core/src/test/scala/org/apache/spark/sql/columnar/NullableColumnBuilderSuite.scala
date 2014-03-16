package org.apache.spark.sql
package columnar

import scala.util.Random

import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.types.DataType

class NullableColumnBuilderSuite extends FunSuite {
  testNullableColumnBuilder(new IntColumnBuilder,     INT)
  testNullableColumnBuilder(new LongColumnBuilder,    LONG)
  testNullableColumnBuilder(new ShortColumnBuilder,   SHORT)
  testNullableColumnBuilder(new BooleanColumnBuilder, BOOLEAN)
  testNullableColumnBuilder(new ByteColumnBuilder,    BYTE)
  testNullableColumnBuilder(new StringColumnBuilder,  STRING)
  testNullableColumnBuilder(new DoubleColumnBuilder,  DOUBLE)
  testNullableColumnBuilder(new FloatColumnBuilder,   FLOAT)
  testNullableColumnBuilder(new BinaryColumnBuilder,  BINARY)
  testNullableColumnBuilder(new GenericColumnBuilder, GENERIC)

  val nonNullTestRow = {
    val row = new GenericMutableRow(10)

    row(INT.typeId)     = Random.nextInt()
    row(LONG.typeId)    = Random.nextLong()
    row(FLOAT.typeId)   = Random.nextFloat()
    row(DOUBLE.typeId)  = Random.nextDouble()
    row(BOOLEAN.typeId) = Random.nextBoolean()
    row(BYTE.typeId)    = Random.nextInt(Byte.MaxValue).asInstanceOf[Byte]
    row(SHORT.typeId)   = Random.nextInt(Short.MaxValue).asInstanceOf[Short]
    row(STRING.typeId)  = Random.nextString(4)
    row(BINARY.typeId)  = {
      val bytes = new Array[Byte](4)
      Random.nextBytes(bytes)
      bytes
    }
    row(GENERIC.typeId) = Map(Random.nextInt() -> Random.nextString(4))

    row
  }

  val nullTestRow = {
    val row = new GenericMutableRow(10)
    (0 until 10).foreach(row.setNullAt)
    row
  }

  def testNullableColumnBuilder[T <: DataType, JvmType](
      columnBuilder: ColumnBuilder,
      columnType: ColumnType[T, JvmType]) {

    val columnBuilderName = columnBuilder.getClass.getSimpleName

    test(s"$columnBuilderName: empty column") {
      columnBuilder.initialize(4)

      val buffer = columnBuilder.build()

      // For null count
      assert(buffer.getInt === 0)
      // For column type ID
      assert(buffer.getInt() === columnType.typeId)
      assert(!buffer.hasRemaining)
    }

    test(s"$columnBuilderName: buffer size auto growth") {
      columnBuilder.initialize(4)

      (0 until 4) foreach { _ =>
        columnBuilder.append(nonNullTestRow, columnType.typeId)
      }

      val buffer = columnBuilder.build()

      // For null count
      assert(buffer.getInt() === 0)
      // For column type ID
      assert(buffer.getInt() === columnType.typeId)
    }

    test(s"$columnBuilderName: null values") {
      columnBuilder.initialize(4)

      (0 until 4) foreach { _ =>
        columnBuilder.append(nonNullTestRow, columnType.typeId)
        columnBuilder.append(nullTestRow, columnType.typeId)
      }

      val buffer = columnBuilder.build()

      // For null count
      assert(buffer.getInt() === 4)
      // For null positions
      (1 to 7 by 2).foreach(i => assert(buffer.getInt() === i))

      // For column type ID
      assert(buffer.getInt() === columnType.typeId)

      // For non-null values
      (0 until 4).foreach { _ =>
        assert(columnType.extract(buffer) === nonNullTestRow(columnType.typeId))
      }

      assert(!buffer.hasRemaining)
    }
  }
}
