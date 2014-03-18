package org.apache.spark.sql
package columnar

import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.types.DataType
import org.apache.spark.sql.execution.KryoSerializer

class NullableColumnBuilderSuite extends FunSuite {
  import ColumnarTestData._

  testNullableColumnBuilder(INT)
  testNullableColumnBuilder(LONG)
  testNullableColumnBuilder(SHORT)
  testNullableColumnBuilder(BOOLEAN)
  testNullableColumnBuilder(BYTE)
  testNullableColumnBuilder(STRING)
  testNullableColumnBuilder(DOUBLE)
  testNullableColumnBuilder(FLOAT)
  testNullableColumnBuilder(BINARY)
  testNullableColumnBuilder(GENERIC)

  def testNullableColumnBuilder[T <: DataType, JvmType](columnType: ColumnType[T, JvmType]) {
    val columnBuilder = ColumnBuilder(columnType.typeId)
    val typeName = columnType.getClass.getSimpleName.stripSuffix("$")

    test(s"$typeName column builder: empty column") {
      columnBuilder.initialize(4)

      val buffer = columnBuilder.build()

      // For column type ID
      assert(buffer.getInt() === columnType.typeId)
      // For null count
      assert(buffer.getInt === 0)
      assert(!buffer.hasRemaining)
    }

    test(s"$typeName column builder: buffer size auto growth") {
      columnBuilder.initialize(4)

      (0 until 4) foreach { _ =>
        columnBuilder.appendFrom(nonNullRandomRow, columnType.typeId)
      }

      val buffer = columnBuilder.build()

      // For column type ID
      assert(buffer.getInt() === columnType.typeId)
      // For null count
      assert(buffer.getInt() === 0)
    }

    test(s"$typeName column builder: null values") {
      columnBuilder.initialize(4)

      (0 until 4) foreach { _ =>
        columnBuilder.appendFrom(nonNullRandomRow, columnType.typeId)
        columnBuilder.appendFrom(nullRow, columnType.typeId)
      }

      val buffer = columnBuilder.build()

      // For column type ID
      assert(buffer.getInt() === columnType.typeId)
      // For null count
      assert(buffer.getInt() === 4)
      // For null positions
      (1 to 7 by 2).foreach(i => assert(buffer.getInt() === i))

      // For non-null values
      (0 until 4).foreach { _ =>
        val actual = if (columnType == GENERIC) {
          KryoSerializer.deserialize[Any](GENERIC.extract(buffer))
        } else {
          columnType.extract(buffer)
        }
        assert(actual === nonNullRandomRow(columnType.typeId))
      }

      assert(!buffer.hasRemaining)
    }
  }
}
