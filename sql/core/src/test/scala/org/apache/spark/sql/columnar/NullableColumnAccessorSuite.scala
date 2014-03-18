package org.apache.spark.sql
package columnar

import org.scalatest.FunSuite
import org.apache.spark.sql.catalyst.types.DataType
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow

class NullableColumnAccessorSuite extends FunSuite {
  import ColumnarTestData._

  testNullableColumnAccessor(BOOLEAN)
  testNullableColumnAccessor(INT)
  testNullableColumnAccessor(SHORT)
  testNullableColumnAccessor(LONG)
  testNullableColumnAccessor(BYTE)
  testNullableColumnAccessor(DOUBLE)
  testNullableColumnAccessor(FLOAT)
  testNullableColumnAccessor(STRING)
  testNullableColumnAccessor(BINARY)
  testNullableColumnAccessor(GENERIC)

  def testNullableColumnAccessor[T <: DataType, JvmType](columnType: ColumnType[T, JvmType]) {
    val typeName = columnType.getClass.getSimpleName.stripSuffix("$")

    test(s"$typeName accessor: empty column") {
      val builder = ColumnBuilder(columnType.typeId, 4)
      val accessor = ColumnAccessor(builder.build())
      assert(!accessor.hasNext)
    }

    test(s"$typeName accessor: access null values") {
      val builder = ColumnBuilder(columnType.typeId, 4)

      (0 until 4).foreach { _ =>
        builder.appendFrom(nonNullRandomRow, columnType.typeId)
        builder.appendFrom(nullRow, columnType.typeId)
      }

      val accessor = ColumnAccessor(builder.build())
      val row = new GenericMutableRow(1)

      (0 until 4).foreach { _ =>
        accessor.extractTo(row, 0)
        assert(row(0) === nonNullRandomRow(columnType.typeId))

        accessor.extractTo(row, 0)
        assert(row(0) === null)
      }
    }
  }
}
