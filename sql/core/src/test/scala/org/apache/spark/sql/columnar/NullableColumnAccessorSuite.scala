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

import org.scalatest.FunSuite
import org.apache.spark.sql.catalyst.types.DataType
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow

class NullableColumnAccessorSuite extends FunSuite {
  import ColumnarTestData._

  Seq(INT, LONG, SHORT, BOOLEAN, BYTE, STRING, DOUBLE, FLOAT, BINARY, GENERIC).foreach {
    testNullableColumnAccessor(_)
  }

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
