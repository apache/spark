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
import org.apache.spark.sql.execution.SparkSqlSerializer

class NullableColumnBuilderSuite extends FunSuite {
  import ColumnarTestData._

  Seq(INT, LONG, SHORT, BOOLEAN, BYTE, STRING, DOUBLE, FLOAT, BINARY, GENERIC).foreach {
    testNullableColumnBuilder(_)
  }

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
          SparkSqlSerializer.deserialize[Any](GENERIC.extract(buffer))
        } else {
          columnType.extract(buffer)
        }
        assert(actual === nonNullRandomRow(columnType.typeId))
      }

      assert(!buffer.hasRemaining)
    }
  }
}
