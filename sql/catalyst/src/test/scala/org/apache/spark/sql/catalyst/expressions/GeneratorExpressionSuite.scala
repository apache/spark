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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.{SPARK_DOC_ROOT, SparkFunSuite}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._

class GeneratorExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {
  private def checkTuple(actual: Expression, expected: Seq[InternalRow]): Unit = {
    assert(actual.eval(null).asInstanceOf[IterableOnce[InternalRow]].iterator.to(Seq) === expected)
  }

  private final val empty_array = CreateArray(Seq.empty)
  private final val int_array = CreateArray(Seq(1, 2, 3).map(Literal(_)))
  private final val str_array = CreateArray(Seq("a", "b", "c").map(Literal(_)))

  test("explode") {
    val int_correct_answer = Seq(create_row(1), create_row(2), create_row(3))
    val str_correct_answer = Seq(create_row("a"), create_row("b"), create_row("c"))

    checkTuple(Explode(empty_array), Seq.empty)
    checkTuple(Explode(int_array), int_correct_answer)
    checkTuple(Explode(str_array), str_correct_answer)
  }

  test("posexplode") {
    val int_correct_answer = Seq(create_row(0, 1), create_row(1, 2), create_row(2, 3))
    val str_correct_answer = Seq(create_row(0, "a"), create_row(1, "b"), create_row(2, "c"))

    checkTuple(PosExplode(CreateArray(Seq.empty)), Seq.empty)
    checkTuple(PosExplode(int_array), int_correct_answer)
    checkTuple(PosExplode(str_array), str_correct_answer)
  }

  test("inline") {
    val correct_answer = Seq(create_row(0, "a"), create_row(1, "b"), create_row(2, "c"))

    checkTuple(
      Inline(Literal.create(Array(), ArrayType(new StructType().add("id", LongType)))),
      Seq.empty)

    checkTuple(
      Inline(CreateArray(Seq(
        CreateStruct(Seq(Literal(0), Literal("a"))),
        CreateStruct(Seq(Literal(1), Literal("b"))),
        CreateStruct(Seq(Literal(2), Literal("c")))
      ))),
      correct_answer)
  }

  test("stack") {
    checkTuple(Stack(Seq(1, 1).map(Literal(_))), Seq(create_row(1)))
    checkTuple(Stack(Seq(1, 1, 2).map(Literal(_))), Seq(create_row(1, 2)))
    checkTuple(Stack(Seq(2, 1, 2).map(Literal(_))), Seq(create_row(1), create_row(2)))
    checkTuple(Stack(Seq(2, 1, 2, 3).map(Literal(_))), Seq(create_row(1, 2), create_row(3, null)))
    checkTuple(Stack(Seq(3, 1, 2, 3).map(Literal(_))), Seq(1, 2, 3).map(create_row(_)))
    checkTuple(Stack(Seq(4, 1, 2, 3).map(Literal(_))), Seq(1, 2, 3, null).map(create_row(_)))

    checkTuple(
      Stack(Seq(3, 1, 1.0, "a", 2, 2.0, "b", 3, 3.0, "c").map(Literal(_))),
      Seq(create_row(1, 1.0, "a"), create_row(2, 2.0, "b"), create_row(3, 3.0, "c")))

    checkError(
      exception = intercept[AnalysisException] {
        Stack(Seq(Literal(1))).checkInputDataTypes()
      },
      condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map(
        "functionName" -> "`stack`",
        "expectedNum" -> "> 1",
        "actualNum" -> "1",
        "docroot" -> SPARK_DOC_ROOT)
    )
    checkError(
      exception = intercept[AnalysisException] {
        Stack(Seq(Literal(1.0))).checkInputDataTypes()
      },
      condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map(
        "functionName" -> "`stack`",
        "expectedNum" -> "> 1",
        "actualNum" -> "1",
        "docroot" -> SPARK_DOC_ROOT)
    )
    assert(Stack(Seq(Literal(1), Literal(1), Literal(1.0))).checkInputDataTypes().isSuccess)
    assert(Stack(Seq(Literal(2), Literal(1), Literal(1.0))).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "STACK_COLUMN_DIFF_TYPES",
        messageParameters = Map(
          "rightParamIndex" -> "2",
          "leftType" -> "\"INT\"",
          "leftParamIndex" -> "1",
          "columnIndex" -> "0",
          "rightType" -> "\"DOUBLE\""
        )
      )
    )
  }

  test("unnest - eval is lazy and only reads elements as rows are pulled") {
    // Backing array that records which ordinals were read, to prove that pulling the first N rows
    // touches only the first N elements rather than materializing the whole expansion up front.
    val readOrdinals = scala.collection.mutable.ArrayBuffer.empty[Int]
    val tracking = new GenericArrayData(Array[Any](10, 20, 30, 40, 50)) {
      override def get(ordinal: Int, elementType: DataType): AnyRef = {
        readOrdinals += ordinal
        super.get(ordinal, elementType)
      }
    }
    val result = Unnest(Seq(Literal(tracking, ArrayType(IntegerType))), withOrdinality = true)
      .eval(null)
    // The returned value is a lazy Iterator, not an eagerly materialized collection, and building
    // it must not read any element.
    assert(result.isInstanceOf[Iterator[_]])
    // Literal's constructor validates its value by reading element 0; ignore reads made before the
    // iterator is created and observe only what pulling rows drives.
    readOrdinals.clear()

    val it = result.iterator
    assert(readOrdinals.isEmpty, "no element should be read before the iterator is advanced")
    assert(it.next() === create_row(10, 1L))
    assert(it.next() === create_row(20, 2L))
    // Only the two consumed rows' elements were read; rows 2..4 remain untouched.
    assert(readOrdinals.toSeq === Seq(0, 1))
  }

  test("unnest - single array") {
    checkTuple(Unnest(Seq(empty_array), withOrdinality = false), Seq.empty)
    checkTuple(
      Unnest(Seq(int_array), withOrdinality = false),
      Seq(create_row(1), create_row(2), create_row(3)))
    // A null array is treated as empty and contributes no rows.
    checkTuple(
      Unnest(Seq(Literal.create(null, ArrayType(IntegerType))), withOrdinality = false),
      Seq.empty)
  }

  test("unnest - single column naming and ordinality") {
    // With a single array the output column keeps the default name `col`.
    assert(Unnest(Seq(int_array), withOrdinality = false).elementSchema ===
      new StructType().add("col", IntegerType, nullable = false))
    // WITH ORDINALITY appends a 1-based, non-nullable bigint column.
    assert(Unnest(Seq(int_array), withOrdinality = true).elementSchema ===
      new StructType()
        .add("col", IntegerType, nullable = false)
        .add("ordinality", LongType, nullable = false))
    checkTuple(
      Unnest(Seq(str_array), withOrdinality = true),
      Seq(create_row("a", 1L), create_row("b", 2L), create_row("c", 3L)))
  }

  test("unnest - multiple arrays are zipped and padded with nulls") {
    val short_array = CreateArray(Seq(10, 20).map(Literal(_)))
    // With several arrays the columns are named positionally and padded columns are nullable.
    assert(Unnest(Seq(int_array, short_array), withOrdinality = false).elementSchema ===
      new StructType()
        .add("col0", IntegerType, nullable = true)
        .add("col1", IntegerType, nullable = true))
    checkTuple(
      Unnest(Seq(int_array, short_array), withOrdinality = false),
      Seq(create_row(1, 10), create_row(2, 20), create_row(3, null)))
    // WITH ORDINALITY spans the full (longest) length.
    checkTuple(
      Unnest(Seq(int_array, short_array), withOrdinality = true),
      Seq(create_row(1, 10, 1L), create_row(2, 20, 2L), create_row(3, null, 3L)))
  }

  test("unnest - type checks") {
    assert(Unnest(Seq(int_array), withOrdinality = false).checkInputDataTypes().isSuccess)

    // Providing no arguments is rejected.
    checkError(
      exception = intercept[AnalysisException] {
        Unnest(Seq.empty, withOrdinality = false).checkInputDataTypes()
      },
      condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map(
        "functionName" -> "`unnest`",
        "expectedNum" -> "> 0",
        "actualNum" -> "0",
        "docroot" -> SPARK_DOC_ROOT))

    // A non-array argument is rejected, reporting the offending 1-based parameter index.
    assert(Unnest(Seq(int_array, Literal(3)), withOrdinality = false).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> "second",
          "requiredType" -> "\"ARRAY\"",
          "inputSql" -> "\"3\"",
          "inputType" -> "\"INT\"")))
  }

  test("unnest - string representation hides the ordinality flag") {
    // The `withOrdinality` boolean must not leak into plan output as a bare `true` argument; it is
    // rendered as a readable `WITH ORDINALITY` suffix instead (see the EXPLAIN golden results).
    assert(Unnest(Seq(int_array), withOrdinality = false).toString === "unnest(array(1, 2, 3))")
    assert(Unnest(Seq(int_array), withOrdinality = true).toString ===
      "unnest(array(1, 2, 3), WITH ORDINALITY)")
  }
}
