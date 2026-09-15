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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.KeyedPartitioning
import org.apache.spark.sql.types._

class InternalRowComparableWrapperSuite extends SparkFunSuite {

  private val structA = new StructType().add("a", IntegerType)
  private val structB = new StructType().add("b", IntegerType)

  test("SPARK-59187: comparableTypes erases the naming and nothing else") {
    // The erasure has to answer exactly what `DataType.equalsStructurally` answers with
    // `ignoreNullability`, since that is the question two rows are asking of each other. Anything
    // erased beyond the naming would call two different values equal.
    val pairs = Seq(
      (IntegerType, IntegerType),
      (IntegerType, LongType),
      (structA, structB),
      (structA, new StructType().add("a", IntegerType, nullable = false)),
      (structA, new StructType().add("a", LongType)),
      (structA, new StructType().add("a", IntegerType).add("c", IntegerType)),
      (structA, IntegerType),
      (ArrayType(structA), ArrayType(structB)),
      (ArrayType(structA, containsNull = false), ArrayType(structB)),
      (ArrayType(structA), ArrayType(IntegerType)),
      (MapType(structA, structB), MapType(structB, structA)),
      (MapType(IntegerType, structA, valueContainsNull = false), MapType(IntegerType, structB)),
      (DecimalType(10, 2), DecimalType(10, 2)),
      (DecimalType(10, 2), DecimalType(12, 2)),
      (StringType, StringType("UTF8_LCASE")),
      // A char length and a UDT decide where a value belongs too, and a UDT's own `sqlType` names
      // its fields, so the erasure must not descend into one.
      (new StructType().add("c", CharType(5)), new StructType().add("x", CharType(5))),
      (CharType(5), CharType(6)),
      (CharType(5), VarcharType(5)),
      (new TestUDT.MyDenseVectorUDT, new TestUDT.MyDenseVectorUDT),
      (new ExampleBaseTypeUDT, new ExampleSubTypeUDT),
      // Metadata goes with the field name, and `equalsStructurally` ignores it too.
      (structA, new StructType().add(
        "a", IntegerType, nullable = true, new MetadataBuilder().putString("k", "v").build())))

    pairs.foreach { case (left, right) =>
      val erasedAgree = InternalRowComparableWrapper.comparableTypes(Seq(left)) ==
        InternalRowComparableWrapper.comparableTypes(Seq(right))
      assert(erasedAgree === DataType.equalsStructurally(left, right, ignoreNullability = true),
        s"$left and $right")
    }

    // Position by position, so two lists of different length are different.
    assert(InternalRowComparableWrapper.comparableTypes(Seq(IntegerType)) !=
      InternalRowComparableWrapper.comparableTypes(Seq(IntegerType, LongType)))
  }

  test("SPARK-59187: erasing is idempotent, and keeps the list it was given") {
    // Callers pass their own types in and hand the result back to another factory, so the erasure
    // has to settle on the second pass and give that pass its own list back.
    val exact = Seq(structA, ArrayType(structB, containsNull = false), IntegerType)
    val erased = InternalRowComparableWrapper.comparableTypes(exact)
    assert(erased != exact, "test setup: this list has something to erase")
    assert(InternalRowComparableWrapper.comparableTypes(erased) eq erased)

    val alreadyErased = Seq(IntegerType, ArrayType(LongType))
    assert(InternalRowComparableWrapper.comparableTypes(alreadyErased) eq alreadyErased)
  }

  test("SPARK-59249: the grouped key layout and wrapper equality hold one ordering instance") {
    // Identity is the property to assert, because behaviour is not what changes here: both sides
    // were already built by the same function, so they already compared the same way. What one
    // instance buys is that neither side can later be given a definition the other does not have.
    // `InternalRowComparableWrapper.equals` compares its rows with the instance below, and
    // `KeyedPartitioning` sorts and groups partition keys with it. The two type lists are built
    // separately, so this pins the shared cache as well.
    val wrapper = InternalRowComparableWrapper
      .getInternalRowComparableWrapperFactory(Seq(IntegerType, LongType))(InternalRow(1, 2L))

    assert(KeyedPartitioning.groupedKeyRowOrdering(Seq(IntegerType, LongType)) eq wrapper.ordering)
  }

  test("SPARK-59187: a factory answers for the types it settled on") {
    // A caller that reports a type list beside the rows a factory built takes it from here, so the
    // two cannot answer differently.
    val factory = InternalRowComparableWrapper.getInternalRowComparableWrapperFactory(Seq(structA))
    assert(factory.dataTypes !== Seq(structA), "test setup: this type has a name to erase")
    assert(factory.dataTypes === InternalRowComparableWrapper.comparableTypes(Seq(structA)))
    assert(factory(InternalRow(InternalRow(1))).dataTypes eq factory.dataTypes)
  }

  test("SPARK-59187: two rows of one value are equal however their columns were named") {
    // What the erasure is for. A storage-partitioned join compares key rows that came from two
    // sides' own columns, and the same value has to land in the same partition group either way.
    def wrap(dataType: DataType, value: InternalRow): InternalRowComparableWrapper =
      InternalRowComparableWrapper
        .getInternalRowComparableWrapperFactory(Seq(dataType))(value)

    val fromA = wrap(structA, InternalRow(InternalRow(1)))
    val fromB = wrap(structB, InternalRow(InternalRow(1)))
    assert(fromA === fromB, "the field names must not decide")
    assert(fromA.hashCode === fromB.hashCode, "and they must agree on the bucket too")
    assert(Set(fromA, fromB).size === 1, "so a set of them holds one")

    assert(fromA !== wrap(structA, InternalRow(InternalRow(2))), "two values are still two")
    assert(wrap(IntegerType, InternalRow(1)) !== wrap(LongType, InternalRow(1L)),
      "and a type that decides where a value belongs still tells them apart")
  }
}
