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

import scala.math._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{RandomDataGenerator, Row}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, GenerateOrdering, LazilyGeneratedOrdering}
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

class OrderingSuite extends SparkFunSuite with ExpressionEvalHelper {

  def compareArrays(a: Seq[Any], b: Seq[Any], expected: Int): Unit = {
    test(s"compare two arrays: a = $a, b = $b") {
      val dataType = ArrayType(IntegerType)
      val rowType = StructType(StructField("array", dataType, nullable = true) :: Nil)
      val toCatalyst = CatalystTypeConverters.createToCatalystConverter(rowType)
      val rowA = toCatalyst(Row(a)).asInstanceOf[InternalRow]
      val rowB = toCatalyst(Row(b)).asInstanceOf[InternalRow]
      Seq(Ascending, Descending).foreach { direction =>
        val sortOrder = direction match {
          case Ascending => BoundReference(0, dataType, nullable = true).asc
          case Descending => BoundReference(0, dataType, nullable = true).desc
        }
        val expectedCompareResult = direction match {
          case Ascending => signum(expected)
          case Descending => -1 * signum(expected)
        }

        val kryo = new KryoSerializer(new SparkConf).newInstance()
        val intOrdering = new InterpretedOrdering(sortOrder :: Nil)
        val genOrdering = new LazilyGeneratedOrdering(sortOrder :: Nil)
        val kryoIntOrdering = kryo.deserialize[InterpretedOrdering](kryo.serialize(intOrdering))
        val kryoGenOrdering = kryo.deserialize[LazilyGeneratedOrdering](kryo.serialize(genOrdering))

        Seq(intOrdering, genOrdering, kryoIntOrdering, kryoGenOrdering).foreach { ordering =>
          assert(ordering.compare(rowA, rowA) === 0)
          assert(ordering.compare(rowB, rowB) === 0)
          assert(signum(ordering.compare(rowA, rowB)) === expectedCompareResult)
          assert(signum(ordering.compare(rowB, rowA)) === -1 * expectedCompareResult)
        }
      }
    }
  }

  // Two arrays have the same size.
  compareArrays(Seq[Any](), Seq[Any](), 0)
  compareArrays(Seq[Any](1), Seq[Any](1), 0)
  compareArrays(Seq[Any](1, 2), Seq[Any](1, 2), 0)
  compareArrays(Seq[Any](1, 2, 2), Seq[Any](1, 2, 3), -1)

  // Two arrays have different sizes.
  compareArrays(Seq[Any](), Seq[Any](1), -1)
  compareArrays(Seq[Any](1, 2, 3), Seq[Any](1, 2, 3, 4), -1)
  compareArrays(Seq[Any](1, 2, 3), Seq[Any](1, 2, 3, 2), -1)
  compareArrays(Seq[Any](1, 2, 3), Seq[Any](1, 2, 2, 2), 1)

  // Arrays having nulls.
  compareArrays(Seq[Any](1, 2, 3), Seq[Any](1, 2, 3, null), -1)
  compareArrays(Seq[Any](), Seq[Any](null), -1)
  compareArrays(Seq[Any](null), Seq[Any](null), 0)
  compareArrays(Seq[Any](null, null), Seq[Any](null, null), 0)
  compareArrays(Seq[Any](null), Seq[Any](null, null), -1)
  compareArrays(Seq[Any](null), Seq[Any](1), -1)
  compareArrays(Seq[Any](null), Seq[Any](null, 1), -1)
  compareArrays(Seq[Any](null, 1), Seq[Any](1, 1), -1)
  compareArrays(Seq[Any](1, null, 1), Seq[Any](1, null, 1), 0)
  compareArrays(Seq[Any](1, null, 1), Seq[Any](1, null, 2), -1)

  // Test GenerateOrdering for all common types. For each type, we construct random input rows that
  // contain two columns of that type, then for pairs of randomly-generated rows we check that
  // GenerateOrdering agrees with RowOrdering.
  {
    val structType =
      new StructType()
        .add("f1", FloatType, nullable = true)
        .add("f2", ArrayType(BooleanType, containsNull = true), nullable = true)
    val arrayOfStructType = ArrayType(structType)
    val complexTypes = ArrayType(IntegerType) :: structType :: arrayOfStructType :: Nil
    (DataTypeTestUtils.atomicTypes ++ complexTypes ++ Set(NullType)).foreach { dataType =>
      test(s"GenerateOrdering with $dataType") {
        val rowOrdering = InterpretedOrdering.forSchema(Seq(dataType, dataType))
        val genOrdering = GenerateOrdering.generate(
          BoundReference(0, dataType, nullable = true).asc ::
            BoundReference(1, dataType, nullable = true).asc :: Nil)
        val rowType = StructType(
          StructField("a", dataType, nullable = true) ::
            StructField("b", dataType, nullable = true) :: Nil)
        val maybeDataGenerator = RandomDataGenerator.forType(rowType, nullable = false)
        assert(maybeDataGenerator.isDefined)
        val randGenerator = maybeDataGenerator.get
        val toCatalyst = CatalystTypeConverters.createToCatalystConverter(rowType)
        for (_ <- 1 to 50) {
          val a = toCatalyst(randGenerator()).asInstanceOf[InternalRow]
          val b = toCatalyst(randGenerator()).asInstanceOf[InternalRow]
          withClue(s"a = $a, b = $b") {
            assert(genOrdering.compare(a, a) === 0)
            assert(genOrdering.compare(b, b) === 0)
            assert(rowOrdering.compare(a, a) === 0)
            assert(rowOrdering.compare(b, b) === 0)
            assert(signum(genOrdering.compare(a, b)) === -1 * signum(genOrdering.compare(b, a)))
            assert(signum(rowOrdering.compare(a, b)) === -1 * signum(rowOrdering.compare(b, a)))
            assert(
              signum(rowOrdering.compare(a, b)) === signum(genOrdering.compare(a, b)),
              "Generated and non-generated orderings should agree")
          }
        }
      }
    }
  }

  test("SPARK-16845: GeneratedClass$SpecificOrdering grows beyond 64 KiB") {
    val sortOrder = Literal("abc").asc

    // this is passing prior to SPARK-16845, and it should also be passing after SPARK-16845
    GenerateOrdering.generate(Array.fill(40)(sortOrder).toImmutableArraySeq)

    // verify that we can support up to 5000 ordering comparisons, which should be sufficient
    GenerateOrdering.generate(Array.fill(5000)(sortOrder).toImmutableArraySeq)
  }

  test("SPARK-21344: BinaryType comparison does signed byte array comparison") {
    val data = Seq(
      (Array[Byte](1), Array[Byte](-1)),
      (Array[Byte](1, 1, 1, 1, 1), Array[Byte](1, 1, 1, 1, -1)),
      (Array[Byte](1, 1, 1, 1, 1, 1, 1, 1, 1), Array[Byte](1, 1, 1, 1, 1, 1, 1, 1, -1))
      )
    data.foreach { case (b1, b2) =>
      val rowOrdering = InterpretedOrdering.forSchema(Seq(BinaryType))
      val genOrdering = GenerateOrdering.generate(
        BoundReference(0, BinaryType, nullable = true).asc :: Nil)
      val rowType = StructType(StructField("b", BinaryType, nullable = true) :: Nil)
      val toCatalyst = CatalystTypeConverters.createToCatalystConverter(rowType)
      val rowB1 = toCatalyst(Row(b1)).asInstanceOf[InternalRow]
      val rowB2 = toCatalyst(Row(b2)).asInstanceOf[InternalRow]
      assert(rowOrdering.compare(rowB1, rowB2) < 0)
      assert(genOrdering.compare(rowB1, rowB2) < 0)
    }
  }

  test("SPARK-22591: GenerateOrdering shouldn't change ctx.INPUT_ROW") {
    val ctx = new CodegenContext()
    ctx.INPUT_ROW = null

    val schema = new StructType().add("field", FloatType, nullable = true)
    GenerateOrdering.genComparisons(ctx, schema)
    assert(ctx.INPUT_ROW == null)
  }
}
