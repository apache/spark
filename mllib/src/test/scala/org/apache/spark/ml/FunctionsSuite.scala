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

package org.apache.spark.ml

import org.apache.spark.SparkException
import org.apache.spark.ml.functions._
import org.apache.spark.ml.linalg.{Matrices, MatrixUDT, Vector, Vectors, VectorUDT}
import org.apache.spark.ml.util.MLTest
import org.apache.spark.mllib.linalg.{Matrices => OldMatrices, MatrixUDT => OldMatrixUDT,
  Vector => OldVector, Vectors => OldVectors, VectorUDT => OldVectorUDT}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.functions.{col, unwrap_udt, wrap_udt}
import org.apache.spark.sql.types.{StructField, StructType, UserDefinedType}

class FunctionsSuite extends MLTest {

  import testImplicits._

  private def checkWrapUDTConversion(
      df: DataFrame,
      targetUDT: UserDefinedType[_],
      expected: Any): Unit = {
    val converted = df.select(wrap_udt(unwrap_udt(col("value")), targetUDT).as("value"))
    assert(converted.schema("value").dataType === targetUDT)
    assert(converted.first().get(0) === expected)
  }

  private def checkWrapUDTTypeMismatch(df: DataFrame, targetUDT: UserDefinedType[_]): Unit = {
    val e = intercept[AnalysisException] {
      df.select(wrap_udt(unwrap_udt(col("value")), targetUDT).as("value")).collect()
    }
    assert(e.getCondition === "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
  }

  private def checkNullableWrapUDTConversion(
      value: Any,
      sourceUDT: UserDefinedType[_],
      targetUDT: UserDefinedType[_],
      expected: Any): Unit = {
    val schema = StructType(Seq(StructField("value", sourceUDT, nullable = true)))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(value), Row(null))),
      schema)
    val converted = df.select(wrap_udt(unwrap_udt(col("value")), targetUDT).as("value"))

    assert(converted.schema("value").dataType === targetUDT)
    assert(converted.schema("value").nullable)
    assert(converted.collect().map(_.get(0)).toSeq === Seq(expected, null))
  }

  private def normalizeNaN(rows: Seq[(Int, Int, Double)]): Seq[(Int, Int, String)] = {
    rows.map {
      case (id, index, value) if value.isNaN => (id, index, "NaN")
      case (id, index, value) => (id, index, value.toString)
    }
  }

  test("test vector_to_array") {
    val df = Seq(
      (Vectors.dense(1.0, 2.0, 3.0), OldVectors.dense(10.0, 20.0, 30.0)),
      (Vectors.sparse(3, Seq((0, 2.0), (2, 3.0))), OldVectors.sparse(3, Seq((0, 20.0), (2, 30.0))))
    ).toDF("vec", "oldVec")

    val result = df.select(vector_to_array($"vec"), vector_to_array($"oldVec"))
                   .as[(Seq[Double], Seq[Double])].collect().toSeq

    val expected = Seq(
      (Seq(1.0, 2.0, 3.0), Seq(10.0, 20.0, 30.0)),
      (Seq(2.0, 0.0, 3.0), Seq(20.0, 0.0, 30.0))
    )
    assert(result === expected)

    val df2 = Seq(
      (Vectors.dense(1.0, 2.0, 3.0),
       OldVectors.dense(10.0, 20.0, 30.0), 1),
      (null, null, 0)
    ).toDF("vec", "oldVec", "label")

    for ((colName, valType) <- Seq(
        ("vec", "null"), ("oldVec", "null"), ("label", "java.lang.Integer"))) {
      val thrown1 = intercept[SparkException] {
        df2.select(vector_to_array(col(colName))).count()
      }
      assert(thrown1.getCause.getMessage.contains(
        "function vector_to_array requires a non-null input argument and input type must be " +
        "`org.apache.spark.ml.linalg.Vector` or `org.apache.spark.mllib.linalg.Vector`, " +
        s"but got ${valType}"))
    }

    val df3 = Seq(
      (Vectors.dense(1.0, 2.0, 3.0), OldVectors.dense(10.0, 20.0, 30.0)),
      (Vectors.sparse(3, Seq((0, 2.0), (2, 3.0))), OldVectors.sparse(3, Seq((0, 20.0), (2, 30.0))))
    ).toDF("vec", "oldVec")
    val dfArrayFloat = df3.select(
      vector_to_array($"vec", dtype = "float32"), vector_to_array($"oldVec", dtype = "float32"))

    // Check values are correct
    val result3 = dfArrayFloat.as[(Seq[Float], Seq[Float])].collect().toSeq

    val expected3 = Seq(
      (Seq(1.0, 2.0, 3.0), Seq(10.0, 20.0, 30.0)),
      (Seq(2.0, 0.0, 3.0), Seq(20.0, 0.0, 30.0))
    )
    assert(result3 === expected3)

    // Check data types are correct
    assert(dfArrayFloat.schema.simpleString ===
      "struct<UDF(vec):array<float>,UDF(oldVec):array<float>>")

    val thrown2 = intercept[AnalysisException] {
      df3.select(
        vector_to_array($"vec", dtype = "float16"), vector_to_array($"oldVec", dtype = "float16"))
    }
    assert(thrown2.getMessage.contains(
      "Unsupported dtype: \"float16\". Valid values: float64, float32."))
  }

  test("test array_to_vector") {
    val df1 = Seq(Tuple1(Array(0.5, 1.5))).toDF("c1")
    val resultVec = df1.select(array_to_vector(col("c1"))).collect()(0)(0).asInstanceOf[Vector]
    assert(resultVec === Vectors.dense(Array(0.5, 1.5)))

    val df2 = Seq(Tuple1(Array(1.5f, 2.5f))).toDF("c1")
    val resultVec2 = df2.select(array_to_vector(col("c1"))).collect()(0)(0).asInstanceOf[Vector]
    assert(resultVec2 === Vectors.dense(Array(1.5, 2.5)))

    val df3 = Seq(Tuple1(Array(1, 2))).toDF("c1")
    val resultVec3 = df3.select(array_to_vector(col("c1"))).collect()(0)(0).asInstanceOf[Vector]
    assert(resultVec3 === Vectors.dense(Array(1.0, 2.0)))
  }

  test("test vector_posexplode with vector UDT") {
    val df = Seq(
      (0, Vectors.dense(1.0, 0.0, 3.0), OldVectors.dense(10.0, 0.0, 30.0)),
      (1, Vectors.sparse(4, Seq((1, 2.0), (2, 0.0), (3, 4.0))),
        OldVectors.sparse(4, Seq((0, 20.0), (1, 0.0), (2, 30.0)))),
      (2, null.asInstanceOf[Vector], null.asInstanceOf[OldVector]),
      (3, Vectors.sparse(10, Array.emptyIntArray, Array.emptyDoubleArray),
        OldVectors.sparse(10, Array.emptyIntArray, Array.emptyDoubleArray)),
      (4, Vectors.dense(Array.emptyDoubleArray),
        OldVectors.dense(Array.emptyDoubleArray))
    ).toDF("id", "vec", "oldVec")

    val result = df.select($"id", vector_posexplode($"vec"))
      .as[(Int, Int, Double)]
      .collect()
      .toSeq
    assert(normalizeNaN(result) === Seq(
      (0, -4, "NaN"),
      (0, 0, "1.0"),
      (0, 2, "3.0"),
      (1, -5, "NaN"),
      (1, 1, "2.0"),
      (1, 3, "4.0"),
      (3, -11, "NaN"),
      (4, -1, "NaN")))

    val oldResult = df.select($"id", vector_posexplode($"oldVec"))
      .as[(Int, Int, Double)]
      .collect()
      .toSeq
    assert(normalizeNaN(oldResult) === Seq(
      (0, -4, "NaN"),
      (0, 0, "10.0"),
      (0, 2, "30.0"),
      (1, -5, "NaN"),
      (1, 0, "20.0"),
      (1, 2, "30.0"),
      (3, -11, "NaN"),
      (4, -1, "NaN")))

    val denseResult = df
      .where($"id" === 1)
      .select($"id", vector_posexplode($"vec", mode = "dense"))
      .as[(Int, Int, Double)]
      .collect()
      .toSeq
    assert(normalizeNaN(denseResult) === Seq(
      (1, -5, "NaN"),
      (1, 0, "0.0"),
      (1, 1, "2.0"),
      (1, 2, "0.0"),
      (1, 3, "4.0")))

    val sparseResult = df.select($"id", vector_posexplode($"vec", mode = "sparse"))
      .as[(Int, Int, Double)]
      .collect()
      .toSeq
    assert(normalizeNaN(sparseResult) === Seq(
      (0, -4, "NaN"),
      (0, 0, "1.0"),
      (0, 2, "3.0"),
      (1, -5, "NaN"),
      (1, 1, "2.0"),
      (1, 3, "4.0"),
      (3, -11, "NaN"),
      (4, -1, "NaN")))

    val schema = df.select(vector_posexplode($"vec")).schema
    assert(schema.simpleString === "struct<index:int,value:double>")
  }

  test("test get_vector") {
    val df = Seq(
      (Vectors.dense(1.0, 2.0, 3.0), 0),
      (Vectors.dense(1.0, 2.0, 3.0), 1),
      (Vectors.dense(1.0, 2.0, 3.0), 2),
      (Vectors.sparse(3, Seq((0, -1.0))), 0),
      (Vectors.sparse(3, Seq((0, -1.0))), 1),
      (Vectors.sparse(3, Seq((0, -1.0))), 2)
    ).toDF("vec", "idx")

    val result = df.select(vector_get(col("vec"), col("idx"))).as[Double].collect()
    assert(result === Array(1.0, 2.0, 3.0, -1.0, 0.0, 0.0))
  }

  test("test array_argmax") {
    val df = Seq(
      Tuple1.apply(Array(1.0, 2.0, 3.0)),
      Tuple1.apply(Array(1.0, 3.0, 2.0)),
      Tuple1.apply(Array(3.0, 2.0, 1.0)),
      Tuple1.apply(Array(1.0, 3.0, 3.0)),
      Tuple1.apply(Array(3.0, 3.0, 3.0)),
      Tuple1.apply(Array.emptyDoubleArray)
    ).toDF("arr")

    val result = df.select(array_argmax(col("arr"))).as[Int].collect()
    assert(result === Array(2, 1, 0, 1, 0, -1))
  }

  test("wrap and unwrap vector and matrix UDT columns") {
    val oldVector = OldVectors.sparse(3, Array(1), Array(2.0))
    val oldVectorDF = Seq(Tuple1(oldVector)).toDF("value")
    checkWrapUDTConversion(
      oldVectorDF,
      new OldVectorUDT,
      oldVector)
    checkWrapUDTConversion(
      oldVectorDF,
      new VectorUDT,
      oldVector.asML)
    checkWrapUDTTypeMismatch(
      oldVectorDF,
      new OldMatrixUDT)
    checkWrapUDTTypeMismatch(
      oldVectorDF,
      new MatrixUDT)

    val mlVector = Vectors.dense(1.0, 2.0)
    val mlVectorDF = Seq(Tuple1(mlVector)).toDF("value")
    checkWrapUDTConversion(
      mlVectorDF,
      new VectorUDT,
      mlVector)
    checkWrapUDTConversion(
      mlVectorDF,
      new OldVectorUDT,
      OldVectors.fromML(mlVector))
    checkWrapUDTTypeMismatch(
      mlVectorDF,
      new OldMatrixUDT)
    checkWrapUDTTypeMismatch(
      mlVectorDF,
      new MatrixUDT)

    val oldMatrix = OldMatrices.dense(2, 2, Array(1.0, 2.0, 3.0, 4.0))
    val oldMatrixDF = Seq(Tuple1(oldMatrix)).toDF("value")
    checkWrapUDTConversion(
      oldMatrixDF,
      new OldMatrixUDT,
      oldMatrix)
    checkWrapUDTConversion(
      oldMatrixDF,
      new MatrixUDT,
      oldMatrix.asML)
    checkWrapUDTTypeMismatch(
      oldMatrixDF,
      new OldVectorUDT)
    checkWrapUDTTypeMismatch(
      oldMatrixDF,
      new VectorUDT)

    val mlMatrix = Matrices.dense(2, 2, Array(1.0, 2.0, 3.0, 4.0))
    val mlMatrixDF = Seq(Tuple1(mlMatrix)).toDF("value")
    checkWrapUDTConversion(
      mlMatrixDF,
      new MatrixUDT,
      mlMatrix)
    checkWrapUDTConversion(
      mlMatrixDF,
      new OldMatrixUDT,
      OldMatrices.fromML(mlMatrix))
    checkWrapUDTTypeMismatch(
      mlMatrixDF,
      new OldVectorUDT)
    checkWrapUDTTypeMismatch(
      mlMatrixDF,
      new VectorUDT)
  }

  test("wrap and unwrap nullable vector UDT columns") {
    val oldVector = OldVectors.sparse(3, Array(1), Array(2.0))
    checkNullableWrapUDTConversion(
      oldVector,
      new OldVectorUDT,
      new VectorUDT,
      oldVector.asML)

    val mlVector = Vectors.dense(1.0, 2.0)
    checkNullableWrapUDTConversion(
      mlVector,
      new VectorUDT,
      new OldVectorUDT,
      OldVectors.fromML(mlVector))
  }
}
