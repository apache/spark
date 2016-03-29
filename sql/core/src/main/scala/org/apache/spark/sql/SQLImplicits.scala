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

package org.apache.spark.sql

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructField
import org.apache.spark.unsafe.types.UTF8String

/**
 * A collection of implicit methods for converting common Scala objects into [[DataFrame]]s.
 *
 * @since 1.6.0
 */
abstract class SQLImplicits {

  protected def _sqlContext: SQLContext

  /** @since 1.6.0 */
  implicit def newProductEncoder[T <: Product : TypeTag]: Encoder[T] = ExpressionEncoder()

  // Primitives

  /** @since 1.6.0 */
  implicit def newIntEncoder: Encoder[Int] = ExpressionEncoder()

  /** @since 1.6.0 */
  implicit def newLongEncoder: Encoder[Long] = ExpressionEncoder()

  /** @since 1.6.0 */
  implicit def newDoubleEncoder: Encoder[Double] = ExpressionEncoder()

  /** @since 1.6.0 */
  implicit def newFloatEncoder: Encoder[Float] = ExpressionEncoder()

  /** @since 1.6.0 */
  implicit def newByteEncoder: Encoder[Byte] = ExpressionEncoder()

  /** @since 1.6.0 */
  implicit def newShortEncoder: Encoder[Short] = ExpressionEncoder()

  /** @since 1.6.0 */
  implicit def newBooleanEncoder: Encoder[Boolean] = ExpressionEncoder()

  /** @since 1.6.0 */
  implicit def newStringEncoder: Encoder[String] = ExpressionEncoder()

  // Seqs

  /** @since 1.6.1 */
  implicit def newIntSeqEncoder: Encoder[Seq[Int]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newLongSeqEncoder: Encoder[Seq[Long]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newDoubleSeqEncoder: Encoder[Seq[Double]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newFloatSeqEncoder: Encoder[Seq[Float]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newByteSeqEncoder: Encoder[Seq[Byte]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newShortSeqEncoder: Encoder[Seq[Short]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newBooleanSeqEncoder: Encoder[Seq[Boolean]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newStringSeqEncoder: Encoder[Seq[String]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newProductSeqEncoder[A <: Product : TypeTag]: Encoder[Seq[A]] = ExpressionEncoder()

  // Arrays

  /** @since 1.6.1 */
  implicit def newIntArrayEncoder: Encoder[Array[Int]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newLongArrayEncoder: Encoder[Array[Long]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newDoubleArrayEncoder: Encoder[Array[Double]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newFloatArrayEncoder: Encoder[Array[Float]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newByteArrayEncoder: Encoder[Array[Byte]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newShortArrayEncoder: Encoder[Array[Short]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newBooleanArrayEncoder: Encoder[Array[Boolean]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newStringArrayEncoder: Encoder[Array[String]] = ExpressionEncoder()

  /** @since 1.6.1 */
  implicit def newProductArrayEncoder[A <: Product : TypeTag]: Encoder[Array[A]] =
    ExpressionEncoder()

  /**
   * Creates a [[Dataset]] from an RDD.
   * @since 1.6.0
   */
  implicit def rddToDatasetHolder[T : Encoder](rdd: RDD[T]): DatasetHolder[T] = {
    DatasetHolder(_sqlContext.createDataset(rdd))
  }

  /**
   * Creates a [[Dataset]] from a local Seq.
   * @since 1.6.0
   */
  implicit def localSeqToDatasetHolder[T : Encoder](s: Seq[T]): DatasetHolder[T] = {
    DatasetHolder(_sqlContext.createDataset(s))
  }

  /**
   * An implicit conversion that turns a Scala `Symbol` into a [[Column]].
   * @since 1.3.0
   */
  implicit def symbolToColumn(s: Symbol): ColumnName = new ColumnName(s.name)

  /**
   * Creates a DataFrame from an RDD of Product (e.g. case classes, tuples).
   * @since 1.3.0
   */
  implicit def rddToDataFrameHolder[A <: Product : TypeTag](rdd: RDD[A]): DataFrameHolder = {
    DataFrameHolder(_sqlContext.createDataFrame(rdd))
  }

  /**
   * Creates a DataFrame from a local Seq of Product.
   * @since 1.3.0
   */
  implicit def localSeqToDataFrameHolder[A <: Product : TypeTag](data: Seq[A]): DataFrameHolder =
  {
    DataFrameHolder(_sqlContext.createDataFrame(data))
  }

  // Do NOT add more implicit conversions for primitive types.
  // They are likely to break source compatibility by making existing implicit conversions
  // ambiguous. In particular, RDD[Double] is dangerous because of [[DoubleRDDFunctions]].

  /**
   * Creates a single column DataFrame from an RDD[Int].
   * @since 1.3.0
   */
  implicit def intRddToDataFrameHolder(data: RDD[Int]): DataFrameHolder = {
    val dataType = IntegerType
    val rows = data.mapPartitions { iter =>
      val row = new SpecificMutableRow(dataType :: Nil)
      iter.map { v =>
        row.setInt(0, v)
        row: InternalRow
      }
    }
    DataFrameHolder(
      _sqlContext.internalCreateDataFrame(rows, StructType(StructField("_1", dataType) :: Nil)))
  }

  /**
   * Creates a single column DataFrame from an RDD[Long].
   * @since 1.3.0
   */
  implicit def longRddToDataFrameHolder(data: RDD[Long]): DataFrameHolder = {
    val dataType = LongType
    val rows = data.mapPartitions { iter =>
      val row = new SpecificMutableRow(dataType :: Nil)
      iter.map { v =>
        row.setLong(0, v)
        row: InternalRow
      }
    }
    DataFrameHolder(
      _sqlContext.internalCreateDataFrame(rows, StructType(StructField("_1", dataType) :: Nil)))
  }

  /**
   * Creates a single column DataFrame from an RDD[String].
   * @since 1.3.0
   */
  implicit def stringRddToDataFrameHolder(data: RDD[String]): DataFrameHolder = {
    val dataType = StringType
    val rows = data.mapPartitions { iter =>
      val row = new SpecificMutableRow(dataType :: Nil)
      iter.map { v =>
        row.update(0, UTF8String.fromString(v))
        row: InternalRow
      }
    }
    DataFrameHolder(
      _sqlContext.internalCreateDataFrame(rows, StructType(StructField("_1", dataType) :: Nil)))
  }
}
