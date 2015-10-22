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

package org.apache.spark.sql.catalyst.encoders


import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.{StructField, StructType}

// Most of this file is codegen.
// scalastyle:off

/**
 * A set of composite encoders that take sub encoders and map each of their objects to a
 * Scala tuple.  Note that currently the implementation is fairly limited and only supports going
 * from an internal row to a tuple.
 */
object TupleEncoder {

  /** Code generator for composite tuple encoders. */
  def main(args: Array[String]): Unit = {
    (2 to 5).foreach { i =>
      val types = (1 to i).map(t => s"T$t").mkString(", ")
      val tupleType = s"($types)"
      val args = (1 to i).map(t => s"e$t: Encoder[T$t]").mkString(", ")
      val fields = (1 to i).map(t => s"""StructField("_$t", e$t.schema)""").mkString(", ")
      val fromRow = (1 to i).map(t => s"e$t.fromRow(row)").mkString(", ")

      println(
        s"""
          |class Tuple${i}Encoder[$types]($args) extends Encoder[$tupleType] {
          |  val schema = StructType(Array($fields))
          |
          |  def clsTag: ClassTag[$tupleType] = scala.reflect.classTag[$tupleType]
          |
          |  def fromRow(row: InternalRow): $tupleType = {
          |    ($fromRow)
          |  }
          |
          |  override def toRow(t: $tupleType): InternalRow =
          |    throw new UnsupportedOperationException("Tuple Encoders only support fromRow.")
          |
          |  override def bind(schema: Seq[Attribute]): Encoder[$tupleType] = {
          |    this
          |  }
          |
          |  override def rebind(oldSchema: Seq[Attribute], newSchema: Seq[Attribute]): Encoder[$tupleType] =
          |    throw new UnsupportedOperationException("Tuple Encoders only support bind.")
          |
          |
          |  override def bindOrdinals(schema: Seq[Attribute]): Encoder[$tupleType] =
          |    throw new UnsupportedOperationException("Tuple Encoders only support bind.")
          |}
        """.stripMargin)
    }
  }
}

class Tuple2Encoder[T1, T2](e1: Encoder[T1], e2: Encoder[T2]) extends Encoder[(T1, T2)] {
  val schema = StructType(Array(StructField("_1", e1.schema), StructField("_2", e2.schema)))

  def clsTag: ClassTag[(T1, T2)] = scala.reflect.classTag[(T1, T2)]

  def fromRow(row: InternalRow): (T1, T2) = {
    (e1.fromRow(row), e2.fromRow(row))
  }

  override def toRow(t: (T1, T2)): InternalRow =
    throw new UnsupportedOperationException("Tuple Encoders only support fromRow.")

  override def bind(schema: Seq[Attribute]): Encoder[(T1, T2)] = {
    this
  }

  override def rebind(oldSchema: Seq[Attribute], newSchema: Seq[Attribute]): Encoder[(T1, T2)] =
    throw new UnsupportedOperationException("Tuple Encoders only support bind.")


  override def bindOrdinals(schema: Seq[Attribute]): Encoder[(T1, T2)] =
    throw new UnsupportedOperationException("Tuple Encoders only support bind.")
}


class Tuple3Encoder[T1, T2, T3](e1: Encoder[T1], e2: Encoder[T2], e3: Encoder[T3]) extends Encoder[(T1, T2, T3)] {
  val schema = StructType(Array(StructField("_1", e1.schema), StructField("_2", e2.schema), StructField("_3", e3.schema)))

  def clsTag: ClassTag[(T1, T2, T3)] = scala.reflect.classTag[(T1, T2, T3)]

  def fromRow(row: InternalRow): (T1, T2, T3) = {
    (e1.fromRow(row), e2.fromRow(row), e3.fromRow(row))
  }

  override def toRow(t: (T1, T2, T3)): InternalRow =
    throw new UnsupportedOperationException("Tuple Encoders only support fromRow.")

  override def bind(schema: Seq[Attribute]): Encoder[(T1, T2, T3)] = {
    this
  }

  override def rebind(oldSchema: Seq[Attribute], newSchema: Seq[Attribute]): Encoder[(T1, T2, T3)] =
    throw new UnsupportedOperationException("Tuple Encoders only support bind.")


  override def bindOrdinals(schema: Seq[Attribute]): Encoder[(T1, T2, T3)] =
    throw new UnsupportedOperationException("Tuple Encoders only support bind.")
}


class Tuple4Encoder[T1, T2, T3, T4](e1: Encoder[T1], e2: Encoder[T2], e3: Encoder[T3], e4: Encoder[T4]) extends Encoder[(T1, T2, T3, T4)] {
  val schema = StructType(Array(StructField("_1", e1.schema), StructField("_2", e2.schema), StructField("_3", e3.schema), StructField("_4", e4.schema)))

  def clsTag: ClassTag[(T1, T2, T3, T4)] = scala.reflect.classTag[(T1, T2, T3, T4)]

  def fromRow(row: InternalRow): (T1, T2, T3, T4) = {
    (e1.fromRow(row), e2.fromRow(row), e3.fromRow(row), e4.fromRow(row))
  }

  override def toRow(t: (T1, T2, T3, T4)): InternalRow =
    throw new UnsupportedOperationException("Tuple Encoders only support fromRow.")

  override def bind(schema: Seq[Attribute]): Encoder[(T1, T2, T3, T4)] = {
    this
  }

  override def rebind(oldSchema: Seq[Attribute], newSchema: Seq[Attribute]): Encoder[(T1, T2, T3, T4)] =
    throw new UnsupportedOperationException("Tuple Encoders only support bind.")


  override def bindOrdinals(schema: Seq[Attribute]): Encoder[(T1, T2, T3, T4)] =
    throw new UnsupportedOperationException("Tuple Encoders only support bind.")
}


class Tuple5Encoder[T1, T2, T3, T4, T5](e1: Encoder[T1], e2: Encoder[T2], e3: Encoder[T3], e4: Encoder[T4], e5: Encoder[T5]) extends Encoder[(T1, T2, T3, T4, T5)] {
  val schema = StructType(Array(StructField("_1", e1.schema), StructField("_2", e2.schema), StructField("_3", e3.schema), StructField("_4", e4.schema), StructField("_5", e5.schema)))

  def clsTag: ClassTag[(T1, T2, T3, T4, T5)] = scala.reflect.classTag[(T1, T2, T3, T4, T5)]

  def fromRow(row: InternalRow): (T1, T2, T3, T4, T5) = {
    (e1.fromRow(row), e2.fromRow(row), e3.fromRow(row), e4.fromRow(row), e5.fromRow(row))
  }

  override def toRow(t: (T1, T2, T3, T4, T5)): InternalRow =
    throw new UnsupportedOperationException("Tuple Encoders only support fromRow.")

  override def bind(schema: Seq[Attribute]): Encoder[(T1, T2, T3, T4, T5)] = {
    this
  }

  override def rebind(oldSchema: Seq[Attribute], newSchema: Seq[Attribute]): Encoder[(T1, T2, T3, T4, T5)] =
    throw new UnsupportedOperationException("Tuple Encoders only support bind.")


  override def bindOrdinals(schema: Seq[Attribute]): Encoder[(T1, T2, T3, T4, T5)] =
    throw new UnsupportedOperationException("Tuple Encoders only support bind.")
}
