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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.types._

/** An encoder for primitive Long types. */
case class LongEncoder(fieldName: String = "value", ordinal: Int = 0) extends Encoder[Long] {
  private val row = UnsafeRow.createFromByteArray(64, 1)

  override def clsTag: ClassTag[Long] = ClassTag.Long
  override def schema: StructType =
    StructType(StructField(fieldName, LongType) :: Nil)

  override def fromRow(row: InternalRow): Long = row.getLong(ordinal)

  override def toRow(t: Long): InternalRow = {
    row.setLong(ordinal, t)
    row
  }

  override def bindOrdinals(schema: Seq[Attribute]): Encoder[Long] = this
  override def bind(schema: Seq[Attribute]): Encoder[Long] = this
  override def rebind(oldSchema: Seq[Attribute], newSchema: Seq[Attribute]): Encoder[Long] = this
}

/** An encoder for primitive Integer types. */
case class IntEncoder(fieldName: String = "value", ordinal: Int = 0) extends Encoder[Int] {
  private val row = UnsafeRow.createFromByteArray(64, 1)

  override def clsTag: ClassTag[Int] = ClassTag.Int
  override def schema: StructType =
    StructType(StructField(fieldName, IntegerType) :: Nil)

  override def fromRow(row: InternalRow): Int = row.getInt(ordinal)

  override def toRow(t: Int): InternalRow = {
    row.setInt(ordinal, t)
    row
  }

  override def bindOrdinals(schema: Seq[Attribute]): Encoder[Int] = this
  override def bind(schema: Seq[Attribute]): Encoder[Int] = this
  override def rebind(oldSchema: Seq[Attribute], newSchema: Seq[Attribute]): Encoder[Int] = this
}

/** An encoder for String types. */
case class StringEncoder(
    fieldName: String = "value",
    ordinal: Int = 0) extends Encoder[String] {

  val record = new SpecificMutableRow(StringType :: Nil)

  @transient
  lazy val projection =
    GenerateUnsafeProjection.generate(BoundReference(0, StringType, true) :: Nil)

  override def schema: StructType =
    StructType(
      StructField("value", StringType, nullable = false) :: Nil)

  override def clsTag: ClassTag[String] = scala.reflect.classTag[String]


  override final def fromRow(row: InternalRow): String = {
    row.getString(ordinal)
  }

  override final def toRow(value: String): InternalRow = {
    val utf8String = UTF8String.fromString(value)
    record(0) = utf8String
    // TODO: this is a bit of a hack to produce UnsafeRows
    projection(record)
  }

  override def bindOrdinals(schema: Seq[Attribute]): Encoder[String] = this
  override def bind(schema: Seq[Attribute]): Encoder[String] = this
  override def rebind(oldSchema: Seq[Attribute], newSchema: Seq[Attribute]): Encoder[String] = this
}
