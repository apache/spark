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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

case class J[L, R](left: L, right: R) extends Product2[L, R] {
  override def _1: L = left
  override def _2: R = right
}

class JoinedEncoder[T, U](left: Encoder[T], right: Encoder[U]) extends Encoder[J[T, U]] {
  val schema = StructType(left.schema.fields ++ right.schema.fields)
  def clsTag = scala.reflect.classTag[J[T, U]]

  def fromRow(row: InternalRow): J[T, U] = {
    J(left.fromRow(row), right.fromRow(row))
  }

  override def toRow(t: J[T, U]): InternalRow = ???

  override def bind(schema: Seq[Attribute]): Encoder[J[T, U]] = {
    val leftSchema = schema.take(left.schema.size)
    val rightSchema = schema.drop(left.schema.size)
    new JoinedEncoder[T, U](left.bind(leftSchema), right.bind(rightSchema))
  }

  override def rebind(oldSchema: Seq[Attribute], newSchema: Seq[Attribute]): ClassEncoder[J[T, U]] = ???

  override def bindOrdinals(schema: Seq[Attribute]): ClassEncoder[J[T, U]] = ???
}
