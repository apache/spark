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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * Used to convert a JVM object of type `T` to and from the internal Spark SQL representation.
 *
 * Encoders are not intended to be thread-safe and thus they are allow to avoid internal locking
 * and reuse internal buffers to improve performance.
 */
trait Encoder[T] {
  /** Returns the schema of encoding this type of object as a Row. */
  def schema: StructType

  /** A ClassTag that can be used to construct and Array to contain a collection of `T`. */
  def clsTag: ClassTag[T]

  /**
   * Returns an encoded version of `t` as a Spark SQL row.  Note that multiple calls to
   * toRow are allowed to return the same actual [[InternalRow]] object.  Thus, the caller should
   * copy the result before making another call if required.
   */
  def toRow(t: T): InternalRow

  /**
   * Returns an object of type `T`, extracting the required values from the provided row.  Note that
   * you must bind` and encoder to a specific schema before you can call this function.
   */
  def fromRow(row: InternalRow): T

  /**
   * Returns a new copy of this encoder, where the expressions used by `fromRow` are bound to the
   * given schema
   */
  def bind(schema: Seq[Attribute]): Encoder[T]
}
