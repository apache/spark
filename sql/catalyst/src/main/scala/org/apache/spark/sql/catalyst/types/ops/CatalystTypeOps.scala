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

package org.apache.spark.sql.catalyst.types.ops

import org.apache.arrow.vector.ValueVector

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.arrow.ArrowFieldWriter
import org.apache.spark.sql.types.DataType

/**
 * Optional catalyst-layer type operations for the Types Framework.
 *
 * This trait extends TypeOps with operations needed by catalyst-level client infrastructure:
 * serializer/deserializer expression building (SerializerBuildHelper, DeserializerBuildHelper)
 * and Arrow field writer creation (ArrowWriter).
 *
 * USAGE - integration points use CatalystTypeOps(dt) which returns Option[CatalystTypeOps]:
 * {{{
 * // DataType-keyed (ArrowWriter):
 * CatalystTypeOps(dt).map(_.createArrowFieldWriter(vector)).getOrElse { ... }
 *
 * // Encoder-keyed (SerializerBuildHelper): use enc.dataType to get the DataType
 * CatalystTypeOps(enc.dataType).map(_.createSerializer(input)).getOrElse { ... }
 * }}}
 *
 * @see
 *   TimeTypeOps for a reference implementation
 * @since 4.2.0
 */
trait CatalystTypeOps { self: TypeOps =>

  /**
   * Creates a serializer expression that converts an external value to its internal
   * Catalyst representation.
   *
   * Used by SerializerBuildHelper for Dataset[T] serialization.
   *
   * @param input
   *   the input expression representing the external value
   * @return
   *   an Expression that performs the conversion
   */
  def createSerializer(input: Expression): Expression

  /**
   * Creates a deserializer expression that converts an internal Catalyst value
   * to its external representation.
   *
   * Used by DeserializerBuildHelper for Dataset[T] deserialization.
   *
   * @param path
   *   the expression representing the internal value
   * @return
   *   an Expression that performs the conversion
   */
  def createDeserializer(path: Expression): Expression

  /**
   * Creates an ArrowFieldWriter for writing values of this type to an Arrow vector.
   *
   * Used by ArrowWriter for Arrow-based data exchange.
   *
   * @param vector
   *   the Arrow ValueVector to write to (must be the correct vector type for this data type)
   * @return
   *   an ArrowFieldWriter configured for this type
   */
  def createArrowFieldWriter(vector: ValueVector): ArrowFieldWriter
}

/**
 * Factory object for CatalystTypeOps lookup.
 *
 * Delegates to TypeOps and narrows via collect to find implementations that mix in
 * CatalystTypeOps. Returns None if the type is not supported, the framework is disabled,
 * or the type's TypeOps does not implement CatalystTypeOps.
 */
object CatalystTypeOps {

  /**
   * Returns a CatalystTypeOps instance for the given DataType, if available.
   *
   * @param dt
   *   the DataType to get operations for
   * @return
   *   Some(CatalystTypeOps) if supported, None otherwise
   */
  def apply(dt: DataType): Option[CatalystTypeOps] =
    TypeOps(dt).collect { case co: CatalystTypeOps => co }
}
