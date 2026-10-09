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
package org.apache.spark.sql.execution.datasources.v2.ffi

import java.util.Base64

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode

import org.apache.spark.sql.connector.expressions.{Expression, GeneralScalarExpression, Literal, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.types._

/**
 * Encodes Data Source V2 predicates as the JSON expression trees passed to
 * `NativeBridge.pushPredicates`.
 */
object NativePredicates {
  private val mapper = new ObjectMapper()

  /** Returns the JSON of the predicate, or None if it cannot be expressed in JSON. */
  def toJson(predicate: Predicate): Option[String] = {
    encode(predicate).map(mapper.writeValueAsString)
  }

  private def encode(expression: Expression): Option[ObjectNode] = expression match {
    case reference: NamedReference =>
      val node = mapper.createObjectNode().put("type", "column")
      val name = node.putArray("name")
      reference.fieldNames().foreach(name.add)
      Some(node)

    case literal: Literal[_] =>
      encodeLiteral(literal.value(), literal.dataType())

    case function: GeneralScalarExpression =>
      val children = function.children().map(encode)
      if (children.forall(_.isDefined)) {
        val node = mapper.createObjectNode()
          .put("type", "function")
          .put("name", function.name())
        val array = node.putArray("children")
        children.foreach(child => array.add(child.get))
        Some(node)
      } else {
        None
      }

    case _ => None
  }

  private def encodeLiteral(value: Any, dataType: DataType): Option[ObjectNode] = {
    val node = mapper.createObjectNode()
      .put("type", "literal")
      .put("dataType", dataType.catalogString)
    if (value == null) {
      Some(node.putNull("value"))
    } else {
      val encoded = (value, dataType) match {
        case (b: Boolean, BooleanType) => Some(node.put("value", b))
        case (n: Byte, ByteType) => Some(node.put("value", n.toInt))
        case (n: Short, ShortType) => Some(node.put("value", n))
        case (n: Int, IntegerType | DateType) => Some(node.put("value", n))
        case (n: Long, LongType | TimestampType | TimestampNTZType) => Some(node.put("value", n))
        case (n: Float, FloatType) if !n.isNaN && !n.isInfinite => Some(node.put("value", n))
        case (n: Double, DoubleType) if !n.isNaN && !n.isInfinite => Some(node.put("value", n))
        // A source compares strings by their bytes, so only push down binary collations.
        case (s, st: StringType) if st.isUTF8BinaryCollation => Some(node.put("value", s.toString))
        case (bytes: Array[Byte], BinaryType) =>
          Some(node.put("value", Base64.getEncoder.encodeToString(bytes)))
        case (d: Decimal, _: DecimalType) =>
          Some(node.put("value", d.toJavaBigDecimal.toPlainString))
        case _ => None
      }
      encoded
    }
  }
}
