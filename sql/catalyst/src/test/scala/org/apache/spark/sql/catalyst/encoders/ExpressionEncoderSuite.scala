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

import java.util.Arrays

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.ArrayType

abstract class ExpressionEncoderSuite extends SparkFunSuite {
  protected def encodeDecodeTest[T](
      input: T,
      encoder: ExpressionEncoder[T],
      testName: String): Unit = {
    test(s"encode/decode for $testName: $input") {
      val row = encoder.toRow(input)
      val schema = encoder.schema.toAttributes
      val boundEncoder = encoder.resolve(schema).bind(schema)
      val convertedBack = try boundEncoder.fromRow(row) catch {
        case e: Exception =>
          fail(
           s"""Exception thrown while decoding
              |Converted: $row
              |Schema: ${schema.mkString(",")}
              |${encoder.schema.treeString}
              |
              |Encoder:
              |$boundEncoder
              |
            """.stripMargin, e)
      }

      val isCorrect = (input, convertedBack) match {
        case (b1: Array[Byte], b2: Array[Byte]) => Arrays.equals(b1, b2)
        case (b1: Array[Int], b2: Array[Int]) => Arrays.equals(b1, b2)
        case (b1: Array[Array[_]], b2: Array[Array[_]]) =>
          Arrays.deepEquals(b1.asInstanceOf[Array[AnyRef]], b2.asInstanceOf[Array[AnyRef]])
        case (b1: Array[_], b2: Array[_]) =>
          Arrays.equals(b1.asInstanceOf[Array[AnyRef]], b2.asInstanceOf[Array[AnyRef]])
        case _ => input == convertedBack
      }

      if (!isCorrect) {
        val types = convertedBack match {
          case c: Product =>
            c.productIterator.filter(_ != null).map(_.getClass.getName).mkString(",")
          case other => other.getClass.getName
        }

        val encodedData = try {
          row.toSeq(encoder.schema).zip(schema).map {
            case (a: ArrayData, AttributeReference(_, ArrayType(et, _), _, _)) =>
              a.toArray[Any](et).toSeq
            case (other, _) =>
              other
          }.mkString("[", ",", "]")
        } catch {
          case e: Throwable => s"Failed to toSeq: $e"
        }

        fail(
          s"""Encoded/Decoded data does not match input data
             |
             |in:  $input
             |out: $convertedBack
             |types: $types
             |
             |Encoded Data: $encodedData
             |Schema: ${schema.mkString(",")}
             |${encoder.schema.treeString}
             |
             |fromRow Expressions:
             |${boundEncoder.fromRowExpression.treeString}
         """.stripMargin)
      }
    }
  }
}
