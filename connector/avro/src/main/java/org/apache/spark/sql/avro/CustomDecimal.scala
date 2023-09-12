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

package org.apache.spark.sql.avro

import org.apache.avro.LogicalType
import org.apache.avro.Schema

import org.apache.spark.sql.types.DecimalType

private[spark] object CustomDecimal {
  val TYPE_NAME = "custom-decimal"
}

// A customized logical type, which will be registered to Avro. This logical type is similar to
// Avro's builtin Decimal type, but is meant to be registered for long type. It indicates that
// the long type should be converted to Spark's Decimal type, with provided precision and scale.
private[spark] class CustomDecimal(schema: Schema) extends LogicalType(CustomDecimal.TYPE_NAME) {
  val scale : Int = {
    val obj = schema.getObjectProp("scale")
    obj match {
      case null =>
        throw new IllegalArgumentException(s"Invalid ${CustomDecimal.TYPE_NAME}: missing scale");
      case i : Integer =>
        i
      case other =>
        throw new IllegalArgumentException(s"Expected int ${CustomDecimal.TYPE_NAME}:scale")
    }
  }
  val precision : Int = {
    val obj = schema.getObjectProp("precision")
    obj match {
      case null =>
        throw new IllegalArgumentException(
          s"Invalid ${CustomDecimal.TYPE_NAME}: missing precision");
      case i: Integer =>
        i
      case other =>
        throw new IllegalArgumentException(s"Expected int ${CustomDecimal.TYPE_NAME}:precision")
    }
  }
  val className : String = schema.getProp("className")

  override def validate(schema: Schema): Unit = {
    super.validate(schema)
    if (schema.getType != Schema.Type.LONG) {
      throw new IllegalArgumentException(
        s"${CustomDecimal.TYPE_NAME} can only be used with an underlying long type")
    }
    if (precision <= 0) {
      throw new IllegalArgumentException(s"Invalid decimal precision: $precision" +
        " (must be positive)");
    } else if (precision > DecimalType.MAX_PRECISION) {
      throw new IllegalArgumentException(
        s"cannot store $precision digits (max ${DecimalType.MAX_PRECISION})")
    }
    if (scale < 0) {
      throw new IllegalArgumentException(s"Invalid decimal scale: $scale" +
        " (must be positive)");
    } else if (scale > precision) {
      throw new IllegalArgumentException(s"Invalid decimal scale: $scale (greater than " +
        s"precision: $precision)");
    }
  }
}
