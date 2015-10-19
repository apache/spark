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

package org.apache.spark.sql.catalyst

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class CatalystTypeConvertersSuite extends SparkFunSuite {

  private val simpleTypes: Seq[DataType] = Seq(
    StringType,
    DateType,
    BooleanType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType.SYSTEM_DEFAULT,
    DecimalType.USER_DEFAULT)

  test("null handling in rows") {
    val schema = StructType(simpleTypes.map(t => StructField(t.getClass.getName, t)))
    val convertToCatalyst = CatalystTypeConverters.createToCatalystConverter(schema)
    val convertToScala = CatalystTypeConverters.createToScalaConverter(schema)

    val scalaRow = Row.fromSeq(Seq.fill(simpleTypes.length)(null))
    assert(convertToScala(convertToCatalyst(scalaRow)) === scalaRow)
  }

  test("null handling for individual values") {
    for (dataType <- simpleTypes) {
      assert(CatalystTypeConverters.createToScalaConverter(dataType)(null) === null)
    }
  }

  test("option handling in convertToCatalyst") {
    // convertToCatalyst doesn't handle unboxing from Options. This is inconsistent with
    // createToCatalystConverter but it may not actually matter as this is only called internally
    // in a handful of places where we don't expect to receive Options.
    assert(CatalystTypeConverters.convertToCatalyst(Some(123)) === Some(123))
  }

  test("option handling in createToCatalystConverter") {
    assert(CatalystTypeConverters.createToCatalystConverter(IntegerType)(Some(123)) === 123)
  }
}
