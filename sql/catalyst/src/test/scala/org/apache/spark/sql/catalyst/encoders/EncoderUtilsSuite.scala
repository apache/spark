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

import org.apache.spark.{SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.objects.{GetExternalRowField, ValidateExternalType}
import org.apache.spark.sql.types.{ObjectType, TimeType}

class EncoderUtilsSuite extends SparkFunSuite {

  test("SPARK-53520: javaBoxedType for TimeType enables correct type validation") {
    // ValidateExternalType uses javaBoxedType(dataType) in its fallback checkType case.
    // Without TimeType in javaBoxedType, it returns java.lang.Object, which makes
    // Object.isInstance(value) always true -- disabling type validation entirely.
    // With the fix, it returns java.lang.Long, so only Long values pass validation.
    val inputObject = BoundReference(0, ObjectType(classOf[Row]), nullable = true)
    val dt = TimeType()
    val validateType = ValidateExternalType(
      GetExternalRowField(inputObject, index = 0, fieldName = "t"),
      dt,
      dt)

    // Valid: Long value (time as microseconds since midnight) should pass
    val validRow = InternalRow.fromSeq(Seq(Row(61000000L)))
    assert(validateType.eval(validRow) === 61000000L)

    // Invalid: String value must be rejected.
    // Without the fix, javaBoxedType returns Object and this would incorrectly pass.
    val invalidRow = InternalRow.fromSeq(Seq(Row("not-a-time")))
    intercept[SparkRuntimeException] {
      validateType.eval(invalidRow)
    }
  }
}
