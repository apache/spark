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

import java.math.BigInteger

import scala.beans.BeanProperty

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{CheckOverflow, Expression, Literal}
import org.apache.spark.sql.types.DecimalType

class DummyBean() {
  @BeanProperty var bigInteger = null: BigInteger
}

class JavaTypeInferenceSuite extends SparkFunSuite {

  test("SPARK-41007: JavaTypeInference returns the correct serializer for BigInteger") {
    var serializer = JavaTypeInference.serializerFor(classOf[DummyBean])
    var bigIntegerFieldName: Expression = serializer.children(0)
    assert(bigIntegerFieldName.asInstanceOf[Literal].value.toString == "bigInteger")
    var bigIntegerFieldExpression: Expression = serializer.children(1)
    assert(bigIntegerFieldExpression.asInstanceOf[CheckOverflow].dataType ==
      DecimalType.BigIntDecimal)
  }
}
