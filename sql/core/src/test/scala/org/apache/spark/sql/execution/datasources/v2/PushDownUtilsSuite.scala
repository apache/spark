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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.{SparkFunSuite, SparkNumberFormatException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Cast, EqualTo, EvalMode, Literal}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.internal.connector.PartitionPredicateField
import org.apache.spark.sql.types.{IntegerType, StringType, StructField}
import org.apache.spark.unsafe.types.UTF8String

class PushDownUtilsSuite extends SparkFunSuite {

  test("SPARK-59572: only a runtime partition predicate keeps a key it cannot evaluate") {
    val ref = DataTypeUtils.toAttribute(StructField("p", StringType, nullable = true))
    val fields = Seq(PartitionPredicateField(Seq("p"), Some(ref)))
    // An ANSI cast of a non-numeric string throws when evaluated.
    val filter = EqualTo(Cast(ref, IntegerType, None, EvalMode.ANSI), Literal(1))
    val failing = InternalRow(UTF8String.fromString("hr"))

    // A runtime filter only prunes, so a partition it cannot evaluate is kept.
    val runtime = PushDownUtils.createRuntimePartitionPredicates(Seq(filter), fields)
    assert(runtime.size === 1)
    assert(runtime.head.eval(failing) === true)

    // Everywhere else Spark drops the filter the connector accepts, so the failure must surface.
    val (pushed, _) = PushDownUtils.createPartitionPredicates(Seq(filter), fields)
    assert(pushed.size === 1)
    intercept[SparkNumberFormatException](pushed.head.eval(failing))
  }
}
