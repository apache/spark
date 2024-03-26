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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.variant.ParseJson
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.types.variant.VariantBuilder
import org.apache.spark.unsafe.types.VariantVal

class VariantFunctionSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("variant function parse_json - Codegen Support") {
    val df = Seq(("1", """{"a": 1}""")).toDF("key", "vstring").toDF()
    val variantDF = df.select(Column(ParseJson(Column("vstring").expr)))
    val plan = variantDF.queryExecution.executedPlan
    assert(plan.isInstanceOf[WholeStageCodegenExec])
    val v = VariantBuilder.parseJson("""{"a":1}""")
    val expected = new VariantVal(v.getValue, v.getMetadata)
    checkAnswer(variantDF, Seq(Row(expected)))
  }
}
