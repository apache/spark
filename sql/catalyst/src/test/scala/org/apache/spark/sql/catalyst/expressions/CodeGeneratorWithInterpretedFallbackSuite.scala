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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.PlanTestBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType}

class CodeGeneratorWithInterpretedFallbackSuite extends SparkFunSuite with PlanTestBase {

  test("UnsafeProjection with codegen factory mode") {
    val input = Seq(LongType, IntegerType)
      .zipWithIndex.map(x => BoundReference(x._2, x._1, true))

    val codegenOnly = CodegenObjectFactoryMode.CODEGEN_ONLY.toString
    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenOnly) {
      val obj = UnsafeProjection.createObject(input)
      assert(obj.getClass.getName.contains("GeneratedClass$SpecificUnsafeProjection"))
    }

    val noCodegen = CodegenObjectFactoryMode.NO_CODEGEN.toString
    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> noCodegen) {
      val obj = UnsafeProjection.createObject(input)
      assert(obj.isInstanceOf[InterpretedUnsafeProjection])
    }
  }
}
