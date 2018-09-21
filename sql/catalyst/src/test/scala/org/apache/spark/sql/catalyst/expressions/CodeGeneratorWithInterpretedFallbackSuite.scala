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

import java.util.concurrent.ExecutionException

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeGenerator}
import org.apache.spark.sql.catalyst.plans.PlanTestBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

class CodeGeneratorWithInterpretedFallbackSuite extends SparkFunSuite with PlanTestBase {

  object FailedCodegenProjection
      extends CodeGeneratorWithInterpretedFallback[Seq[Expression], UnsafeProjection] {

    override protected def createCodeGeneratedObject(in: Seq[Expression]): UnsafeProjection = {
      val invalidCode = new CodeAndComment("invalid code", Map.empty)
      // We assume this compilation throws an exception
      CodeGenerator.compile(invalidCode)
      null
    }

    override protected def createInterpretedObject(in: Seq[Expression]): UnsafeProjection = {
      InterpretedUnsafeProjection.createProjection(in)
    }
  }

  test("UnsafeProjection with codegen factory mode") {
    val input = Seq(BoundReference(0, IntegerType, nullable = true))
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

  test("fallback to the interpreter mode") {
    val input = Seq(BoundReference(0, IntegerType, nullable = true))
    val fallback = CodegenObjectFactoryMode.FALLBACK.toString
    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> fallback) {
      val obj = FailedCodegenProjection.createObject(input)
      assert(obj.isInstanceOf[InterpretedUnsafeProjection])
    }
  }

  test("codegen failures in the CODEGEN_ONLY mode") {
    val errMsg = intercept[ExecutionException] {
      val input = Seq(BoundReference(0, IntegerType, nullable = true))
      val codegenOnly = CodegenObjectFactoryMode.CODEGEN_ONLY.toString
      withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenOnly) {
        FailedCodegenProjection.createObject(input)
      }
    }.getMessage
    assert(errMsg.contains("failed to compile: org.codehaus.commons.compiler.CompileException:"))
  }
}
