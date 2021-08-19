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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.NoOp
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeGenerator}
import org.apache.spark.sql.catalyst.plans.PlanTestBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructType}

class CodeGeneratorWithInterpretedFallbackSuite extends SparkFunSuite with PlanTestBase {

  val codegenOnly = CodegenObjectFactoryMode.CODEGEN_ONLY.toString
  val noCodegen = CodegenObjectFactoryMode.NO_CODEGEN.toString

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
    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenOnly) {
      val obj = UnsafeProjection.createObject(input)
      assert(obj.getClass.getName.contains("GeneratedClass$SpecificUnsafeProjection"))
    }

    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> noCodegen) {
      val obj = UnsafeProjection.createObject(input)
      assert(obj.isInstanceOf[InterpretedUnsafeProjection])
    }
  }

  test("MutableProjection with codegen factory mode") {
    val input = Seq(BoundReference(0, IntegerType, nullable = true))
    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenOnly) {
      val obj = MutableProjection.createObject(input)
      assert(obj.getClass.getName.contains("GeneratedClass$SpecificMutableProjection"))
    }

    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> noCodegen) {
      val obj = MutableProjection.createObject(input)
      assert(obj.isInstanceOf[InterpretedMutableProjection])
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
      withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenOnly) {
        FailedCodegenProjection.createObject(input)
      }
    }.getMessage
    assert(errMsg.contains("failed to compile: org.codehaus.commons.compiler.CompileException:"))
  }

  test("SPARK-25358 Correctly handles NoOp in MutableProjection") {
    val exprs = Seq(Add(BoundReference(0, IntegerType, nullable = true), Literal.create(1)), NoOp)
    val input = InternalRow.fromSeq(1 :: 1 :: Nil)
    val expected = 2 :: null :: Nil
    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenOnly) {
      val proj = MutableProjection.createObject(exprs)
      assert(proj(input).toSeq(StructType.fromDDL("c0 int, c1 int")) === expected)
    }

    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> noCodegen) {
      val proj = MutableProjection.createObject(exprs)
      assert(proj(input).toSeq(StructType.fromDDL("c0 int, c1 int")) === expected)
    }
  }

  test("SPARK-25374 Correctly handles NoOp in SafeProjection") {
    val exprs = Seq(Add(BoundReference(0, IntegerType, nullable = true), Literal.create(1)), NoOp)
    val input = InternalRow.fromSeq(1 :: 1 :: Nil)
    val expected = 2 :: null :: Nil
    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenOnly) {
      val proj = SafeProjection.createObject(exprs)
      assert(proj(input).toSeq(StructType.fromDDL("c0 int, c1 int")) === expected)
    }

    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> noCodegen) {
      val proj = SafeProjection.createObject(exprs)
      assert(proj(input).toSeq(StructType.fromDDL("c0 int, c1 int")) === expected)
    }
  }
}
