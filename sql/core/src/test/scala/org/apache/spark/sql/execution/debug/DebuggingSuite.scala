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

package org.apache.spark.sql.execution.debug

import java.io.ByteArrayOutputStream

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.execution.{CodegenSupport, LeafExecNode, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.test.SQLTestData.TestData
import org.apache.spark.sql.types.StructType

abstract class DebuggingSuiteBase extends SharedSparkSession {

  test("DataFrame.debug()") {
    testData.debug()
  }

  test("Dataset.debug()") {
    import testImplicits._
    testData.as[TestData].debug()
  }

  test("debugCodegen") {
    val df = spark.range(10).groupBy(col("id") * 2).count()
    df.collect()
    val res = codegenString(df.queryExecution.executedPlan)
    assert(res.contains("Subtree 1 / 2"))
    assert(res.contains("Subtree 2 / 2"))
    assert(res.contains("Object[]"))
  }

  test("debugCodegenStringSeq") {
    val df = spark.range(10).groupBy(col("id") * 2).count()
    df.collect()
    val res = codegenStringSeq(df.queryExecution.executedPlan)
    assert(res.length == 2)
    assert(res.forall{ case (subtree, code, _) =>
      subtree.contains("Range") && code.contains("Object[]")})
  }

  case class DummyCodeGeneratorPlan(useInnerClass: Boolean)
      extends CodegenSupport with LeafExecNode {
    override def output: Seq[Attribute] = toAttributes(StructType.fromDDL("d int"))
    override def inputRDDs(): Seq[RDD[InternalRow]] = Seq(spark.sparkContext.emptyRDD[InternalRow])
    override protected def doExecute(): RDD[InternalRow] = sys.error("Not used")
    override protected def doProduce(ctx: CodegenContext): String = {
      if (useInnerClass) {
        val innerClassName = ctx.freshName("innerClass")
        ctx.addInnerClass(
          s"""
             |public class $innerClassName {
             |  public $innerClassName() {}
             |}
           """.stripMargin)
      }
      ""
    }
  }

  test("Prints bytecode statistics in debugCodegen") {
    Seq(true, false).foreach { useInnerClass =>
      val plan = WholeStageCodegenExec(DummyCodeGeneratorPlan(useInnerClass))(codegenStageId = 0)

      val genCodes = codegenStringSeq(plan)
      assert(genCodes.length == 1)
      val (_, _, codeStats) = genCodes.head
      val expectedNumInnerClasses = if (useInnerClass) 1 else 0
      assert(codeStats.maxMethodCodeSize > 0 && codeStats.maxConstPoolSize > 0 &&
        codeStats.numInnerClasses == expectedNumInnerClasses)

      val debugCodegenStr = codegenString(plan)
      assert(debugCodegenStr.contains("maxMethodCodeSize:"))
      assert(debugCodegenStr.contains("maxConstantPoolSize:"))
      assert(debugCodegenStr.contains("numInnerClasses:"))
    }
  }
}

// Disable AQE because the WholeStageCodegenExec is added when running QueryStageExec
class DebuggingSuite extends DebuggingSuiteBase with DisableAdaptiveExecutionSuite {

  test("SPARK-28537: DebugExec cannot debug broadcast related queries") {
    val rightDF = spark.range(10)
    val leftDF = spark.range(10)
    val joinedDF = leftDF.join(rightDF, leftDF("id") === rightDF("id"))

    val captured = new ByteArrayOutputStream()
    Console.withOut(captured) {
      joinedDF.debug()
    }

    val output = captured.toString()
    val hashedModeString = "HashedRelationBroadcastMode(List(input[0, bigint, false]),false)"
    assert(output.replaceAll("\\[plan_id=\\d+\\]", "[plan_id=x]").contains(
      s"""== BroadcastExchange $hashedModeString, [plan_id=x] ==
         |Tuples output: 0
         | id LongType: {}
         |== WholeStageCodegen (1) ==
         |Tuples output: 10
         | id LongType: {java.lang.Long}
         |== Range (0, 10, step=1, splits=2) ==
         |Tuples output: 0
         | id LongType: {}""".stripMargin))
  }

  test("SPARK-28537: DebugExec cannot debug columnar related queries") {
    withTempPath { workDir =>
      val workDirPath = workDir.getAbsolutePath
      val input = spark.range(5).toDF("id")
      input.write.parquet(workDirPath)
      val df = spark.read.parquet(workDirPath)

      val captured = new ByteArrayOutputStream()
      Console.withOut(captured) {
        df.debug()
      }

      val output = captured.toString()
        .replaceAll("== FileScan parquet \\[id#\\d+L] .* ==", "== FileScan parquet [id#xL] ==")
      assert(output.contains(
        """== FileScan parquet [id#xL] ==
          |Tuples output: 0
          | id LongType: {}
          |""".stripMargin))
    }
  }
}

class DebuggingSuiteAE extends DebuggingSuiteBase with EnableAdaptiveExecutionSuite
