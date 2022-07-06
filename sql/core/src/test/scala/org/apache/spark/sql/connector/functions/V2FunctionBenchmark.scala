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

package org.apache.spark.sql.connector.functions

import test.org.apache.spark.sql.connector.catalog.functions.JavaLongAdd
import test.org.apache.spark.sql.connector.catalog.functions.JavaLongAdd.{JavaLongAddDefault, JavaLongAddMagic, JavaLongAddStaticMagic}

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{BinaryArithmetic, Expression}
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryCatalog}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction, UnboundFunction}
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AbstractDataType, DataType, LongType, NumericType, StructType}

/**
 * Benchmark to measure DataSourceV2 UDF performance
 * {{{
 *   To run this benchmark:
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/V2FunctionBenchmark-results.txt".
 * }}}
 * '''NOTE''': to update the result of this benchmark, please use Github benchmark action:
 *   https://spark.apache.org/developer-tools.html#github-workflow-benchmarks
 */
object V2FunctionBenchmark extends SqlBasedBenchmark {
  val catalogName: String = "benchmark_catalog"

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val N = 500L * 1000 * 1000
    Seq(true, false).foreach { codegenEnabled =>
      Seq(true, false).foreach { resultNullable =>
        scalarFunctionBenchmark(N, codegenEnabled = codegenEnabled,
          resultNullable = resultNullable)
      }
    }
  }

  private def scalarFunctionBenchmark(
      N: Long,
      codegenEnabled: Boolean,
      resultNullable: Boolean): Unit = {
    withSQLConf(s"spark.sql.catalog.$catalogName" -> classOf[InMemoryCatalog].getName) {
      createFunction("java_long_add_default",
        new JavaLongAdd(new JavaLongAddDefault(resultNullable)))
      createFunction("java_long_add_magic", new JavaLongAdd(new JavaLongAddMagic(resultNullable)))
      createFunction("java_long_add_static_magic",
        new JavaLongAdd(new JavaLongAddStaticMagic(resultNullable)))
      createFunction("scala_long_add_default",
        LongAddUnbound(new LongAddWithProduceResult(resultNullable)))
      createFunction("scala_long_add_magic", LongAddUnbound(new LongAddWithMagic(resultNullable)))

      val codeGenFactoryMode = if (codegenEnabled) FALLBACK else NO_CODEGEN
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegenEnabled.toString,
          SQLConf.CODEGEN_FACTORY_MODE.key -> codeGenFactoryMode.toString) {
        val name = s"scalar function (long + long) -> long, result_nullable = $resultNullable " +
            s"codegen = $codegenEnabled"
        val benchmark = new Benchmark(name, N, output = output)
        benchmark.addCase(s"native_long_add", numIters = 3) { _ =>
          spark.range(N).select(Column(NativeAdd($"id".expr, $"id".expr, resultNullable))).noop()
        }
        Seq("java_long_add_default", "java_long_add_magic", "java_long_add_static_magic",
            "scala_long_add_default", "scala_long_add_magic").foreach { functionName =>
          benchmark.addCase(s"$functionName", numIters = 3) { _ =>
            spark.range(N).selectExpr(s"$catalogName.$functionName(id, id)").noop()
          }
        }
        benchmark.run()
      }
    }
  }

  private def createFunction(name: String, fn: UnboundFunction): Unit = {
    val catalog = spark.sessionState.catalogManager.catalog(catalogName)
    val ident = Identifier.of(Array.empty, name)
    catalog.asInstanceOf[InMemoryCatalog].createFunction(ident, fn)
  }

  case class NativeAdd(
      left: Expression,
      right: Expression,
      override val nullable: Boolean) extends BinaryArithmetic {
    override protected val failOnError: Boolean = false
    override def inputType: AbstractDataType = NumericType
    override def symbol: String = "+"

    private lazy val numeric = TypeUtils.getNumeric(dataType, failOnError)
    protected override def nullSafeEval(input1: Any, input2: Any): Any =
      numeric.plus(input1, input2)

    override protected def withNewChildrenInternal(
        newLeft: Expression,
        newRight: Expression): NativeAdd = copy(left = newLeft, right = newRight)
  }

  case class LongAddUnbound(impl: ScalarFunction[Long]) extends UnboundFunction {
    override def bind(inputType: StructType): BoundFunction = impl
    override def description(): String = name()
    override def name(): String = "long_add_unbound"
  }

  abstract class LongAddBase(resultNullable: Boolean) extends ScalarFunction[Long] {
    override def inputTypes(): Array[DataType] = Array(LongType, LongType)
    override def resultType(): DataType = LongType
    override def isResultNullable: Boolean = resultNullable
  }

  class LongAddWithProduceResult(resultNullable: Boolean) extends LongAddBase(resultNullable) {
    override def produceResult(input: InternalRow): Long = {
      input.getLong(0) + input.getLong(1)
    }
    override def name(): String = "long_add_default"
  }

  class LongAddWithMagic(resultNullable: Boolean) extends LongAddBase(resultNullable) {
    def invoke(left: Long, right: Long): Long = {
      left + right
    }
    override def name(): String = "long_add_magic"
  }
}

