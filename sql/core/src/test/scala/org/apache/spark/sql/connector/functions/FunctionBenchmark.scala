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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryCatalog}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction, UnboundFunction}
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, LongType, StructType}

/**
 * Benchmark to measure DataSourceV2 UDF performance
 * {{{
 *   To run this benchmark:
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/FunctionBenchmark-results.txt".
 * }}}
 */
object FunctionBenchmark extends SqlBasedBenchmark {
  val catalogName: String = "benchmark_catalog"

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val N = 500L * 1000 * 1000
    Seq(true, false).foreach { enableCodeGen =>
      Seq(true, false).foreach { resultNullable =>
        javaScalarBenchmark(N, enableCodeGen = enableCodeGen, resultNullable = resultNullable)
      }
    }
    Seq(true, false).foreach { enableCodeGen =>
      Seq(true, false).foreach { resultNullable =>
        scalaScalarBenchmark(N, enableCodeGen = enableCodeGen, resultNullable = resultNullable)
      }
    }
  }

  private def javaScalarBenchmark(
      N: Long,
      enableCodeGen: Boolean,
      resultNullable: Boolean): Unit = {
    withSQLConf(s"spark.sql.catalog.$catalogName" -> classOf[InMemoryCatalog].getName) {
      createFunction("long_add_default", new JavaLongAdd(new JavaLongAddDefault(resultNullable)))
      createFunction("long_add_magic", new JavaLongAdd(new JavaLongAddMagic(resultNullable)))
      createFunction("long_add_static_magic",
          new JavaLongAdd(new JavaLongAddStaticMagic(resultNullable)))

      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> enableCodeGen.toString) {
        val flag = if (enableCodeGen) "on" else "off"
        val nullable = if (resultNullable) "nullable" else "notnull"
        val name = s"Java scalar function (long + long) -> long/$nullable wholestage $flag"
        val benchmark = new Benchmark(name, N, output = output)
        Seq("long_add_default", "long_add_magic", "long_add_static_magic").foreach { name =>
          benchmark.addCase(s"with $name", numIters = 3) { _ =>
            spark.range(N).selectExpr(s"$catalogName.$name(id, id)").noop()
          }
        }
        benchmark.run()
      }
    }
  }

  private def scalaScalarBenchmark(
      N: Long,
      enableCodeGen: Boolean,
      resultNullable: Boolean): Unit = {
    withSQLConf(s"spark.sql.catalog.$catalogName" -> classOf[InMemoryCatalog].getName) {
      createFunction("long_add_default",
        LongAddUnbound(new LongAddWithProduceResult(resultNullable)))
      createFunction("long_add_magic", LongAddUnbound(new LongAddWithMagic(resultNullable)))

      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> enableCodeGen.toString) {
        val flag = if (enableCodeGen) "on" else "off"
        val nullable = if (resultNullable) "nullable" else "notnull"
        val name = s"Scala scalar function (long + long) -> long/$nullable wholestage $flag"
        val benchmark = new Benchmark(name, N, output = output)
        Seq("long_add_default", "long_add_magic").foreach { name =>
          benchmark.addCase(s"with $name", numIters = 3) { _ =>
            spark.range(N).selectExpr(s"$catalogName.$name(id, id)").noop()
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

