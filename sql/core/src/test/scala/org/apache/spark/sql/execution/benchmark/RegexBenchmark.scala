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

package org.apache.spark.sql.execution.benchmark

import scala.util.Random

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}




/**
 * Benchmark to measure performance for Regex.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/RegexBenchmark-results.txt".
 * }}}
 */

object RegexBenchmark extends SqlBasedBenchmark {


  private def makeRandomStr(cardinality: Int): DataFrame = {
    var rdd1 = spark.sparkContext.range(0, cardinality).map(num => {
      var str = new StringBuffer()
      var j = 0
      for (j <- 1 to 10) {
        str.append(('a' + Random.nextInt(10)).toChar)
      }
      Row(num, str.toString)
    })
    val schema = StructType(
      Seq(
        StructField("id", LongType, true)
        , StructField("t1", StringType, true)
      )
    )
    spark.createDataFrame(rdd1, schema)
  }

  private def doRunBenchmarkRLike(cardinality: Int): Unit = {
    val df1 = makeRandomStr(cardinality)
    df1.filter(s"t1 rlike '.*a.cd.*e.*'").count()
  }


  private def doRunBenchmarkLike(cardinality: Int): Unit = {
    val df1 = makeRandomStr(cardinality)
    df1.filter(s"t1 like '%a_cd%e%'").count()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val cardinality = 1000000


    runBenchmark("Regex rlike** ") {
      codegenBenchmarkRegex("Regex rlike", cardinality) {
        doRunBenchmarkRLike(cardinality)
      }
    }


    runBenchmark("Regex like") {
      codegenBenchmarkRegex("Regex like", cardinality) {
        doRunBenchmarkLike(cardinality)
      }
    }

  }

  def codegenBenchmarkRegex(name: String, cardinality: Long)(f: => Unit): Unit = {
    val benchmark = new Benchmark(name, cardinality, output = output)

    benchmark.addCase(s"$name java wholestage off", numIters = 2) { _ =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        f
      }
    }

    benchmark.addCase(s"$name java wholestage on", numIters = 5) { _ =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
        f
      }
    }

    benchmark.addCase(s"$name joni wholestage off", numIters = 2) { _ =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        SQLConf.REGEX_ENGINE.key -> "JONI") {
        f
      }
    }

    benchmark.addCase(s"$name joni wholestage on", numIters = 5) { _ =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
        SQLConf.REGEX_ENGINE.key -> "JONI") {
        f
      }
    }

    benchmark.run()
  }

}
