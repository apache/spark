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

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.catalyst.expressions.codegen.CodeFormatter
import org.apache.spark.util.Utils.times

/**
 * Benchmark for the previous regular expression based comment and extra new line removal vs new
 * custom implementation.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark catalyst test jar>
 *   2. build/sbt "catalyst/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/test:runMain <this class>"
 *      Results will be written to "benchmarks/CodeFormatterBenchmark-results.txt".
 * }}}
 */
object CodeFormatterBenchmark extends BenchmarkBase {

  private val commentRegexp =
    ("""([ |\t]*?\/\*[\s|\S]*?\*\/[ |\t]*?)|""" + // strip /*comment*/
      """([ |\t]*?\/\/[\s\S]*?\n)""").r           // strip //comment
  private val extraNewLinesRegexp = """\n\s*\n""".r       // strip extra newlines

  private def stripExtraNewLinesAndCommentsUsingRegexp(input: String): String = {
    extraNewLinesRegexp.replaceAllIn(commentRegexp.replaceAllIn(input, ""), "\n")
  }

  def test(name: String, sourceCode: String, numIters: Int, stripNum: Int, lineNum: Int): Unit = {
    runBenchmark(name) {
      val benchmark = new Benchmark(name, stripNum * lineNum, output = output)

      benchmark.addCase("regular expression", numIters) { _: Int =>
        times(stripNum) {
          val formatted = stripExtraNewLinesAndCommentsUsingRegexp(sourceCode)
        }
      }

      benchmark.addCase("custom implementation", numIters) { _: Int =>
        times(stripNum) {
          val formatted = CodeFormatter.stripExtraNewLinesAndComments(sourceCode)
        }
      }

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numIters = 5
    val stripNum = 1000
    val lineNum = 1000

    val noComments = new StringBuilder
    var i = 0
    while (i < lineNum) {
      noComments ++= "  code body\n"
      i += 1
    }
    test("No comments", noComments.toString, numIters, stripNum, lineNum)

    val someComments = new StringBuilder
    i = 0
    while (i < lineNum) {
      someComments ++= (if (i % 10 == 0) {
        "  // comment\n"
      } else if (i % 5 == 0) {
        "  /* comment */\n"
      } else {
        "  code body\n"
      })
      i += 1
    }
    test("Some comments", someComments.toString, numIters, stripNum, lineNum)

    val lotsOfComments = new StringBuilder
    i = 0
    while (i < lineNum) {
      lotsOfComments ++= (if (i % 4 == 0) {
        "  // comment\n"
      } else if (i % 2 == 0) {
        "  /* comment */\n"
      } else {
        "  code body\n"
      })
      i += 1
    }
    test("Lots of comments", lotsOfComments.toString, numIters, stripNum, lineNum)
  }
}
