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

package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.fpm.PrefixSpan
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example demonstrating PrefixSpan.
 * Run with
 * {{{
 * bin/run-example ml.PrefixSpanExample
 * }}}
 */
object PrefixSpanExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    import spark.implicits._

    // $example on$
    val smallTestData = Seq(
      Seq(Seq(1, 2), Seq(3)),
      Seq(Seq(1), Seq(3, 2), Seq(1, 2)),
      Seq(Seq(1, 2), Seq(5)),
      Seq(Seq(6)))

    val df = smallTestData.toDF("sequence")
    val result = new PrefixSpan()
      .setMinSupport(0.5)
      .setMaxPatternLength(5)
      .setMaxLocalProjDBSize(32000000)
      .findFrequentSequentialPatterns(df)
      .show()
    // $example off$

    spark.stop()
  }
}
