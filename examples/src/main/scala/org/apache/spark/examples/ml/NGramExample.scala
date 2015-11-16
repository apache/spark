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

// scalastyle:off println

package org.apache.spark.examples.ml

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.feature.NGram

/**
 * An example runner for n-gram. Run with
 * {{{
 * ./bin/run-example ml.NGramExample [options]
 * }}}
 */
object NGramExample {

  val conf = new SparkConf().setAppName("NGramExample")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val wordDataFrame = sqlContext.createDataFrame(Seq(
    (0, Array("Hi", "I", "heard", "about", "Spark")),
    (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
    (2, Array("Logistic", "regression", "models", "are", "neat"))
  )).toDF("label", "words")

  val ngram = new NGram().setInputCol("words").setOutputCol("ngrams")
  val ngramDataFrame = ngram.transform(wordDataFrame)
  ngramDataFrame.take(3).map(_.getAs[Stream[String]]("ngrams").toList).foreach(println)
}
