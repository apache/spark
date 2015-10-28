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

package org.apache.spark.ml

import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, VectorAssembler}
import org.apache.spark.sql.DataFrame

/**
 * == Feature transformers ==
 *
 * The `ml.feature` package provides common feature transformers that help convert raw data or
 * features into more suitable forms for model fitting.
 * Most feature transformers are implemented as [[Transformer]]s, which transform one [[DataFrame]]
 * into another, e.g., [[HashingTF]].
 * Some feature transformers are implemented as [[Estimator]]s, because the transformation requires
 * some aggregated information of the dataset, e.g., document frequencies in [[IDF]].
 * For those feature transformers, calling [[Estimator!.fit]] is required to obtain the model first,
 * e.g., [[IDFModel]], in order to apply transformation.
 * The transformation is usually done by appending new columns to the input [[DataFrame]], so all
 * input columns are carried over.
 *
 * We try to make each transformer minimal, so it becomes flexible to assemble feature
 * transformation pipelines.
 * [[Pipeline]] can be used to chain feature transformers, and [[VectorAssembler]] can be used to
 * combine multiple feature transformations, for example:
 *
 * {{{
 *   import org.apache.spark.ml.feature._
 *   import org.apache.spark.ml.Pipeline
 *
 *   // a DataFrame with three columns: id (integer), text (string), and rating (double).
 *   val df = sqlContext.createDataFrame(Seq(
 *     (0, "Hi I heard about Spark", 3.0),
 *     (1, "I wish Java could use case classes", 4.0),
 *     (2, "Logistic regression models are neat", 4.0)
 *   )).toDF("id", "text", "rating")
 *
 *   // define feature transformers
 *   val tok = new RegexTokenizer()
 *     .setInputCol("text")
 *     .setOutputCol("words")
 *   val sw = new StopWordsRemover()
 *     .setInputCol("words")
 *     .setOutputCol("filtered_words")
 *   val tf = new HashingTF()
 *     .setInputCol("filtered_words")
 *     .setOutputCol("tf")
 *     .setNumFeatures(10000)
 *   val idf = new IDF()
 *     .setInputCol("tf")
 *     .setOutputCol("tf_idf")
 *   val assembler = new VectorAssembler()
 *     .setInputCols(Array("tf_idf", "rating"))
 *     .setOutputCol("features")
 *
 *   // assemble and fit the feature transformation pipeline
 *   val pipeline = new Pipeline()
 *     .setStages(Array(tok, sw, tf, idf, assembler))
 *   val model = pipeline.fit(df)
 *
 *   // save transformed features with raw data
 *   model.transform(df)
 *     .select("id", "text", "rating", "features")
 *     .write.format("parquet").save("/output/path")
 * }}}
 *
 * Some feature transformers implemented in MLlib are inspired by those implemented in scikit-learn.
 * The major difference is that most scikit-learn feature transformers operate eagerly on the entire
 * input dataset, while MLlib's feature transformers operate lazily on individual columns,
 * which is more efficient and flexible to handle large and complex datasets.
 *
 * @see [[http://scikit-learn.org/stable/modules/preprocessing.html scikit-learn.preprocessing]]
 */
package object feature
