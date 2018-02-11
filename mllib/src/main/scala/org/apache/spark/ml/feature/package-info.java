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


/**
 * Feature transformers
 *
 * The `ml.feature` package provides common feature transformers that help convert raw data or
 * features into more suitable forms for model fitting.
 * Most feature transformers are implemented as {@link org.apache.spark.ml.Transformer}s, which
 * transforms one {@link org.apache.spark.sql.Dataset} into another, e.g.,
 * {@link org.apache.spark.ml.feature.HashingTF}.
 * Some feature transformers are implemented as {@link org.apache.spark.ml.Estimator}}s, because the
 * transformation requires some aggregated information of the dataset, e.g., document
 * frequencies in {@link org.apache.spark.ml.feature.IDF}.
 * For those feature transformers, calling {@link org.apache.spark.ml.Estimator#fit} is required to
 * obtain the model first, e.g., {@link org.apache.spark.ml.feature.IDFModel}, in order to apply
 * transformation.
 * The transformation is usually done by appending new columns to the input
 * {@link org.apache.spark.sql.Dataset}, so all input columns are carried over.
 *
 * We try to make each transformer minimal, so it becomes flexible to assemble feature
 * transformation pipelines.
 * {@link org.apache.spark.ml.Pipeline} can be used to chain feature transformers, and
 * {@link org.apache.spark.ml.feature.VectorAssembler} can be used to combine multiple feature
 * transformations, for example:
 *
 * <pre>
 * <code>
 *   import java.util.Arrays;
 *
 *   import org.apache.spark.api.java.JavaRDD;
 *   import static org.apache.spark.sql.types.DataTypes.*;
 *   import org.apache.spark.sql.types.StructType;
 *   import org.apache.spark.sql.Dataset;
 *   import org.apache.spark.sql.RowFactory;
 *   import org.apache.spark.sql.Row;
 *
 *   import org.apache.spark.ml.feature.*;
 *   import org.apache.spark.ml.Pipeline;
 *   import org.apache.spark.ml.PipelineStage;
 *   import org.apache.spark.ml.PipelineModel;
 *
 *  // a DataFrame with three columns: id (integer), text (string), and rating (double).
 *  StructType schema = createStructType(
 *    Arrays.asList(
 *      createStructField("id", IntegerType, false),
 *      createStructField("text", StringType, false),
 *      createStructField("rating", DoubleType, false)));
 *  JavaRDD&lt;Row&gt; rowRDD = jsc.parallelize(
 *    Arrays.asList(
 *      RowFactory.create(0, "Hi I heard about Spark", 3.0),
 *      RowFactory.create(1, "I wish Java could use case classes", 4.0),
 *      RowFactory.create(2, "Logistic regression models are neat", 4.0)));
 *  Dataset&lt;Row&gt; dataset = jsql.createDataFrame(rowRDD, schema);
 *  // define feature transformers
 *  RegexTokenizer tok = new RegexTokenizer()
 *    .setInputCol("text")
 *    .setOutputCol("words");
 *  StopWordsRemover sw = new StopWordsRemover()
 *    .setInputCol("words")
 *    .setOutputCol("filtered_words");
 *  HashingTF tf = new HashingTF()
 *    .setInputCol("filtered_words")
 *    .setOutputCol("tf")
 *    .setNumFeatures(10000);
 *  IDF idf = new IDF()
 *    .setInputCol("tf")
 *    .setOutputCol("tf_idf");
 *  VectorAssembler assembler = new VectorAssembler()
 *    .setInputCols(new String[] {"tf_idf", "rating"})
 *    .setOutputCol("features");
 *
 *  // assemble and fit the feature transformation pipeline
 *  Pipeline pipeline = new Pipeline()
 *    .setStages(new PipelineStage[] {tok, sw, tf, idf, assembler});
 *  PipelineModel model = pipeline.fit(dataset);
 *
 *  // save transformed features with raw data
 *  model.transform(dataset)
 *    .select("id", "text", "rating", "features")
 *    .write().format("parquet").save("/output/path");
 * </code>
 * </pre>
 *
 * Some feature transformers implemented in MLlib are inspired by those implemented in scikit-learn.
 * The major difference is that most scikit-learn feature transformers operate eagerly on the entire
 * input dataset, while MLlib's feature transformers operate lazily on individual columns,
 * which is more efficient and flexible to handle large and complex datasets.
 *
 * @see <a href="http://scikit-learn.org/stable/modules/preprocessing.html" target="_blank">
 * scikit-learn.preprocessing</a>
 */
package org.apache.spark.ml.feature;
