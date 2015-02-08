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

package org.apache.spark.examples.ml;

import java.util.List;

import com.google.common.collect.Lists;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Row;

/**
 * A simple text classification pipeline that recognizes "spark" from input text. It uses the Java
 * bean classes {@link LabeledDocument} and {@link Document} defined in the Scala counterpart of
 * this example {@link SimpleTextClassificationPipeline}. Run with
 * <pre>
 * bin/run-example ml.JavaSimpleTextClassificationPipeline
 * </pre>
 */
public class JavaSimpleTextClassificationPipeline {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("JavaSimpleTextClassificationPipeline");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext jsql = new SQLContext(jsc);

    // Prepare training documents, which are labeled.
    List<LabeledDocument> localTraining = Lists.newArrayList(
      new LabeledDocument(0L, "a b c d e spark", 1.0),
      new LabeledDocument(1L, "b d", 0.0),
      new LabeledDocument(2L, "spark f g h", 1.0),
      new LabeledDocument(3L, "hadoop mapreduce", 0.0));
    DataFrame training = jsql.applySchema(jsc.parallelize(localTraining), LabeledDocument.class);

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    Tokenizer tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words");
    HashingTF hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol())
      .setOutputCol("features");
    LogisticRegression lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.01);
    Pipeline pipeline = new Pipeline()
      .setStages(new PipelineStage[] {tokenizer, hashingTF, lr});

    // Fit the pipeline to training documents.
    PipelineModel model = pipeline.fit(training);

    // Prepare test documents, which are unlabeled.
    List<Document> localTest = Lists.newArrayList(
      new Document(4L, "spark i j k"),
      new Document(5L, "l m n"),
      new Document(6L, "mapreduce spark"),
      new Document(7L, "apache hadoop"));
    DataFrame test = jsql.applySchema(jsc.parallelize(localTest), Document.class);

    // Make predictions on test documents.
    model.transform(test).registerTempTable("prediction");
    DataFrame predictions = jsql.sql("SELECT id, text, score, prediction FROM prediction");
    for (Row r: predictions.collect()) {
      System.out.println("(" + r.get(0) + ", " + r.get(1) + ") --> prob=" + r.get(2)
          + ", prediction=" + r.get(3));
    }

    jsc.stop();
  }
}
