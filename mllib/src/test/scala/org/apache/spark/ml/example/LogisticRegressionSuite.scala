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

package org.apache.spark.ml.example

import scala.beans.BeanInfo

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.param.ParamGridBuilder
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.test.TestSQLContext._
import org.scalatest.FunSuite

@BeanInfo
case class LabeledDocument(id: Long, text: String, label: Double)

@BeanInfo
case class Document(id: Long, text: String)

class LogisticRegressionSuite extends FunSuite {

  val dataset = MLUtils
    .loadLibSVMFile(sparkContext, "../data/mllib/sample_binary_classification_data.txt")
    .cache()

  test("logistic regression") {
    val lr = new LogisticRegression
    val model = lr.fit(dataset)
    model.transform(dataset)
      .select('label, 'prediction)
      .collect()
      .foreach(println)
  }

  test("logistic regression with setters") {
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(1.0)
    val model = lr.fit(dataset)
    model.transform(dataset, model.threshold -> 0.8) // overwrite threshold
      .select('label, 'score, 'prediction).collect()
      .foreach(println)
  }

  test("logistic regression fit and transform with varargs") {
    val lr = new LogisticRegression
    val model = lr.fit(dataset, lr.maxIter -> 10, lr.regParam -> 1.0)
    model.transform(dataset, model.threshold -> 0.8, model.scoreCol -> "probability")
      .select('label, 'probability, 'prediction)
      .foreach(println)
  }

  test("logistic regression with cross validation") {
    val lr = new LogisticRegression
    val lrParamMaps = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 100.0))
      .addGrid(lr.maxIter, Array(0, 5))
      .build()
    val eval = new BinaryClassificationEvaluator
    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEstimatorParamMaps(lrParamMaps)
      .setEvaluator(eval)
      .setNumFolds(3)
    val bestModel = cv.fit(dataset)
  }

  test("logistic regression with pipeline") {
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
    val lr = new LogisticRegression()
      .setFeaturesCol("scaledFeatures")
    val pipeline = new Pipeline()
      .setStages(Array(scaler, lr))
    val model = pipeline.fit(dataset)
    val predictions = model.transform(dataset)
      .select('label, 'score, 'prediction)
      .collect()
      .foreach(println)
  }

  test("text classification pipeline") {
    val training = sparkContext.parallelize(Seq(
      LabeledDocument(0L, "a b c d e spark", 1.0),
      LabeledDocument(1L, "b d", 0.0),
      LabeledDocument(2L, "spark f g h", 1.0),
      LabeledDocument(3L, "hadoop mapreduce", 0.0)))
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))
    val model = pipeline.fit(training)
    val test = sparkContext.parallelize(Seq(
      Document(4L, "spark i j k"),
      Document(5L, "l m"),
      Document(6L, "mapreduce spark"),
      Document(7L, "apache hadoop")))
    model.transform(test)
      .select('id, 'text, 'prediction, 'score)
      .collect()
      .foreach(println)
  }
}
