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

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.ml.feature.{HashingTF, StringIndexer, Tokenizer}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * An example of an end to end machine learning pipeline that classifies text
 * into one of twenty possible news categories. The dataset is the 20newsgroups
 * dataset (http://qwone.com/~jason/20Newsgroups/20news-bydate.tar.gz)
 *
 * We assume some minimal preprocessing of this dataset has been done to unzip the dataset and
 * load the data into HDFS as follows:
 *  wget http://qwone.com/~jason/20Newsgroups/20news-bydate.tar.gz
 *  tar -xvzf 20news-bydate.tar.gz
 *  hadoop fs -mkdir ${20news.root.dir}
 *  hadoop fs -copyFromLocal 20news-bydate-train/ ${20news.root.dir}
 *  hadoop fs -copyFromLocal 20news-bydate-test/ ${20news.root.dir}
 *
 * This example uses Hive to schematize the data as tables, in order to map the folder
 * structure ${20news.root.dir}/{20news-bydate-train, 20news-bydate-train}/{newsgroup}/
 * to partition columns type, newsgroup resulting in a dataset with three columns:
 *  type, newsgroup, text
 *
 * In order to run this example, Spark needs to be build with hive, and at runtime there
 * should be a valid hive-site.xml in $SPARK_HOME/conf with at minimal the following
 * configuration:
 * <configuration>
 *   <property>
 *     <name>hive.metastore.uris</name>
 * <!-- Ensure that the following statement points to the Hive Metastore URI in your cluster -->
 *     <value>thrift://${thriftserver.host}:${thriftserver.port}</value>
 *   <description>URI for client to contact metastore server</description>
 *   </property>
 * </configuration>
 *
 * Run with
 * {{{
 * bin/spark-submit --class org.apache.spark.examples.ml.ComplexPipelineExample
 *   --driver-memory 4g [examples JAR path] ${20news.root.dir}
 * }}}
 */
object ComplexPipelineExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ComplexPipelineExample")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val path = args(0)

    sqlContext.sql(s"""CREATE EXTERNAL TABLE IF NOT EXISTS 20NEWS(text String)
      PARTITIONED BY (type String, newsgroup String)
      STORED AS TEXTFILE location '$path'""")

    val newsgroups = Array("alt.atheism", "comp.graphics",
      "comp.os.ms-windows.misc", "comp.sys.ibm.pc.hardware",
      "comp.sys.mac.hardware", "comp.windows.x", "misc.forsale",
      "rec.autos", "rec.motorcycles", "rec.sport.baseball",
      "rec.sport.hockey", "sci.crypt", "sci.electronics",
      "sci.med", "sci.space", "soc.religion.christian",
      "talk.politics.guns", "talk.politics.mideast",
      "talk.politics.misc", "talk.religion.misc")

    for (t <- Array("20news-bydate-train", "20news-bydate-train")) {
      for (newsgroup <- newsgroups) {
        sqlContext.sql(
          s"""ALTER TABLE 20NEWS ADD IF NOT EXISTS
             | PARTITION(type='$t', newsgroup='$newsgroup') LOCATION '$path/$t/$newsgroup/'"""
          .stripMargin)
      }
    }

    // shuffle the data
    val partitions = 100
    val data = sqlContext.sql("SELECT * FROM 20NEWS")
      .coalesce(partitions)  // by default we have over 19k partitions
      .repartition(partitions)
      .cache()

    import sqlContext.implicits._
    val train = data.filter($"type" === "20news-bydate-train").cache()
    val test = data.filter($"type" === "20news-bydate-train").cache()

    // convert string labels into numeric
    val labelIndexer = new StringIndexer()
      .setInputCol("newsgroup")
      .setOutputCol("label")

    // tokenize text into words
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")

    // extract hash based TF-IDF features
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")

    val lr = new LogisticRegression()
      .setLabelCol(labelIndexer.getOutputCol)
      .setMaxIter(10)
      .setRegParam(0.001)

    // learn multiclass classifier with Logistic Regression as base classifier
    val ovr = new OneVsRest()
      .setClassifier(lr)

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, tokenizer, hashingTF, ovr))

    val model = pipeline.fit(train)
    val predictions = model.transform(test).cache()
    val predictionAndLabels = predictions.select($"prediction", $"label")
      .map { case Row(prediction: Double, label: Double) => (prediction, label)}

    // compute multiclass metrics
    val metrics = new MulticlassMetrics(predictionAndLabels)

    val labelToIndexMap = predictions.select($"label", $"newsgroup").distinct
      .map {case Row(x: Double, y: String) => (y, x)}
      .collectAsMap()

    val fprs = labelToIndexMap.map { case (newsgroup, label) =>
      (newsgroup, metrics.falsePositiveRate(label))
    }

    println(metrics.confusionMatrix)
    println("label\tfpr")
    println(fprs.map {case (label, fpr) => label + "\t" + fpr}.mkString("\n"))

    sc.stop()
  }

}
