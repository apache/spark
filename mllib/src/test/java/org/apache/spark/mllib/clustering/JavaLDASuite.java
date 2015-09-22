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

package org.apache.spark.mllib.clustering;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

import scala.Tuple2;
import scala.Tuple3;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class JavaLDASuite implements Serializable {
  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaLDA");
    ArrayList<Tuple2<Long, Vector>> tinyCorpus = new ArrayList<Tuple2<Long, Vector>>();
    for (int i = 0; i < LDASuite.tinyCorpus().length; i++) {
      tinyCorpus.add(new Tuple2<Long, Vector>((Long)LDASuite.tinyCorpus()[i]._1(),
          LDASuite.tinyCorpus()[i]._2()));
    }
    JavaRDD<Tuple2<Long, Vector>> tmpCorpus = sc.parallelize(tinyCorpus, 2);
    corpus = JavaPairRDD.fromJavaRDD(tmpCorpus);
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  @Test
  public void localLDAModel() {
    Matrix topics = LDASuite.tinyTopics();
    double[] topicConcentration = new double[topics.numRows()];
    Arrays.fill(topicConcentration, 1.0D / topics.numRows());
    LocalLDAModel model = new LocalLDAModel(topics, Vectors.dense(topicConcentration), 1D, 100D);

    // Check: basic parameters
    assertEquals(model.k(), tinyK);
    assertEquals(model.vocabSize(), tinyVocabSize);
    assertEquals(model.topicsMatrix(), tinyTopics);

    // Check: describeTopics() with all terms
    Tuple2<int[], double[]>[] fullTopicSummary = model.describeTopics();
    assertEquals(fullTopicSummary.length, tinyK);
    for (int i = 0; i < fullTopicSummary.length; i++) {
      assertArrayEquals(fullTopicSummary[i]._1(), tinyTopicDescription[i]._1());
      assertArrayEquals(fullTopicSummary[i]._2(), tinyTopicDescription[i]._2(), 1e-5);
    }
  }

  @Test
  public void distributedLDAModel() {
    int k = 3;
    double topicSmoothing = 1.2;
    double termSmoothing = 1.2;

    // Train a model
    LDA lda = new LDA();
    lda.setK(k)
      .setDocConcentration(topicSmoothing)
      .setTopicConcentration(termSmoothing)
      .setMaxIterations(5)
      .setSeed(12345);

    DistributedLDAModel model = (DistributedLDAModel)lda.run(corpus);

    // Check: basic parameters
    LocalLDAModel localModel = model.toLocal();
    assertEquals(model.k(), k);
    assertEquals(localModel.k(), k);
    assertEquals(model.vocabSize(), tinyVocabSize);
    assertEquals(localModel.vocabSize(), tinyVocabSize);
    assertEquals(model.topicsMatrix(), localModel.topicsMatrix());

    // Check: topic summaries
    Tuple2<int[], double[]>[] roundedTopicSummary = model.describeTopics();
    assertEquals(roundedTopicSummary.length, k);
    Tuple2<int[], double[]>[] roundedLocalTopicSummary = localModel.describeTopics();
    assertEquals(roundedLocalTopicSummary.length, k);

    // Check: log probabilities
    assertTrue(model.logLikelihood() < 0.0);
    assertTrue(model.logPrior() < 0.0);

    // Check: topic distributions
    JavaPairRDD<Long, Vector> topicDistributions = model.javaTopicDistributions();
    // SPARK-5562. since the topicDistribution returns the distribution of the non empty docs
    // over topics. Compare it against nonEmptyCorpus instead of corpus
    JavaPairRDD<Long, Vector> nonEmptyCorpus = corpus.filter(
      new Function<Tuple2<Long, Vector>, Boolean>() {
        public Boolean call(Tuple2<Long, Vector> tuple2) {
          return Vectors.norm(tuple2._2(), 1.0) != 0.0;
        }
    });
    assertEquals(topicDistributions.count(), nonEmptyCorpus.count());

    // Check: javaTopTopicsPerDocuments
    Tuple3<Long, int[], double[]> topTopics = model.javaTopTopicsPerDocument(3).first();
    Long docId = topTopics._1(); // confirm doc ID type
    int[] topicIndices = topTopics._2();
    double[] topicWeights = topTopics._3();
    assertEquals(3, topicIndices.length);
    assertEquals(3, topicWeights.length);

    // Check: topTopicAssignments
    Tuple3<Long, int[], int[]> topicAssignment = model.javaTopicAssignments().first();
    Long docId2 = topicAssignment._1();
    int[] termIndices2 = topicAssignment._2();
    int[] topicIndices2 = topicAssignment._3();
    assertEquals(termIndices2.length, topicIndices2.length);
  }

  @Test
  public void OnlineOptimizerCompatibility() {
    int k = 3;
    double topicSmoothing = 1.2;
    double termSmoothing = 1.2;

    // Train a model
    OnlineLDAOptimizer op = new OnlineLDAOptimizer()
      .setTau0(1024)
      .setKappa(0.51)
      .setGammaShape(1e40)
      .setMiniBatchFraction(0.5);

    LDA lda = new LDA();
    lda.setK(k)
      .setDocConcentration(topicSmoothing)
      .setTopicConcentration(termSmoothing)
      .setMaxIterations(5)
      .setSeed(12345)
      .setOptimizer(op);

    LDAModel model = lda.run(corpus);

    // Check: basic parameters
    assertEquals(model.k(), k);
    assertEquals(model.vocabSize(), tinyVocabSize);

    // Check: topic summaries
    Tuple2<int[], double[]>[] roundedTopicSummary = model.describeTopics();
    assertEquals(roundedTopicSummary.length, k);
    Tuple2<int[], double[]>[] roundedLocalTopicSummary = model.describeTopics();
    assertEquals(roundedLocalTopicSummary.length, k);
  }

  @Test
  public void localLdaMethods() {
    JavaRDD<Tuple2<Long, Vector>> docs = sc.parallelize(toyData, 2);
    JavaPairRDD<Long, Vector> pairedDocs = JavaPairRDD.fromJavaRDD(docs);

    // check: topicDistributions
    assertEquals(toyModel.topicDistributions(pairedDocs).count(), pairedDocs.count());

    // check: logPerplexity
    double logPerplexity = toyModel.logPerplexity(pairedDocs);

    // check: logLikelihood.
    ArrayList<Tuple2<Long, Vector>> docsSingleWord = new ArrayList<Tuple2<Long, Vector>>();
    docsSingleWord.add(new Tuple2<Long, Vector>(0L, Vectors.dense(1.0, 0.0, 0.0)));
    JavaPairRDD<Long, Vector> single = JavaPairRDD.fromJavaRDD(sc.parallelize(docsSingleWord));
    double logLikelihood = toyModel.logLikelihood(single);
  }

  private static int tinyK = LDASuite.tinyK();
  private static int tinyVocabSize = LDASuite.tinyVocabSize();
  private static Matrix tinyTopics = LDASuite.tinyTopics();
  private static Tuple2<int[], double[]>[] tinyTopicDescription =
      LDASuite.tinyTopicDescription();
  private JavaPairRDD<Long, Vector> corpus;
  private LocalLDAModel toyModel = LDASuite.toyModel();
  private ArrayList<Tuple2<Long, Vector>> toyData = LDASuite.javaToyData();

}
