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

import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;


public class JavaLDASuite implements Serializable {
  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaLDA");
    ArrayList<Tuple2<Long, Vector>> tinyCorpus = new ArrayList<Tuple2<Long, Vector>>();
    for (int i = 0; i < LDASuite$.MODULE$.tinyCorpus().length; i++) {
      tinyCorpus.add(new Tuple2<Long, Vector>((Long)LDASuite$.MODULE$.tinyCorpus()[i]._1(),
          LDASuite$.MODULE$.tinyCorpus()[i]._2()));
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
    LocalLDAModel model = new LocalLDAModel(LDASuite$.MODULE$.tinyTopics());

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

    DistributedLDAModel model = lda.run(corpus);

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
    assert(model.logLikelihood() < 0.0);
    assert(model.logPrior() < 0.0);
  }

  private static int tinyK = LDASuite$.MODULE$.tinyK();
  private static int tinyVocabSize = LDASuite$.MODULE$.tinyVocabSize();
  private static Matrix tinyTopics = LDASuite$.MODULE$.tinyTopics();
  private static Tuple2<int[], double[]>[] tinyTopicDescription =
      LDASuite$.MODULE$.tinyTopicDescription();
  JavaPairRDD<Long, Vector> corpus;

}
