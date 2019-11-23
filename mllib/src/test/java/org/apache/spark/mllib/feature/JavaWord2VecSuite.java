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

package org.apache.spark.mllib.feature;

import java.util.Arrays;
import java.util.List;

import com.google.common.base.Strings;

import scala.Tuple2;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;

public class JavaWord2VecSuite extends SharedSparkSession {

  @Test
  @SuppressWarnings("unchecked")
  public void word2Vec() {
    // The tests are to check Java compatibility.
    String sentence = Strings.repeat("a b ", 100) + Strings.repeat("a c ", 10);
    List<String> words = Arrays.asList(sentence.split(" "));
    List<List<String>> localDoc = Arrays.asList(words, words);
    JavaRDD<List<String>> doc = jsc.parallelize(localDoc);
    Word2Vec word2vec = new Word2Vec()
      .setVectorSize(10)
      .setSeed(42L);
    Word2VecModel model = word2vec.fit(doc);
    Tuple2<String, Object>[] syms = model.findSynonyms("a", 2);
    Assert.assertEquals(2, syms.length);
    Assert.assertEquals("b", syms[0]._1());
    Assert.assertEquals("c", syms[1]._1());
  }
}
