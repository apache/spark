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

package org.apache.spark.mllib.fpm;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.fpm.PrefixSpan.FreqSequence;
import org.apache.spark.util.Utils;

public class JavaPrefixSpanSuite extends SharedSparkSession {

  @Test
  public void runPrefixSpan() {
    JavaRDD<List<List<Integer>>> sequences = jsc.parallelize(Arrays.asList(
      Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3)),
      Arrays.asList(Arrays.asList(1), Arrays.asList(3, 2), Arrays.asList(1, 2)),
      Arrays.asList(Arrays.asList(1, 2), Arrays.asList(5)),
      Arrays.asList(Arrays.asList(6))
    ), 2);
    PrefixSpan prefixSpan = new PrefixSpan()
      .setMinSupport(0.5)
      .setMaxPatternLength(5);
    PrefixSpanModel<Integer> model = prefixSpan.run(sequences);
    JavaRDD<FreqSequence<Integer>> freqSeqs = model.freqSequences().toJavaRDD();
    List<FreqSequence<Integer>> localFreqSeqs = freqSeqs.collect();
    Assert.assertEquals(5, localFreqSeqs.size());
    // Check that each frequent sequence could be materialized.
    for (PrefixSpan.FreqSequence<Integer> freqSeq : localFreqSeqs) {
      List<List<Integer>> seq = freqSeq.javaSequence();
      long freq = freqSeq.freq();
    }
  }

  @Test
  public void runPrefixSpanSaveLoad() {
    JavaRDD<List<List<Integer>>> sequences = jsc.parallelize(Arrays.asList(
      Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3)),
      Arrays.asList(Arrays.asList(1), Arrays.asList(3, 2), Arrays.asList(1, 2)),
      Arrays.asList(Arrays.asList(1, 2), Arrays.asList(5)),
      Arrays.asList(Arrays.asList(6))
    ), 2);
    PrefixSpan prefixSpan = new PrefixSpan()
      .setMinSupport(0.5)
      .setMaxPatternLength(5);
    PrefixSpanModel<Integer> model = prefixSpan.run(sequences);

    File tempDir = Utils.createTempDir(
      System.getProperty("java.io.tmpdir"), "JavaPrefixSpanSuite");
    String outputPath = tempDir.getPath();

    try {
      model.save(spark.sparkContext(), outputPath);
      PrefixSpanModel newModel = PrefixSpanModel.load(spark.sparkContext(), outputPath);
      JavaRDD<FreqSequence<Integer>> freqSeqs = newModel.freqSequences().toJavaRDD();
      List<FreqSequence<Integer>> localFreqSeqs = freqSeqs.collect();
      Assert.assertEquals(5, localFreqSeqs.size());
      // Check that each frequent sequence could be materialized.
      for (PrefixSpan.FreqSequence<Integer> freqSeq : localFreqSeqs) {
        List<List<Integer>> seq = freqSeq.javaSequence();
        long freq = freqSeq.freq();
      }
    } finally {
      Utils.deleteRecursively(tempDir);
    }


  }
}
