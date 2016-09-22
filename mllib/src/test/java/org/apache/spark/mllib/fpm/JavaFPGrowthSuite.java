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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.Utils;

public class JavaFPGrowthSuite extends SharedSparkSession {

  @Test
  public void runFPGrowth() {

    @SuppressWarnings("unchecked")
    JavaRDD<List<String>> rdd = jsc.parallelize(Arrays.asList(
      Arrays.asList("r z h k p".split(" ")),
      Arrays.asList("z y x w v u t s".split(" ")),
      Arrays.asList("s x o n r".split(" ")),
      Arrays.asList("x z y m t s q e".split(" ")),
      Arrays.asList("z".split(" ")),
      Arrays.asList("x z y r q t p".split(" "))), 2);

    FPGrowthModel<String> model = new FPGrowth()
      .setMinSupport(0.5)
      .setNumPartitions(2)
      .run(rdd);

    List<FPGrowth.FreqItemset<String>> freqItemsets = model.freqItemsets().toJavaRDD().collect();
    assertEquals(18, freqItemsets.size());

    for (FPGrowth.FreqItemset<String> itemset : freqItemsets) {
      // Test return types.
      List<String> items = itemset.javaItems();
      long freq = itemset.freq();
    }
  }

  @Test
  public void runFPGrowthSaveLoad() {

    @SuppressWarnings("unchecked")
    JavaRDD<List<String>> rdd = jsc.parallelize(Arrays.asList(
      Arrays.asList("r z h k p".split(" ")),
      Arrays.asList("z y x w v u t s".split(" ")),
      Arrays.asList("s x o n r".split(" ")),
      Arrays.asList("x z y m t s q e".split(" ")),
      Arrays.asList("z".split(" ")),
      Arrays.asList("x z y r q t p".split(" "))), 2);

    FPGrowthModel<String> model = new FPGrowth()
      .setMinSupport(0.5)
      .setNumPartitions(2)
      .run(rdd);

    File tempDir = Utils.createTempDir(
      System.getProperty("java.io.tmpdir"), "JavaFPGrowthSuite");
    String outputPath = tempDir.getPath();

    try {
      model.save(spark.sparkContext(), outputPath);
      @SuppressWarnings("unchecked")
      FPGrowthModel<String> newModel =
        (FPGrowthModel<String>) FPGrowthModel.load(spark.sparkContext(), outputPath);
      List<FPGrowth.FreqItemset<String>> freqItemsets = newModel.freqItemsets().toJavaRDD()
        .collect();
      assertEquals(18, freqItemsets.size());

      for (FPGrowth.FreqItemset<String> itemset : freqItemsets) {
        // Test return types.
        List<String> items = itemset.javaItems();
        long freq = itemset.freq();
      }
    } finally {
      Utils.deleteRecursively(tempDir);
    }
  }
}
