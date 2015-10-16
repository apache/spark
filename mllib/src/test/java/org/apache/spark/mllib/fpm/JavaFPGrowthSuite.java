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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class JavaFPGrowthSuite implements Serializable {
  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "JavaFPGrowth");
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  @Test
  public void runFPGrowth() {

    @SuppressWarnings("unchecked")
    JavaRDD<List<String>> rdd = sc.parallelize(Arrays.asList(
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

    for (FPGrowth.FreqItemset<String> itemset: freqItemsets) {
      // Test return types.
      List<String> items = itemset.javaItems();
      long freq = itemset.freq();
    }
  }
}
