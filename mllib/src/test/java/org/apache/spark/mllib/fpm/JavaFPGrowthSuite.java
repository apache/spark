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
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.google.common.collect.Lists;
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
    JavaRDD<ArrayList<String>> rdd = sc.parallelize(Lists.newArrayList(
      Lists.newArrayList("r z h k p".split(" ")),
      Lists.newArrayList("z y x w v u t s".split(" ")),
      Lists.newArrayList("s x o n r".split(" ")),
      Lists.newArrayList("x z y m t s q e".split(" ")),
      Lists.newArrayList("z".split(" ")),
      Lists.newArrayList("x z y r q t p".split(" "))), 2);

    FPGrowth fpg = new FPGrowth();

    FPGrowthModel<String> model6 = fpg
      .setMinSupport(0.9)
      .setNumPartitions(1)
      .run(rdd);
    assertEquals(0, model6.javaFreqItemsets().count());

    FPGrowthModel<String> model3 = fpg
      .setMinSupport(0.5)
      .setNumPartitions(2)
      .run(rdd);
    assertEquals(18, model3.javaFreqItemsets().count());

    FPGrowthModel<String> model2 = fpg
      .setMinSupport(0.3)
      .setNumPartitions(4)
      .run(rdd);
    assertEquals(54, model2.javaFreqItemsets().count());

    FPGrowthModel<String> model1 = fpg
      .setMinSupport(0.1)
      .setNumPartitions(8)
      .run(rdd);
    assertEquals(625, model1.javaFreqItemsets().count());
  }
}
