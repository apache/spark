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

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset;

public class JavaAssociationRulesSuite extends SharedSparkSession {

  @Test
  public void runAssociationRules() {

    JavaRDD<FPGrowth.FreqItemset<String>> freqItemsets = jsc.parallelize(Arrays.asList(
      new FreqItemset<>(new String[]{"a"}, 15L),
      new FreqItemset<>(new String[]{"b"}, 35L),
      new FreqItemset<>(new String[]{"a", "b"}, 12L)
    ));

    JavaRDD<AssociationRules.Rule<String>> results = (new AssociationRules()).run(freqItemsets);
  }
}
