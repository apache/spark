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

package org.apache.spark;

import org.junit.Test;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

// The test suite itself is Serializable so that anonymous Function implementations can be
// serialized, as an alternative to converting these anonymous classes to static inner classes;
// see http://stackoverflow.com/questions/758570/.
public class GuavaSuite implements Serializable {
  /**
   * Test for SPARK-3647. This test needs to use the maven-built assembly to trigger the issue,
   * since that's the only artifact where Guava classes have been relocated.
   */
  @Test
  public void testGuavaOptional() {
    JavaSparkContext localCluster = new JavaSparkContext("local-cluster[1,1,1024]", "JavaAPISuite");
    try {
      JavaRDD<Integer> rdd1 = localCluster.parallelize(Arrays.asList(1, 2, null), 3);
      JavaRDD<Optional<Integer>> rdd2 = rdd1.map(
        new Function<Integer, Optional<Integer>>() {
          @Override
          public Optional<Integer> call(Integer i) {
            return Optional.fromNullable(i);
          }
        });
      rdd2.collect();
    } finally {
      localCluster.stop();
    }
  }

}
