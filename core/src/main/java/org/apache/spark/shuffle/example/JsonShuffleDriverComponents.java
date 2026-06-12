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

package org.apache.spark.shuffle.example;

import java.util.Collections;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.api.ShuffleDriverComponents;

public class JsonShuffleDriverComponents implements ShuffleDriverComponents {

  private final SparkConf sparkConf;

  public JsonShuffleDriverComponents(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
  }

  @Override
  public Map<String, String> initializeApplication() {
    String outputDir = sparkConf.get("spark.shuffle.sort.io.json.outputDir", "/tmp/json-shuffle");
    return Collections.singletonMap("spark.shuffle.sort.io.json.outputDir", outputDir);
  }

  @Override
  public void cleanupApplication() {
    // No global cleanup performed by this example driver component.
  }

  @Override
  public void registerShuffle(int shuffleId) {
    // This example does not maintain shuffle registration state.
  }

  @Override
  public void removeShuffle(int shuffleId, boolean blocking) {
    // In a real external shuffle store, remove shuffle-specific metadata/data here.
  }

  @Override
  public boolean supportsReliableStorage() {
    return true;
  }
}
