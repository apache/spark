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

package org.apache.spark.launcher;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.*;

public class SparkClassCommandBuilderSuite extends BaseSuite {

  @Test
  public void testBeelineBuilder() throws Exception {
    List<String> args = Arrays.asList("myBeelineArg");
    SparkClassCommandBuilder builder =
      new SparkClassCommandBuilder("org.apache.hive.beeline.BeeLine", args);
    List<String> strings = builder.buildCommand(new HashMap<>());
    assertTrue(strings.contains("-DmyKey=yourValue"));
    assertTrue(strings.contains("myBeelineArg"));
  }
}
