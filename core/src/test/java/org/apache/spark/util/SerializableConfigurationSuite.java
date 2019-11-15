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
package org.apache.spark.util;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import static org.junit.Assert.assertEquals;


public class SerializableConfigurationSuite {
  private transient JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = new JavaSparkContext("local", "SerializableConfigurationSuite");
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
  }

  @Test
  public void testSerializableConfiguration() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);
    Configuration hadoopConfiguration = new Configuration(false);
    hadoopConfiguration.set("test.property", "value");
    SerializableConfiguration scs = new SerializableConfiguration(hadoopConfiguration);
    SerializableConfiguration actual = rdd.map(val -> scs).collect().get(0);
    assertEquals("value", actual.value().get("test.property"));
  }
}
