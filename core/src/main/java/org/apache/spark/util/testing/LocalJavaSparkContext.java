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
package org.apache.spark.util.testing;

import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.junit.*;

/**
 * Creates a local `SparkContext` and `JavaSparkContext` for each test in a suite and
 * shutting down the SparkContext. The `conf` variable is used when constructing the
 * `SparkContext` and defaults to  mode with 1 core.
 * Extend this class to provide a JavaSparkContext for use with JUnit based tests
 * when individual tests may be destructive to the SparkContext.
 */
public class LocalJavaSparkContext {
  private static transient SparkContext _sc;
  private static transient JavaSparkContext _jsc;
  /**
   * SparkConf used to create the SparkContext for testing.
   * Override to change the master, application name, disable web ui
   * or other changes.
   */
  public static SparkConf _conf = new SparkConf().
    setMaster("local").
    setAppName("test");

  public SparkConf conf() {
    return _conf;
  }

  public SparkContext sc() {
    return _sc;
  }

  public JavaSparkContext jsc() {
    return _jsc;
  }

  @Before
  public void runBefore() {
    _sc = new SparkContext(conf());
    _jsc = new JavaSparkContext(_sc);
  }

  @After
  public void runAfterClass() {
    LocalSparkContext$.MODULE$.stop(_sc);
    _sc = null;
    _jsc = null;
  }
}
