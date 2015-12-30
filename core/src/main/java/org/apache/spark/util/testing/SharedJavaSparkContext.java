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

/** Shares a local `SparkContext` between all tests in a suite and closes it at the end */
public class SharedJavaSparkContext {
  private static transient SparkContext _sc;
  private static transient JavaSparkContext _jsc;
  public static SparkConf _conf = new SparkConf().
    setMaster("local[4]").
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
  public synchronized void runBefore() {
    if (_sc != null) {
      _sc = new SparkContext(conf());
      _jsc = new JavaSparkContext(_sc);
    }
  }

  @AfterClass
  static public void runAfterClass() {
    LocalSparkContext$.MODULE$.stop(_sc);
    _sc = null;
    _jsc = null;
  }
}
