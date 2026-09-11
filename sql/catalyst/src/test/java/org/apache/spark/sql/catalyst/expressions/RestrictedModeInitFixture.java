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
package org.apache.spark.sql.catalyst.expressions;

/**
 * A test fixture for a {@code reflect} target whose class initializer has an observable side
 * effect: it sets the {@link #INIT_PROPERTY} system property. A test can read that property to
 * check whether this class was ever initialized without initializing it itself -- {@code classOf}
 * and reading the compile-time constant {@link #INIT_PROPERTY} do not trigger initialization.
 */
public final class RestrictedModeInitFixture {

  public static final String INIT_PROPERTY =
      "spark.test.restrictedModeInitFixture.initialized";

  static {
    System.setProperty(INIT_PROPERTY, "true");
  }

  public static String touch() {
    return "touched";
  }
}
