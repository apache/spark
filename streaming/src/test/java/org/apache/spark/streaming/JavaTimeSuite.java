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

package org.apache.spark.streaming;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JavaTimeSuite {

  // Just testing the methods that are specially exposed for Java.
  // This does not repeat all tests found in the Scala suite.

  @Test
  public void testLess() {
    Assertions.assertTrue(new Time(999).less(new Time(1000)));
  }

  @Test
  public void testLessEq() {
    Assertions.assertTrue(new Time(1000).lessEq(new Time(1000)));
  }

  @Test
  public void testGreater() {
    Assertions.assertTrue(new Time(1000).greater(new Time(999)));
  }

  @Test
  public void testGreaterEq() {
    Assertions.assertTrue(new Time(1000).greaterEq(new Time(1000)));
  }

  @Test
  public void testPlus() {
    Assertions.assertEquals(new Time(1100), new Time(1000).plus(new Duration(100)));
  }

  @Test
  public void testMinusTime() {
    Assertions.assertEquals(new Duration(900), new Time(1000).minus(new Time(100)));
  }

  @Test
  public void testMinusDuration() {
    Assertions.assertEquals(new Time(900), new Time(1000).minus(new Duration(100)));
  }

}
