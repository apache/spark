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

import org.junit.Assert;
import org.junit.Test;

public class JavaDurationSuite {

  // Just testing the methods that are specially exposed for Java.
  // This does not repeat all tests found in the Scala suite.

  @Test
  public void testLess() {
    Assert.assertTrue(new Duration(999).less(new Duration(1000)));
  }

  @Test
  public void testLessEq() {
    Assert.assertTrue(new Duration(1000).lessEq(new Duration(1000)));
  }

  @Test
  public void testGreater() {
    Assert.assertTrue(new Duration(1000).greater(new Duration(999)));
  }

  @Test
  public void testGreaterEq() {
    Assert.assertTrue(new Duration(1000).greaterEq(new Duration(1000)));
  }

  @Test
  public void testPlus() {
    Assert.assertEquals(new Duration(1100), new Duration(1000).plus(new Duration(100)));
  }

  @Test
  public void testMinus() {
    Assert.assertEquals(new Duration(900), new Duration(1000).minus(new Duration(100)));
  }

  @Test
  public void testTimes() {
    Assert.assertEquals(new Duration(200), new Duration(100).times(2));
  }

  @Test
  public void testDiv() {
    Assert.assertEquals(200.0, new Duration(1000).div(new Duration(5)), 1.0e-12);
  }

  @Test
  public void testMilliseconds() {
    Assert.assertEquals(new Duration(100), Durations.milliseconds(100));
  }

  @Test
  public void testSeconds() {
    Assert.assertEquals(new Duration(30 * 1000), Durations.seconds(30));
  }

  @Test
  public void testMinutes() {
    Assert.assertEquals(new Duration(2 * 60 * 1000), Durations.minutes(2));
  }

}
