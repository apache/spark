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

package org.apache.spark.unsafe.types;

import org.junit.Test;

import static junit.framework.Assert.*;
import static org.apache.spark.unsafe.types.Interval.*;

public class IntervalSuite {

  @Test
  public void equalsTest() {
    Interval i1 = new Interval(3, 123);
    Interval i2 = new Interval(3, 321);
    Interval i3 = new Interval(1, 123);
    Interval i4 = new Interval(3, 123);

    assertNotSame(i1, i2);
    assertNotSame(i1, i3);
    assertNotSame(i2, i3);
    assertEquals(i1, i4);
  }

  @Test
  public void toStringTest() {
    Interval i;

    i = new Interval(34, 0);
    assertEquals(i.toString(), "interval 2 years 10 months");

    i = new Interval(-34, 0);
    assertEquals(i.toString(), "interval -2 years -10 months");

    i = new Interval(0, 3 * MICROS_PER_WEEK + 13 * MICROS_PER_HOUR + 123);
    assertEquals(i.toString(), "interval 3 weeks 13 hours 123 microseconds");

    i = new Interval(0, -3 * MICROS_PER_WEEK - 13 * MICROS_PER_HOUR - 123);
    assertEquals(i.toString(), "interval -3 weeks -13 hours -123 microseconds");

    i = new Interval(34, 3 * MICROS_PER_WEEK + 13 * MICROS_PER_HOUR + 123);
    assertEquals(i.toString(), "interval 2 years 10 months 3 weeks 13 hours 123 microseconds");
  }

  @Test
  public void fromStringTest() {
    testSingleUnit("year", 3, 36, 0);
    testSingleUnit("month", 3, 3, 0);
    testSingleUnit("week", 3, 0, 3 * MICROS_PER_WEEK);
    testSingleUnit("day", 3, 0, 3 * MICROS_PER_DAY);
    testSingleUnit("hour", 3, 0, 3 * MICROS_PER_HOUR);
    testSingleUnit("minute", 3, 0, 3 * MICROS_PER_MINUTE);
    testSingleUnit("second", 3, 0, 3 * MICROS_PER_SECOND);
    testSingleUnit("millisecond", 3, 0, 3 * MICROS_PER_MILLI);
    testSingleUnit("microsecond", 3, 0, 3);

    String input;

    input = "interval   -5  years  23   month";
    Interval result = new Interval(-5 * 12 + 23, 0);
    assertEquals(Interval.fromString(input), result);

    // Error cases
    input = "interval   3month 1 hour";
    assertEquals(Interval.fromString(input), null);

    input = "interval 3 moth 1 hour";
    assertEquals(Interval.fromString(input), null);

    input = "interval";
    assertEquals(Interval.fromString(input), null);

    input = "int";
    assertEquals(Interval.fromString(input), null);

    input = "";
    assertEquals(Interval.fromString(input), null);

    input = null;
    assertEquals(Interval.fromString(input), null);
  }

  private void testSingleUnit(String unit, int number, int months, long microseconds) {
    String input1 = "interval " + number + " " + unit;
    String input2 = "interval " + number + " " + unit + "s";
    Interval result = new Interval(months, microseconds);
    assertEquals(Interval.fromString(input1), result);
    assertEquals(Interval.fromString(input2), result);
  }
}
