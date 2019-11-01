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

import static org.junit.Assert.*;
import static org.apache.spark.unsafe.types.CalendarInterval.*;

public class CalendarIntervalSuite {

  @Test
  public void equalsTest() {
    CalendarInterval i1 = new CalendarInterval(3, 2, 123);
    CalendarInterval i2 = new CalendarInterval(3, 2,321);
    CalendarInterval i3 = new CalendarInterval(3, 4,123);
    CalendarInterval i4 = new CalendarInterval(1, 2, 123);
    CalendarInterval i5 = new CalendarInterval(1, 4, 321);
    CalendarInterval i6 = new CalendarInterval(3, 2, 123);

    assertNotSame(i1, i2);
    assertNotSame(i1, i3);
    assertNotSame(i1, i4);
    assertNotSame(i2, i3);
    assertNotSame(i2, i4);
    assertNotSame(i3, i4);
    assertNotSame(i1, i5);
    assertEquals(i1, i6);
  }

  @Test
  public void toStringTest() {
    CalendarInterval i;

    i = new CalendarInterval(0, 0, 0);
    assertEquals("interval 0 microseconds", i.toString());

    i = new CalendarInterval(34, 0, 0);
    assertEquals("interval 2 years 10 months", i.toString());

    i = new CalendarInterval(-34, 0, 0);
    assertEquals("interval -2 years -10 months", i.toString());

    i = new CalendarInterval(0, 31, 0);
    assertEquals("interval 4 weeks 3 days", i.toString());

    i = new CalendarInterval(0, -31, 0);
    assertEquals("interval -4 weeks -3 days", i.toString());

    i = new CalendarInterval(0, 0, 3 * MICROS_PER_HOUR + 13 * MICROS_PER_MINUTE + 123);
    assertEquals("interval 3 hours 13 minutes 123 microseconds", i.toString());

    i = new CalendarInterval(0, 0, -3 * MICROS_PER_HOUR - 13 * MICROS_PER_MINUTE - 123);
    assertEquals("interval -3 hours -13 minutes -123 microseconds", i.toString());

    i = new CalendarInterval(34, 31, 3 * MICROS_PER_HOUR + 13 * MICROS_PER_MINUTE + 123);
    assertEquals("interval 2 years 10 months 4 weeks 3 days 3 hours 13 minutes 123 microseconds",
      i.toString());
  }

  @Test
  public void addTest() {
    CalendarInterval input1 = new CalendarInterval(3, 1, 1 * MICROS_PER_HOUR);
    CalendarInterval input2 = new CalendarInterval(2, 4, 100 * MICROS_PER_HOUR);
    assertEquals(input1.add(input2), new CalendarInterval(5, 5, 101 * MICROS_PER_HOUR));

    input1 = new CalendarInterval(-10, -30, -81 * MICROS_PER_HOUR);
    input2 = new CalendarInterval(75, 150, 200 * MICROS_PER_HOUR);
    assertEquals(input1.add(input2), new CalendarInterval(65, 120, 119 * MICROS_PER_HOUR));
  }

  @Test
  public void subtractTest() {
    CalendarInterval input1 = new CalendarInterval(3, 1, 1 * MICROS_PER_HOUR);
    CalendarInterval input2 = new CalendarInterval(2, 4, 100 * MICROS_PER_HOUR);
    assertEquals(input1.subtract(input2), new CalendarInterval(1, -3, -99 * MICROS_PER_HOUR));

    input1 = new CalendarInterval(-10, -30, -81 * MICROS_PER_HOUR);
    input2 = new CalendarInterval(75, 150, 200 * MICROS_PER_HOUR);
    assertEquals(input1.subtract(input2), new CalendarInterval(-85, -180, -281 * MICROS_PER_HOUR));
  }

  @Test
  public void multiplyTest() {
    CalendarInterval interval = new CalendarInterval(0, 0, 0);
    assertEquals(interval, interval.multiply(0));

    interval = new CalendarInterval(123, 456, 789);
    assertEquals(new CalendarInterval(123 * 42, 456 * 42, 789 * 42), interval.multiply(42));

    interval = new CalendarInterval(-123, -456, -789);
    assertEquals(new CalendarInterval(-123 * 42, -456 * 42, -789 * 42), interval.multiply(42));

    assertEquals(
      new CalendarInterval(1, 22, 12 * MICROS_PER_HOUR),
      new CalendarInterval(1, 5, 0).multiply(1.5));
    assertEquals(
      new CalendarInterval(2, 14, 12 * MICROS_PER_HOUR),
      new CalendarInterval(2, 2, 2 * MICROS_PER_HOUR).multiply(1.2));

    try {
      interval = new CalendarInterval(2, 0, 0);
      interval.multiply(Integer.MAX_VALUE);
      fail("Expected to throw an exception on months overflow");
    } catch (java.lang.ArithmeticException e) {
      assertTrue(e.getMessage().contains("overflow"));
    }
  }

    @Test
    public void divideTest() {
      CalendarInterval interval = new CalendarInterval(0, 0, 0);
      assertEquals(interval, interval.divide(10));

      interval = new CalendarInterval(1, 0, 30 * MICROS_PER_SECOND);
      assertEquals(
        new CalendarInterval(0, 15, 15 * MICROS_PER_SECOND),
        interval.divide(2));
      assertEquals(
        new CalendarInterval(2, 0, MICROS_PER_MINUTE),
        interval.divide(0.5));

      interval = new CalendarInterval(-1, 0, -30 * MICROS_PER_SECOND);
      assertEquals(
        new CalendarInterval(0, -15, -15 * MICROS_PER_SECOND),
        interval.divide(2));
      assertEquals(
        new CalendarInterval(-2, 0, -1 * MICROS_PER_MINUTE),
        interval.divide(0.5));

      try {
        interval = new CalendarInterval(123, 456, 789);
        interval.divide(0);
        fail("Expected to throw an exception on divide by zero");
      } catch (java.lang.ArithmeticException e) {
        assertTrue(e.getMessage().contains("overflow"));
      }
    }
}
