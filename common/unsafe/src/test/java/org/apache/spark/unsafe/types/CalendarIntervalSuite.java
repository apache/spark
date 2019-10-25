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
    CalendarInterval i1 = new CalendarInterval(3, 123);
    CalendarInterval i2 = new CalendarInterval(3, 321);
    CalendarInterval i3 = new CalendarInterval(1, 123);
    CalendarInterval i4 = new CalendarInterval(3, 123);

    assertNotSame(i1, i2);
    assertNotSame(i1, i3);
    assertNotSame(i2, i3);
    assertEquals(i1, i4);
  }

  @Test
  public void toStringTest() {
    CalendarInterval i;

    i = new CalendarInterval(0, 0);
    assertEquals("interval 0 microseconds", i.toString());

    i = new CalendarInterval(34, 0);
    assertEquals("interval 2 years 10 months", i.toString());

    i = new CalendarInterval(-34, 0);
    assertEquals("interval -2 years -10 months", i.toString());

    i = new CalendarInterval(0, 3 * MICROS_PER_WEEK + 13 * MICROS_PER_HOUR + 123);
    assertEquals("interval 3 weeks 13 hours 123 microseconds", i.toString());

    i = new CalendarInterval(0, -3 * MICROS_PER_WEEK - 13 * MICROS_PER_HOUR - 123);
    assertEquals("interval -3 weeks -13 hours -123 microseconds", i.toString());

    i = new CalendarInterval(34, 3 * MICROS_PER_WEEK + 13 * MICROS_PER_HOUR + 123);
    assertEquals("interval 2 years 10 months 3 weeks 13 hours 123 microseconds", i.toString());
  }

  @Test
  public void fromYearMonthStringTest() {
    String input;
    CalendarInterval i;

    input = "99-10";
    i = new CalendarInterval(99 * 12 + 10, 0L);
    assertEquals(fromYearMonthString(input), i);

    input = "-8-10";
    i = new CalendarInterval(-8 * 12 - 10, 0L);
    assertEquals(fromYearMonthString(input), i);

    try {
      input = "99-15";
      fromYearMonthString(input);
      fail("Expected to throw an exception for the invalid input");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("month 15 outside range"));
    }
  }

  @Test
  public void fromDayTimeStringTest() {
    String input;
    CalendarInterval i;

    input = "5 12:40:30.999999999";
    i = new CalendarInterval(0, 5 * MICROS_PER_DAY + 12 * MICROS_PER_HOUR +
      40 * MICROS_PER_MINUTE + 30 * MICROS_PER_SECOND + 999999L);
    assertEquals(fromDayTimeString(input), i);

    input = "10 0:12:0.888";
    i = new CalendarInterval(0, 10 * MICROS_PER_DAY + 12 * MICROS_PER_MINUTE +
      888 * MICROS_PER_MILLI);
    assertEquals(fromDayTimeString(input), i);

    input = "-3 0:0:0";
    i = new CalendarInterval(0, -3 * MICROS_PER_DAY);
    assertEquals(fromDayTimeString(input), i);

    try {
      input = "5 30:12:20";
      fromDayTimeString(input);
      fail("Expected to throw an exception for the invalid input");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("hour 30 outside range"));
    }

    try {
      input = "5 30-12";
      fromDayTimeString(input);
      fail("Expected to throw an exception for the invalid input");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("not match day-time format"));
    }

    try {
      input = "5 1:12:20";
      fromDayTimeString(input, "hour", "microsecond");
      fail("Expected to throw an exception for the invalid convention type");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Cannot support (interval"));
    }
  }

  @Test
  public void addTest() {
    CalendarInterval input1 = new CalendarInterval(3, 1 * MICROS_PER_HOUR);
    CalendarInterval input2 = new CalendarInterval(2, 100 * MICROS_PER_HOUR);
    assertEquals(input1.add(input2), new CalendarInterval(5, 101 * MICROS_PER_HOUR));

    input1 = new CalendarInterval(-10, -81 * MICROS_PER_HOUR);
    input2 = new CalendarInterval(75, 200 * MICROS_PER_HOUR);
    assertEquals(input1.add(input2), new CalendarInterval(65, 119 * MICROS_PER_HOUR));
  }

  @Test
  public void subtractTest() {
    CalendarInterval input1 = new CalendarInterval(3, 1 * MICROS_PER_HOUR);
    CalendarInterval input2 = new CalendarInterval(2, 100 * MICROS_PER_HOUR);
    assertEquals(input1.subtract(input2), new CalendarInterval(1, -99 * MICROS_PER_HOUR));

    input1 = new CalendarInterval(-10, -81 * MICROS_PER_HOUR);
    input2 = new CalendarInterval(75, 200 * MICROS_PER_HOUR);
    assertEquals(input1.subtract(input2), new CalendarInterval(-85, -281 * MICROS_PER_HOUR));
  }

  @Test
  public void multiplyTest() {
    CalendarInterval interval = new CalendarInterval(0, 0);
    assertEquals(interval, interval.multiply(0));

    interval = new CalendarInterval(123, 456);
    assertEquals(new CalendarInterval(123 * 42, 456 * 42), interval.multiply(42));

    interval = new CalendarInterval(-123, -456);
    assertEquals(new CalendarInterval(-123 * 42, -456 * 42), interval.multiply(42));

    assertEquals(
      fromString("interval 1 month 22 days 12 hours"),
      fromString("interval 1 month 5 days").multiply(1.5));

    try {
      interval = new CalendarInterval(2, 0);
      interval.multiply(Integer.MAX_VALUE);
      fail("Expected to throw an exception on months overflow");
    } catch (java.lang.ArithmeticException e) {
      assertTrue(e.getMessage().contains("overflow"));
    }
  }

    @Test
    public void divideTest() {
      CalendarInterval interval = new CalendarInterval(0, 0);
      assertEquals(interval, interval.divide(10));

      interval = fromString("1 month 30 seconds");
      assertEquals(fromString("15 days 15 seconds"), interval.divide(2));
      assertEquals(fromString("2 months 1 minute"), interval.divide(0.5));

      interval = fromString("-1 month -30 seconds");
      assertEquals(fromString("-15 days -15 seconds"), interval.divide(2));
      assertEquals(fromString("-2 months -1 minute"), interval.divide(0.5));

      try {
        interval = new CalendarInterval(123, 456);
        interval.divide(0);
        fail("Expected to throw an exception on divide by zero");
      } catch (java.lang.ArithmeticException e) {
        assertTrue(e.getMessage().contains("overflow"));
      }
    }
}
