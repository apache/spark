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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

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

    CalendarInterval result = new CalendarInterval(-5 * 12 + 23, 0);
    Arrays.asList(
      "interval   -5  years  23   month",
      "  -5  years  23   month",
      "interval   -5  years  23   month   ",
      "  -5  years  23   month   ",
      "  interval   -5  years  23   month   ").forEach(input ->
      assertEquals(fromString(input), result)
    );

    // Error cases
    Arrays.asList(
      "interval   3month 1 hour",
      "3month 1 hour",
      "interval 3 moth 1 hour",
      "3 moth 1 hour",
      "interval",
      "int",
      "",
      null).forEach(input -> assertNull(fromString(input)));
  }

  @Test
  public void fromCaseInsensitiveStringTest() {
    for (String input : new String[]{"5 MINUTES", "5 minutes", "5 Minutes"}) {
      assertEquals(fromCaseInsensitiveString(input), new CalendarInterval(0, 5L * 60 * 1_000_000));
    }

    for (String input : new String[]{null, "", " "}) {
      try {
        fromCaseInsensitiveString(input);
        fail("Expected to throw an exception for the invalid input");
      } catch (IllegalArgumentException e) {
        String msg = e.getMessage();
        if (input == null) assertTrue(msg.contains("cannot be null"));
        else assertTrue(msg.contains("cannot be blank"));
      }
    }

    for (String input : new String[]{"interval", "interval1 day", "foo", "foo 1 day"}) {
      try {
        fromCaseInsensitiveString(input);
        fail("Expected to throw an exception for the invalid input");
      } catch (IllegalArgumentException e) {
        String msg = e.getMessage();
        if (input.trim().equalsIgnoreCase("interval")) {
          assertTrue(msg.contains("Interval string must have time units"));
        } else {
          assertTrue(msg.contains("Invalid interval:"));
        }
      }
    }
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
  public void fromSingleUnitStringTest() {
    String input;
    CalendarInterval i;

    input = "12";
    i = new CalendarInterval(12 * 12, 0L);
    assertEquals(fromSingleUnitString("year", input), i);

    input = "100";
    i = new CalendarInterval(0, 100 * MICROS_PER_DAY);
    assertEquals(fromSingleUnitString("day", input), i);

    input = "1999.38888";
    i = new CalendarInterval(0, 1999 * MICROS_PER_SECOND + 38);
    assertEquals(fromSingleUnitString("second", input), i);

    try {
      input = String.valueOf(Integer.MAX_VALUE);
      fromSingleUnitString("year", input);
      fail("Expected to throw an exception for the invalid input");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("outside range"));
    }

    try {
      input = String.valueOf(Long.MAX_VALUE / MICROS_PER_HOUR + 1);
      fromSingleUnitString("hour", input);
      fail("Expected to throw an exception for the invalid input");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("outside range"));
    }
  }

  @Test
  public void addTest() {
    String input = "interval 3 month 1 hour";
    String input2 = "interval 2 month 100 hour";

    CalendarInterval interval = fromString(input);
    CalendarInterval interval2 = fromString(input2);

    assertEquals(interval.add(interval2), new CalendarInterval(5, 101 * MICROS_PER_HOUR));

    input = "interval -10 month -81 hour";
    input2 = "interval 75 month 200 hour";

    interval = fromString(input);
    interval2 = fromString(input2);

    assertEquals(interval.add(interval2), new CalendarInterval(65, 119 * MICROS_PER_HOUR));
  }

  @Test
  public void subtractTest() {
    String input = "interval 3 month 1 hour";
    String input2 = "interval 2 month 100 hour";

    CalendarInterval interval = fromString(input);
    CalendarInterval interval2 = fromString(input2);

    assertEquals(interval.subtract(interval2), new CalendarInterval(1, -99 * MICROS_PER_HOUR));

    input = "interval -10 month -81 hour";
    input2 = "interval 75 month 200 hour";

    interval = fromString(input);
    interval2 = fromString(input2);

    assertEquals(interval.subtract(interval2), new CalendarInterval(-85, -281 * MICROS_PER_HOUR));
  }

  private static void testSingleUnit(String unit, int number, int months, long microseconds) {
    Arrays.asList("interval ", "").forEach(prefix -> {
      String input1 = prefix + number + " " + unit;
      String input2 = prefix + number + " " + unit + "s";
      CalendarInterval result = new CalendarInterval(months, microseconds);
      assertEquals(fromString(input1), result);
      assertEquals(fromString(input2), result);
    });
  }

  @Test
  public void fromStringCaseSensitivityTest() {
    testSingleUnit("YEAR", 3, 36, 0);
    testSingleUnit("Month", 3, 3, 0);
    testSingleUnit("Week", 3, 0, 3 * MICROS_PER_WEEK);
    testSingleUnit("DAY", 3, 0, 3 * MICROS_PER_DAY);
    testSingleUnit("HouR", 3, 0, 3 * MICROS_PER_HOUR);
    testSingleUnit("MiNuTe", 3, 0, 3 * MICROS_PER_MINUTE);
    testSingleUnit("Second", 3, 0, 3 * MICROS_PER_SECOND);
    testSingleUnit("MilliSecond", 3, 0, 3 * MICROS_PER_MILLI);
    testSingleUnit("MicroSecond", 3, 0, 3);

    String input;

    input = "INTERVAL -5 YEARS 23 MONTHS";
    CalendarInterval result = new CalendarInterval(-5 * 12 + 23, 0);
    assertEquals(fromString(input), result);

    assertNull(fromString("INTERVAL"));
    assertNull(fromString("  Interval "));
  }

  @Test
  public void durationTest() {
    assertEquals(fromString("0 seconds").getDuration(31, TimeUnit.MILLISECONDS), 0);
    assertEquals(fromString("1 month").getDuration(31, TimeUnit.DAYS), 31);
    assertEquals(fromString("1 microsecond").getDuration(30, TimeUnit.MICROSECONDS), 1);
    assertEquals(fromString("1 month -30 days").getDuration(31, TimeUnit.DAYS), 1);

    try {
      fromString(Integer.MAX_VALUE + " month").getDuration(31, TimeUnit.SECONDS);
      fail("Expected to throw an exception for the invalid input");
    } catch (ArithmeticException e) {
      assertTrue(e.getMessage().contains("overflow"));
    }
  }

  @Test
  public void pisitiveIntervalTest() {
    assertTrue(fromString("1 months").isPositive(28));
    assertTrue(fromString("1 microsecond").isPositive(30));
    assertTrue(fromString("1 year -360 days").isPositive(31));
    assertTrue(fromString("-1 year 380 days").isPositive(31));

    assertFalse(fromString("0 months").isPositive(28));
    assertFalse(fromString("-1 month 30 days").isPositive(31));
    assertFalse(fromString("2 months -60 days").isPositive(30));
  }
}
