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

    i = new CalendarInterval(34, 0);
    assertEquals(i.toString(), "interval 2 years 10 months");

    i = new CalendarInterval(-34, 0);
    assertEquals(i.toString(), "interval -2 years -10 months");

    i = new CalendarInterval(0, 3 * MICROS_PER_WEEK + 13 * MICROS_PER_HOUR + 123);
    assertEquals(i.toString(), "interval 3 weeks 13 hours 123 microseconds");

    i = new CalendarInterval(0, -3 * MICROS_PER_WEEK - 13 * MICROS_PER_HOUR - 123);
    assertEquals(i.toString(), "interval -3 weeks -13 hours -123 microseconds");

    i = new CalendarInterval(34, 3 * MICROS_PER_WEEK + 13 * MICROS_PER_HOUR + 123);
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
    CalendarInterval result = new CalendarInterval(-5 * 12 + 23, 0);
    assertEquals(CalendarInterval.fromString(input), result);

    input = "interval   -5  years  23   month   ";
    assertEquals(CalendarInterval.fromString(input), result);

    input = "  interval   -5  years  23   month   ";
    assertEquals(CalendarInterval.fromString(input), result);

    // Error cases
    input = "interval   3month 1 hour";
    assertEquals(CalendarInterval.fromString(input), null);

    input = "interval 3 moth 1 hour";
    assertEquals(CalendarInterval.fromString(input), null);

    input = "interval";
    assertEquals(CalendarInterval.fromString(input), null);

    input = "int";
    assertEquals(CalendarInterval.fromString(input), null);

    input = "";
    assertEquals(CalendarInterval.fromString(input), null);

    input = null;
    assertEquals(CalendarInterval.fromString(input), null);
  }

  @Test
  public void fromYearMonthStringTest() {
    String input;
    CalendarInterval i;

    input = "99-10";
    i = new CalendarInterval(99 * 12 + 10, 0L);
    assertEquals(CalendarInterval.fromYearMonthString(input), i);

    input = "-8-10";
    i = new CalendarInterval(-8 * 12 - 10, 0L);
    assertEquals(CalendarInterval.fromYearMonthString(input), i);

    try {
      input = "99-15";
      CalendarInterval.fromYearMonthString(input);
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
    assertEquals(CalendarInterval.fromDayTimeString(input), i);

    input = "10 0:12:0.888";
    i = new CalendarInterval(0, 10 * MICROS_PER_DAY + 12 * MICROS_PER_MINUTE);
    assertEquals(CalendarInterval.fromDayTimeString(input), i);

    input = "-3 0:0:0";
    i = new CalendarInterval(0, -3 * MICROS_PER_DAY);
    assertEquals(CalendarInterval.fromDayTimeString(input), i);

    try {
      input = "5 30:12:20";
      CalendarInterval.fromDayTimeString(input);
      fail("Expected to throw an exception for the invalid input");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("hour 30 outside range"));
    }

    try {
      input = "5 30-12";
      CalendarInterval.fromDayTimeString(input);
      fail("Expected to throw an exception for the invalid input");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("not match day-time format"));
    }
  }

  @Test
  public void fromSingleUnitStringTest() {
    String input;
    CalendarInterval i;

    input = "12";
    i = new CalendarInterval(12 * 12, 0L);
    assertEquals(CalendarInterval.fromSingleUnitString("year", input), i);

    input = "100";
    i = new CalendarInterval(0, 100 * MICROS_PER_DAY);
    assertEquals(CalendarInterval.fromSingleUnitString("day", input), i);

    input = "1999.38888";
    i = new CalendarInterval(0, 1999 * MICROS_PER_SECOND + 38);
    assertEquals(CalendarInterval.fromSingleUnitString("second", input), i);

    try {
      input = String.valueOf(Integer.MAX_VALUE);
      CalendarInterval.fromSingleUnitString("year", input);
      fail("Expected to throw an exception for the invalid input");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("outside range"));
    }

    try {
      input = String.valueOf(Long.MAX_VALUE / MICROS_PER_HOUR + 1);
      CalendarInterval.fromSingleUnitString("hour", input);
      fail("Expected to throw an exception for the invalid input");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("outside range"));
    }
  }

  @Test
  public void addTest() {
    String input = "interval 3 month 1 hour";
    String input2 = "interval 2 month 100 hour";

    CalendarInterval interval = CalendarInterval.fromString(input);
    CalendarInterval interval2 = CalendarInterval.fromString(input2);

    assertEquals(interval.add(interval2), new CalendarInterval(5, 101 * MICROS_PER_HOUR));

    input = "interval -10 month -81 hour";
    input2 = "interval 75 month 200 hour";

    interval = CalendarInterval.fromString(input);
    interval2 = CalendarInterval.fromString(input2);

    assertEquals(interval.add(interval2), new CalendarInterval(65, 119 * MICROS_PER_HOUR));
  }

  @Test
  public void subtractTest() {
    String input = "interval 3 month 1 hour";
    String input2 = "interval 2 month 100 hour";

    CalendarInterval interval = CalendarInterval.fromString(input);
    CalendarInterval interval2 = CalendarInterval.fromString(input2);

    assertEquals(interval.subtract(interval2), new CalendarInterval(1, -99 * MICROS_PER_HOUR));

    input = "interval -10 month -81 hour";
    input2 = "interval 75 month 200 hour";

    interval = CalendarInterval.fromString(input);
    interval2 = CalendarInterval.fromString(input2);

    assertEquals(interval.subtract(interval2), new CalendarInterval(-85, -281 * MICROS_PER_HOUR));
  }

  private void testSingleUnit(String unit, int number, int months, long microseconds) {
    String input1 = "interval " + number + " " + unit;
    String input2 = "interval " + number + " " + unit + "s";
    CalendarInterval result = new CalendarInterval(months, microseconds);
    assertEquals(CalendarInterval.fromString(input1), result);
    assertEquals(CalendarInterval.fromString(input2), result);
  }
}
