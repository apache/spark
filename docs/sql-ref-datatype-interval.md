---
layout: global
title: Data Types - Interval
displayTitle: Data Types - Interval
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

## SQL Keyword

INTERVAL
    
## Definition

The interval type represents fixed periods of time comprising values of fields `months`, `days` and `microseconds`.
You can use intervals to add or subtract a period of time to/from a date, or timestamp value.

### Internals

The interval is stored internally in three components:
 
  - an integer value representing the number of `months` in this interval
  - an integer value representing the number of `days` in this interval
  - a long value representing the number of `microseconds` in this interval

The `months` and `days` are not units of time with a constant length (unlike hours, seconds), so
they are two separated fields from microseconds.
One month may be equal to 28, 29, 30 or 31 days and one day may be equal to 23, 24 or 25 hours (daylight saving).

### Supported Units

| Unit | Remarks |
|---|---|
| YEAR[S] | Stored as 12 months per year in the `months` part |
| MONTH[S] | | 
| WEEK[S] | Stored as 7 days per week in the `days` part |
| DAY[S] | |
| HOUR[S] | Stored as 3,600,000,000 microseconds per hour in the `microseconds` part |
| MINUTE[S] | Stored as 60,000,000 microseconds per hour in the `microseconds` part |
| SECOND[S] | Stored as 1,000,000 microseconds per second in the `microseconds` part |
| MILLISECOND[S] | Stored as 1,000 microseconds per millisecond in the `microseconds` part |
| MICROSECOND[S] |  |

## Constants

Please refer to the section [Interval Literals](sql-ref-literals.html#interval-literal) to learn more.

## Functions and Operations

### Operations

| Operation | Example | Result | Result Type |
| --- | --- | --- | --- |
| date + interval | SELECT DATE '2020-02-29' + INTERVAL '1 month 1 day' | 2020-03-30 | date |
| interval + date | SELECT INTERVAL '1 month 1 day' + DATE '2020-02-29' | 2020-03-30 | date |
| date - interval | SELECT DATE '2020-03-30' - INTERVAL '1 month 1 day' | 2020-02-28 | date |
| timestamp + interval | SELECT TIMESTAMP '2020-02-29 11:22:33.44' + INTERVAL '1 month 1 day 1 hour' | 2020-03-30 12:22:33.44 | timestamp |
| interval + timestamp | SELECT INTERVAL '1 month 1 day 1 hour' + TIMESTAMP '2020-02-29 11:22:33.44' | 2020-03-30 12:22:33.44 | timestamp |
| timestamp - interval | SELECT TIMESTAMP '2020-02-29 11:22:33.44' - INTERVAL '1 month 1 day 1 hour' | 2020-01-28 10:22:33.44 | timestamp |
| string + interval | SELECT '2020-02-29 11:22:33.44' + INTERVAL '1 month 1 day 1 hour' | 2020-03-30 12:22:33.44 | string |
| interval + string | SELECT INTERVAL '1 month 1 day 1 hour' + '2020-02-29 11:22:33.44' | 2020-03-30 12:22:33.44 | string |
| string - interval | SELECT '2020-02-29 11:22:33.44' - INTERVAL '1 month 1 day 1 hour' | 2020-01-28 10:22:33.44 | string |
| interval + interval | SELECT INTERVAL '1 month 1 day 1 hour' + INTERVAL '2 weeks 5 seconds' | 1 months 15 days 1 hours 5 seconds | interval |
| interval - interval | SELECT INTERVAL '1 month 1 day 1 hour' - INTERVAL '2 weeks 5 seconds' | 1 months -13 days 59 minutes 55 seconds | interval|
| interval * numeric | SELECT INTERVAL '1 month 1 day 1 hour' * 2.5 | 2 months 2 days 14 hours 30 minutes | interval |
| numeric * interval | SELECT 2.5 * INTERVAL '1 month 1 day 1 hour'| 2 months 2 days 14 hours 30 minutes | interval |
| interval / numeric | SELECT INTERVAL '1 month 1 day 1 hour' / 2.5 | 10 hours| interval |
| date - date | SELECT DATE '2022-07-28' - DATE '2020-02-29' | 2 years 4 months 29 days | interval |
| timestamp - timestamp | SELECT TIMESTAMP '2022-07-29 11:22:33.44' - TIMESTAMP '2020-02-29 12:23:34.45' | 21142 hours 58 minutes 58.99 seconds | interval |
| - interval | SELECT -INTERVAL '1 month 1 day 1 hour' | -1 months -1 days -1 hours | interval |
| + interval | SELECT +INTERVAL '1 month 1 day 1 hour' | -1 months -1 days -1 hours | interval |


### Additional Remarks

  - It's better to use intervals that contains no time part units, e.g. `INTERVAL '1 month 1 day'`,
    both for better performance and unambiguity, when using them to add to or subtract from a date value.
    For historical reasons, Spark supports intervals with times added to or subtracted from dates,
    but makes these operators being timezone aware and inefficient. 
  - It performs a 'Big-Unit-First' order, when using them to add to or subtract from a date or timestamp value. For example,
    for query `SELECT DATE '2020-02-29' + INTERVAL '1 month 1 day' - INTERVAL '1 month 1 day'`, you will get `2020-02-28` instead of
    the original value `2020-02-29`, with the order of `((((DATE '2020-02-29' + 1 month) + 1 day) - 1 month) - 1 day)`.
    If you want these unit be performed in a clear order, you need to split them into separate part first, then add or subtract them
    one by one, e.g.
    ```sql
    SELECT (DATE '2020-02-29' + INTERVAL '1 month' + INTERVAL '1 day') AS col
    +----------+
    |       col|
    +----------+
    |2020-03-30|
    +----------+
    SELECT (DATE '2020-02-29' + INTERVAL '1 day' + INTERVAL '1 month') as col
    +----------+
    |       col|
    +----------+
    |2020-04-01|
    +----------+
    ```
  - For operations that involving strings and intervals, the string values should be valid datetime representations.

### Functions

<!-- TODO We do not have an auto generated datetime/interval function group yet -->
Please refer to the section [Built-in Functions](sql-ref-functions.html) to learn more about those functions involving intervals.
