---
layout: global
title: Literals
displayTitle: Literals
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

A literal (also known as a constant) represents a fixed data value. Spark SQL supports the following literals:

 * [String Literal](#string-literal)
 * [Null Literal](#null-literal)
 * [Boolean Literal](#boolean-literal)
 * [Numeric Literal](#numeric-literal)
 * [Datetime Literal](#datetime-literal)
 * [Interval Literal](#interval-literal)

### String Literal

A string literal is used to specify a character string value.

#### <em>Syntax</em>

{% highlight sql %}
'c [ ... ]' | "c [ ... ]"
{% endhighlight %}
c: one character of the user's character set.

#### <em>Examples</em>

{% highlight sql %}
SELECT 'Hello, World!' AS col;
  +-------------+
  |col          |
  +-------------+
  |Hello, World!|
  +-------------+

SELECT "SPARK SQL" AS col;
  +---------+
  |col      |
  +---------+
  |Spark SQL|
  +---------+
{% endhighlight %}

### Null Literal

A null literal is used to specify a null value.

#### <em>Syntax</em>

{% highlight sql %}
NULL
{% endhighlight %}

#### <em>Examples</em>

{% highlight sql %}
SELECT NULL AS col;
  +----+
  |col |
  +----+
  |NULL|
  +----+
{% endhighlight %}

### Boolean Literal

#### <em>Syntax</em>

{% highlight sql %}
TRUE | FALSE
{% endhighlight %}

#### <em>Examples</em>

{% highlight sql %}
SELECT TRUE AS col;
  +----+
  |col |
  +----+
  |true|
  +----+
{% endhighlight %}

### Numeric Literal

A numeric literal is used to specify a fixed or floating-point number.

#### Integer Literal

#### <em>Syntax</em>

{% highlight sql %}
[ + | - ] digit [ ... ] [ L | S | Y ]
{% endhighlight %}
digit: one of 0, 1, 2, 3, 4, 5, 6, 7, 8 or 9. <br>
default (no postfix): indicates a 4-byte signed integer number.<br>
L: case insensitive, indicates <code>BIGINT</code>, which is a 8-byte signed integer number.<br>
S: case insensitive, indicates <code>SMALLINT</code>, which is a 2-byte signed integer number.<br>
Y: case insensitive, indicates <code>TINYINT</code>, which is a 1-byte signed integer number.<br>
#### <em>Examples</em>

{% highlight sql %}
SELECT -2147483648 AS col;
  +-----------+
  |col        |
  +-----------+
  |-2147483648|
  +-----------+

SELECT 9223372036854775807l AS col;
  +-------------------+
  |col                |
  +-------------------+
  |9223372036854775807|
  +-------------------+

SELECT -32Y AS col;
  +---+
  |col|
  +---+
  |-32|
  +---+

SELECT 482S AS col;
  +---+
  |col|
  +---+
  |482|
  +---+
{% endhighlight %}

#### Decimal Literal

#### <em>Syntax</em>

{% highlight sql %}
 [ + | - ] { digit [ ... ] . [ digit [ ... ] ] | . digit [ ... ] }
{% endhighlight %}

#### <em>Examples</em>

{% highlight sql %}
SELECT 12.578 AS col;
  +------+
  |col   |
  +------+
  |12.578|
  +------+

SELECT -0.1234567 AS col;
  +----------+
  |col       |
  +----------+
  |-0.1234567|
  +----------+

SELECT -.1234567 AS col;
  +----------+
  |col       |
  +----------+
  |-0.1234567|
  +----------+
{% endhighlight %}

#### Floating Point and BigDecimal Literals

#### <em>Syntax</em>

{% highlight sql %}
 [ + | - ] { digit [ ... ] [ E [ + | - ] digit [ ... ] ] [ D | BD ]
             | digit [ ... ] . [ digit [ ... ] ] [ E [ + | - ] digit [ ... ] ] [ D | BD ]
             | . digit [ ... ] [ E [ + | - ] digit [ ... ] ] [ D | BD ] }
{% endhighlight %}
digit: one of 0, 1, 2, 3, 4, 5, 6, 7, 8 or 9. <br>
default (no postfix): indicate a 4-byte single-precision floating point number.<br>
D: case insensitive, indicates <code>DOUBLE</code>, which is a 8-byte double-precision floating point number.<br>
BD: case insensitive, indicates <code>BIGDECIMAL</code>, is an arbitrary-precision signed decimal number.<br>
#### <em>Examples</em>

{% highlight sql %}
SELECT 5E2 AS col;
  +-----+
  |col  |
  +-----+
  |500.0|
  +-----+

SELECT 5D AS col;
  +---+
  |col|
  +---+
  |5.0|
  +---+

SELECT -5BD AS col;
  +---+
  |col|
  +---+
  |-5 |
  +---+

SELECT 12.578e-2d AS col;
  +-------+
  |col    |
  +-------+
  |0.12578|
  +-------+

SELECT -.1234567E+2BD AS col;
  +---------+
  |col      |
  +---------+
  |-12.34567|
  +---------+

SELECT +3.e+3 AS col;
  +------+
  |col   |
  +------+
  |3000.0|
  +------+

SELECT -3.E-3D AS col;
  +------+
  |col   |
  +------+
  |-0.003|
  +------+
{% endhighlight %}

### Datetime Literal

A Datetime literal is used to specify a datetime data type value.

#### Date Literal

#### <em>Syntax</em>

{% highlight sql %}
DATE 'yyyy-MM-dd'
{% endhighlight %}

#### <em>Examples</em>

{% highlight sql %}
SELECT DATE '2011-11-11' AS col;
  +----------+
  |col       |
  +----------+
  |2011-11-11|
  +----------+
{% endhighlight %}

#### Timestamp Literal

#### <em>Syntax</em>

{% highlight sql %}
TIMESTAMP 'yyyy-MM-dd [ HH:mm:ss.SSSSSSzzz ]'
{% endhighlight %}

#### <em>Examples</em>

{% highlight sql %}
SELECT TIMESTAMP '1997-01-31 09:26:56.123' AS col;
  +-----------------------+
  |col                    |
  +-----------------------+
  |1997-01-31 09:26:56.123|
  +-----------------------+

SELECT TIMESTAMP '1997-01-31 09:26:56.66666666CST' AS col;
  +--------------------------+
  |col                       |
  +--------------------------+
  |1997-01-31 07:26:56.666666|
  +--------------------------+
{% endhighlight %}

### Interval Literal

An interval literal is used to specify a fixed period of time.

#### <em>Syntax</em>
{% highlight sql %}
{ INTERVAL interval_value interval_unit [ interval_value interval_unit ... ]
  | INTERVAL interval_value interval_unit TO interval_unit }
{% endhighlight %}

interval_value:
{% highlight sql %}
{ [ + | - ] number_value | string_value }
{% endhighlight %}
Note: string_value needs to be used for INTERNAL ... TO ... format.

interval_unit:
{% highlight sql %}
YEAR | MONTH | DAY | HOUR | MINUTE | SECOND | MILLISECOND | MICROSECOND
{% endhighlight %}

#### <em>Examples</em>

{% highlight sql %}
SELECT INTERVAL 3 YEAR AS col;
  +-------+
  |    col|
  +-------+
  |3 years|
  +-------+

SELECT INTERVAL -2 HOUR 3 MINUTE AS col;
  +--------------------+
  |                 col|
  +--------------------+
  |-1 hours -57 minutes|
  +--------------------+

SELECT INTERVAL 1 YEAR 2 MONTH 3 WEEK 4 DAY 5 HOUR 6 MINUTE 7 SECOND 8
    MILLISECOND 9 MICROSECOND AS col;
  +-----------------------------------------------------------+
  |col                                                        |
  +-----------------------------------------------------------+
  |1 years 2 months 25 days 5 hours 6 minutes 7.008009 seconds|
  +-----------------------------------------------------------+

SELECT INTERVAL '20 15:40:32.99899999' DAY TO SECOND AS col;
  +---------------------------------------------+
  |col                                          |
  +---------------------------------------------+
  |20 days 15 hours 40 minutes 32.998999 seconds|
  +---------------------------------------------+
{% endhighlight %}
