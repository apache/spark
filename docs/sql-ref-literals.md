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
 * [Binary Literal](#binary-literal)
 * [Null Literal](#null-literal)
 * [Boolean Literal](#boolean-literal)
 * [Numeric Literal](#numeric-literal)
 * [Datetime Literal](#datetime-literal)
 * [Interval Literal](#interval-literal)

### String Literal

A string literal is used to specify a character string value.

#### Syntax

{% highlight sql %}
'c [ ... ]' | "c [ ... ]"
{% endhighlight %}

#### <em>Parameters</em>

<dl>
  <dt><code><em>c</em></code></dt>
  <dd>
    One character from the character set. Use <code>\</code> to escape special characters (e.g., <code>'</code> or <code>\</code>).
  </dd>
</dl>

#### <em>Examples</em>

{% highlight sql %}
SELECT 'Hello, World!' AS col;
+-------------+
|          col|
+-------------+
|Hello, World!|
+-------------+

SELECT "SPARK SQL" AS col;
+---------+
|      col|
+---------+
|Spark SQL|
+---------+

SELECT 'it\'s $10.' AS col;
+---------+
|      col|
+---------+
|It's $10.|
+---------+
{% endhighlight %}

### Binary Literal

A binary literal is used to specify a byte sequence value.

#### Syntax

{% highlight sql %}
X { 'c [ ... ]' | "c [ ... ]" }
{% endhighlight %}

#### <em>Parameters</em>

<dl>
  <dt><code><em>c</em></code></dt>
  <dd>
    One character from the character set.
  </dd>
</dl>

#### <em>Examples</em>

{% highlight sql %}
SELECT X'123456' AS col;
+----------+
|       col|
+----------+
|[12 34 56]|
+----------+
{% endhighlight %}

### Null Literal

A null literal is used to specify a null value.

#### Syntax

{% highlight sql %}
NULL
{% endhighlight %}

#### Examples

{% highlight sql %}
SELECT NULL AS col;
+----+
| col|
+----+
|NULL|
+----+
{% endhighlight %}

### Boolean Literal

A boolean literal is used to specify a boolean value.

#### Syntax

{% highlight sql %}
TRUE | FALSE
{% endhighlight %}

#### Examples

{% highlight sql %}
SELECT TRUE AS col;
+----+
| col|
+----+
|true|
+----+
{% endhighlight %}

### Numeric Literal

A numeric literal is used to specify a fixed or floating-point number.

#### Integral Literal

#### Syntax

{% highlight sql %}
[ + | - ] digit [ ... ] [ L | S | Y ]
{% endhighlight %}

#### Parameters

<dl>
  <dt><code><em>digit</em></code></dt>
  <dd>
    Any numeral from 0 to 9.
  </dd>
</dl>
<dl>
  <dt><code><em>L</em></code></dt>
  <dd>
    Case insensitive, indicates <code>BIGINT</code>, which is a 8-byte signed integer number.
  </dd>
</dl>
<dl>
  <dt><code><em>S</em></code></dt>
  <dd>
    Case insensitive, indicates <code>SMALLINT</code>, which is a 2-byte signed integer number.
  </dd>
</dl>
<dl>
  <dt><code><em>Y</em></code></dt>
  <dd>
    Case insensitive, indicates <code>TINYINT</code>, which is a 1-byte signed integer number.
  </dd>
</dl>
<dl>
  <dt><code><em>default (no postfix)</em></code></dt>
  <dd>
    Indicates a 4-byte signed integer number.
  </dd>
</dl>

#### Examples

{% highlight sql %}
SELECT -2147483648 AS col;
+-----------+
|        col|
+-----------+
|-2147483648|
+-----------+

SELECT 9223372036854775807l AS col;
+-------------------+
|                col|
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

#### Fractional Literals

#### Syntax

decimal literals:
{% highlight sql %}
decimal_digits { [ BD ] | [ exponent BD ] } | digit [ ... ] [ exponent ] BD
{% endhighlight %}

double literals:
{% highlight sql %}
decimal_digits  { D | exponent [ D ] }  | digit [ ... ] { exponent [ D ] | [ exponent ] D }
{% endhighlight %}

While decimal_digits is defined as
{% highlight sql %}
[ + | - ] { digit [ ... ] . [ digit [ ... ] ] | . digit [ ... ] }
{% endhighlight %}

and exponent is defined as
{% highlight sql %}
E [ + | - ] digit [ ... ]
{% endhighlight %}

#### Parameters

<dl>
  <dt><code><em>digit</em></code></dt>
  <dd>
    Any numeral from 0 to 9.
  </dd>
</dl>
<dl>
  <dt><code><em>D</em></code></dt>
  <dd>
    Case insensitive, indicates <code>DOUBLE</code>, which is a 8-byte double-precision floating point number.
  </dd>
</dl>
<dl>
  <dt><code><em>BD</em></code></dt>
  <dd>
    Case insensitive, indicates <code>DECIMAL</code>, with the total number of digits as precision and the number of digits to right of decimal point as scale.
  </dd>
</dl>

#### Examples

{% highlight sql %}
SELECT 12.578 AS col;
+------+
|   col|
+------+
|12.578|
+------+

SELECT -0.1234567 AS col;
+----------+
|       col|
+----------+
|-0.1234567|
+----------+

SELECT -.1234567 AS col;
+----------+
|       col|
+----------+
|-0.1234567|
+----------+

SELECT 123. AS col;
+---+
|col|
+---+
|123|
+---+

SELECT 123.BD AS col;
+---+
|col|
+---+
|123|
+---+

SELECT 5E2 AS col;
+-----+
|  col|
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
| -5|
+---+

SELECT 12.578e-2d AS col;
+-------+
|    col|
+-------+
|0.12578|
+-------+

SELECT -.1234567E+2BD AS col;
+---------+
|      col|
+---------+
|-12.34567|
+---------+

SELECT +3.e+3 AS col;
+------+
|   col|
+------+
|3000.0|
+------+

SELECT -3.E-3D AS col;
+------+
|   col|
+------+
|-0.003|
+------+
{% endhighlight %}

### Datetime Literal

A Datetime literal is used to specify a datetime value.

#### Date Literal

#### Syntax

{% highlight sql %}
DATE { 'yyyy' |
       'yyyy-[m]m' |
       'yyyy-[m]m-[d]d' |
       'yyyy-[m]m-[d]d[T]' }
{% endhighlight %}
Note: defaults to <code>01</code> if month or day is not specified.

#### Examples

{% highlight sql %}
SELECT DATE '1997' AS col;
+----------+
|       col|
+----------+
|1997-01-01|
+----------+

SELECT DATE '1997-01' AS col;
+----------+
|       col|
+----------+
|1997-01-01|
+----------+

SELECT DATE '2011-11-11' AS col;
+----------+
|       col|
+----------+
|2011-11-11|
+----------+
{% endhighlight %}

#### Timestamp Literal

#### Syntax

{% highlight sql %}
TIMESTAMP { 'yyyy' |
            'yyyy-[m]m' |
            'yyyy-[m]m-[d]d' |
            'yyyy-[m]m-[d]d ' |
            'yyyy-[m]m-[d]d[T][h]h[:]' |
            'yyyy-[m]m-[d]d[T][h]h:[m]m[:]' |
            'yyyy-[m]m-[d]d[T][h]h:[m]m:[s]s[.]' |
            'yyyy-[m]m-[d]d[T][h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]'}
{% endhighlight %}
Note: defaults to <code>00</code> if hour, minute or second is not specified. <br><br>
`zone_id` should have one of the forms:
<ul>
  <li>Z - Zulu time zone UTC+0</li>
  <li>+|-[h]h:[m]m</li>
  <li>An id with one of the prefixes UTC+, UTC-, GMT+, GMT-, UT+ or UT-, and a suffix in the formats:
    <ul>
      <li>+|-h[h]</li>
      <li>+|-hh[:]mm</li>
      <li>+|-hh:mm:ss</li>
      <li>+|-hhmmss</li>
    </ul>
  </li>
  <li>Region-based zone IDs in the form <code>area/city</code>, such as <code>Europe/Paris</code></li>
</ul>
Note: defaults to the session local timezone (set via <code>spark.sql.session.timeZone</code>) if <code>zone_id</code> is not specified.

#### Examples

{% highlight sql %}
SELECT TIMESTAMP '1997-01-31 09:26:56.123' AS col;
+-----------------------+
|                    col|
+-----------------------+
|1997-01-31 09:26:56.123|
+-----------------------+

SELECT TIMESTAMP '1997-01-31 09:26:56.66666666UTC+08:00' AS col;
+--------------------------+
|                      col |
+--------------------------+
|1997-01-30 17:26:56.666666|
+--------------------------+

SELECT TIMESTAMP '1997-01' AS col;
+-------------------+
|                col|
+-------------------+
|1997-01-01 00:00:00|
+-------------------+
{% endhighlight %}

### Interval Literal

An interval literal is used to specify a fixed period of time.

#### Syntax
{% highlight sql %}
{ INTERVAL interval_value interval_unit [ interval_value interval_unit ... ] |
  INTERVAL 'interval_value interval_unit [ interval_value interval_unit ... ]' |
  INTERVAL interval_string_value interval_unit TO interval_unit }
{% endhighlight %}

#### Parameters

<dl>
  <dt><code><em>interval_value</em></code></dt>
  <dd>
    <b>Syntax:</b>
      <code>
        [ + | - ] number_value | '[ + | - ] number_value'
      </code><br>
  </dd>
</dl>
<dl>
  <dt><code><em>interval_string_value</em></code></dt>
    <dd>
      year-month/day-time interval string.
    </dd>
</dl>
<dl>
  <dt><code><em>interval_unit</em></code></dt>
  <dd>
    <b>Syntax:</b><br>
      <code>
        YEAR[S] | MONTH[S] | WEEK[S] | DAY[S] | HOUR[S] | MINUTE[S] | SECOND[S] | <br>
        MILLISECOND[S] | MICROSECOND[S]
      </code>
  </dd>
</dl>

#### Examples

{% highlight sql %}
SELECT INTERVAL 3 YEAR AS col;
+-------+
|    col|
+-------+
|3 years|
+-------+

SELECT INTERVAL -2 HOUR '3' MINUTE AS col;
+--------------------+
|                 col|
+--------------------+
|-1 hours -57 minutes|
+--------------------+

SELECT INTERVAL '1 YEAR 2 DAYS 3 HOURS';
+----------------------+
|                   col|
+----------------------+
|1 years 2 days 3 hours|
+----------------------+

SELECT INTERVAL 1 YEARS 2 MONTH 3 WEEK 4 DAYS 5 HOUR 6 MINUTES 7 SECOND 8
    MILLISECOND 9 MICROSECONDS AS col;
+-----------------------------------------------------------+
|                                                        col|
+-----------------------------------------------------------+
|1 years 2 months 25 days 5 hours 6 minutes 7.008009 seconds|
+-----------------------------------------------------------+

SELECT INTERVAL '2-3' YEAR TO MONTH AS col;
+----------------+
|             col|
+----------------+
|2 years 3 months|
+----------------+

SELECT INTERVAL '20 15:40:32.99899999' DAY TO SECOND AS col;
+---------------------------------------------+
|                                          col|
+---------------------------------------------+
|20 days 15 hours 40 minutes 32.998999 seconds|
+---------------------------------------------+
{% endhighlight %}
