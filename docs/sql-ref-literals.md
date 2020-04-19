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

#### Syntax

{% highlight sql %}
'c [ ... ]' | "c [ ... ]"
{% endhighlight %}

#### <em>Parameters</em>

<dl>
  <dt><code><em>c</em></code></dt>
  <dd>
    One character from the character set. Use <code>\</code> to escape special characters.
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

SELECT SELECT 'it\'s $10.' AS col;
  +---------+
  |      col|
  +---------+
  |It's $10.|
  +---------+
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

#### Integer Literal

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

#### Decimal Literal

#### Syntax

{% highlight sql %}
[ + | - ] { digit [ ... ] . [ digit [ ... ] ] | . digit [ ... ] }
{% endhighlight %}

#### Parameters

<dl>
  <dt><code><em>digit</em></code></dt>
  <dd>
    Any numeral from 0 to 9.
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
{% endhighlight %}

#### Floating Point and BigDecimal Literals

#### Syntax

{% highlight sql %}
[ + | - ] { digit [ ... ] [ E [ + | - ] digit [ ... ] ] [ D | BD ] |
            digit [ ... ] . [ digit [ ... ] ] [ E [ + | - ] digit [ ... ] ] [ D | BD ] |
            . digit [ ... ] [ E [ + | - ] digit [ ... ] ] [ D | BD ] }
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
    Case insensitive, indicates <code>BIGDECIMAL</code>, which is an arbitrary-precision signed decimal number.
  </dd>
</dl>
<dl>
  <dt><code><em>default (no postfix)</em></code></dt>
  <dd>
    Indicate a 4-byte single-precision floating point number.
  </dd>
</dl>

#### Examples

{% highlight sql %}
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
       'yyyy-[m]m-[d]d ' |
       'yyyy-[m]m-[d]d[T]c[...]' }
{% endhighlight %}

#### Parameters

<dl>
  <dt><code><em>c</em></code></dt>
  <dd>
    One character from the character set.
  </dd>
</dl>

#### Examples

{% highlight sql %}
SELECT DATE '1997' AS col;
  +----------+
  |       col|
  +----------+
  |1997-01-01|
  +----------+

SELECT TIMESTAMP '1997-01' AS col;
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
            'yyyy-[m]m-[d]d[T][h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]' |
            '[T][h]h:' |
            '[T][h]h:[m]m[:]' |
            '[T][h]h:[m]m:[s]s[.]' |
            'T[h]h' |
            '[T][h]h:[m]m:[s]s.[ms][ms][ms][us][us][us][zone_id]' }
{% endhighlight %}
`zone_id` should have one of the forms:
<ul>
  <li>Z - Zulu time zone UTC+0</li>
  <li>+|-[h]h:[m]m</li>
  <li>A short id, see <a href="https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html#SHORT_IDS">SHORT_IDS</a></li>
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

#### Examples

{% highlight sql %}
SELECT TIMESTAMP '1997-01-31 09:26:56.123' AS col;
  +-----------------------+
  |                    col|
  +-----------------------+
  |1997-01-31 09:26:56.123|
  +-----------------------+

SELECT TIMESTAMP '1997-01-31 09:26:56.66666666CST' AS col;
  +--------------------------+
  |                      col |
  +--------------------------+
  |1997-01-31 07:26:56.666666|
  +--------------------------+

SELECT TIMESTAMP '1997-01' AS col;
  +-------------------+
  |                col|
  +-------------------+
  |1997-01-01 00:00:00|
  +-------------------+

SELECT TIMESTAMP '09:26' AS col;
  +-------------------+
  |                col|
  +-------------------+
  |2020-04-17 09:26:00|
  +-------------------+
{% endhighlight %}

### Interval Literal

An interval literal is used to specify a fixed period of time.

#### Syntax
{% highlight sql %}
{ INTERVAL interval_value interval_unit [ interval_value interval_unit ... ] |
  INTERVAL interval_value interval_unit TO interval_unit }
{% endhighlight %}

#### Parameters

<dl>
  <dt><code><em>interval_value</em></code></dt>
  <dd>
    <b>Syntax:</b>
      <code>
        { [ + | - ] number_value | string_value }
      </code><br>
      Note: string_value needs to be used for <code>INTERNAL</code> ... <code>TO</code> ... format.
  </dd>
</dl>
<dl>
  <dt><code><em>interval_unit</em></code></dt>
  <dd>
    <b>Syntax:</b>
      <code>
        YEAR | MONTH | DAY | HOUR | MINUTE | SECOND | MILLISECOND | MICROSECOND
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

SELECT INTERVAL -2 HOUR 3 MINUTE AS col;
  +--------------------+
  |                 col|
  +--------------------+
  |-1 hours -57 minutes|
  +--------------------+

SELECT INTERVAL 1 YEAR 2 MONTH 3 WEEK 4 DAY 5 HOUR 6 MINUTE 7 SECOND 8
    MILLISECOND 9 MICROSECOND AS col;
  +-----------------------------------------------------------+
  |                                                        col|
  +-----------------------------------------------------------+
  |1 years 2 months 25 days 5 hours 6 minutes 7.008009 seconds|
  +-----------------------------------------------------------+

SELECT INTERVAL '20 15:40:32.99899999' DAY TO SECOND AS col;
  +---------------------------------------------+
  |                                          col|
  +---------------------------------------------+
  |20 days 15 hours 40 minutes 32.998999 seconds|
  +---------------------------------------------+
{% endhighlight %}
