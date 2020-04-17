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

A literal (aka constant) represents a fixed data value. Spark SQL supports the following literals:

 * String Literals
 * Boolean Literals
 * Numeric Literals
 * Datetime Literals
 * Interval Literals

### String Literals

A string literal is used to specify a character string value.
<dl>
  <dt><code><em>Format:</em></code></dt>
  <dd>
    <code>'c [ ... ]'</code><br>
    c: one character of user's character set.
  </dd>
</dl>
<dl>
  <dt><code><em>Examples:</em></code></dt>
  <dd>
    <code>'Hello, World!',   'Spark SQL',   'dbname.schema'</code>
  </dd>
</dl>

### Boolean Literals

A boolean literal is used to specify a boolean value.
<dl>
  <dt><code><em>Format:</em></code></dt>
  <dd>
    <code>TRUE | FALSE </code>
  </dd>
</dl>

### Numeric Literals

A numeric literal is used to specify a fixed or floating-point number.

#### Integer Literals

<dl>
  <dt><code><em>Format:</em></code></dt>
  <dd>
    <code>[ + | - ] digit [ ... ] [ l | L ]</code><br>
    digit: one of 0, 1, 2, 3, 4, 5, 6, 7, 8 or 9 <br>
    l or L: indicates <code>LongType</code>.
  </dd>
</dl>
<dl>
  <dt><code><em>Examples:</em></code></dt>
  <dd>
    <code>6,  +188,  -54, 1000L</code>
  </dd>
</dl>

#### Floating Point and Decimal Literals

<dl>
  <dt><code><em>Format:</em></code></dt>
  <dd>
    <code>
      [ + | - ] { digit [ ... ] [ . ] [ digit [ ... ] ] | . digit [ ... ] } <br>
      [ { e | E } [ + | - ] digit [ ... ] ] [ d | D ]
    </code><br>
    digit: one of 0, 1, 2, 3, 4, 5, 6, 7, 8 or 9. <br>
    e or E: indicates that the number is in scientific notation format. <br>
    d or D: indicates <code>DoubleType</code>
  </dd>
</dl>
<dl>
  <dt><code><em>Examples:</em></code></dt>
  <dd>
    <code>5E2,   3.e6,   2.3E-2,   +3.e+3,   -4D,   0.25</code>
  </dd>
</dl>

### Datetime Literals

A Datetime literal is used to specify a datetime data type value.

#### Date Literals

<dl>
  <dt><code><em>Format:</em></code></dt>
  <dd>
    <code>
      DATE 'YYYY-MM-DD'
    </code>
  </dd>
</dl>
<dl>
  <dt><code><em>Examples:</em></code></dt>
  <dd>
    <code>DATE '2011-11-11'</code>
  </dd>
</dl>

#### Timestamp Literals

<dl>
  <dt><code><em>Format:</em></code></dt>
  <dd>
    <code>
      TIMESTAMP yyyy-MM-dd HH:mm[:ss.SSSSSSzzz]
    </code>
  </dd>
</dl>
<dl>
  <dt><code><em>Examples:</em></code></dt>
  <dd>
    <code>TIMESTAMP '1997-01-31 09:26:56.123'</code><br>
    <code>TIMESTAMP '1997-01-31 09:26:56.66666666CST'</code>
  </dd>
</dl>

### Interval Literals

An inerval literal is used to specify a fixed period of time.
<dl>
  <dt><code><em>Format:</em></code></dt>
  <dd>
    <code>
      INTERVAL value { YEAR | MONTH | DAY | HOUR | MINUTE | SECOND | MILLISECOND | MICROSECOND }
    </code>
  </dd>
</dl>
<dl>
  <dt><code><em>Examples:</em></code></dt>
  <dd>
    <code>INTERVAL 3 YEAR</code><br>
    <code>INTERVAL 3 YEAR 3 HOUR</code>
  </dd>
</dl>