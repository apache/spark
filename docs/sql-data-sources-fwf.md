---
layout: global
title: Fixed-width Files
displayTitle: Fixed-width Files
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

Spark SQL provides `spark.read().fwf("file_name")` to read a file or directory of files where
each record is a single line with values occupying fixed character positions, and
`dataframe.write().fwf("path")` to write one back out. Function `option()` can be used to
customize the behavior of reading or writing, such as which character positions each column
occupies, whether the first line is a header, and so on.

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% include_example fwf_dataset python/sql/datasource.py %}
</div>

<div data-lang="scala"  markdown="1">
{% include_example fwf_dataset scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala %}
</div>

<div data-lang="java"  markdown="1">
{% include_example fwf_dataset java/org/apache/spark/examples/sql/JavaSQLDataSourceExample.java %}
</div>

<div data-lang="SQL"  markdown="1">
{% highlight sql %}
CREATE TABLE people (name STRING, age INT)
USING fwf
OPTIONS (widths '10,4', header 'true')
LOCATION '/path/to/people/';
{% endhighlight %}
</div>

</div>

## Detecting column positions

By default (`colspecs` unset, or `"infer"`), Spark samples the first `inferNrows` lines
(100 by default) and infers each column's character range from where non-blank text appears
across that sample.

Inferring column positions cannot be combined with a user-supplied `schema`. Pass explicit
`colspecs` or `widths` if you also want to supply a schema. A column that is wider in a later
row than in the sampled rows can be truncated; pass a larger `inferNrows`, or explicit
`colspecs`/`widths`, if needed.

Writing always requires explicit `colspecs` or `widths`.

## Data Source Option

Data source options of fixed-width files can be set via:
* the `.option`/`.options` methods of
  * `DataFrameReader`
  * `DataFrameWriter`
* `OPTIONS` clause at [CREATE TABLE USING DATA_SOURCE](sql-ref-syntax-ddl-create-table-datasource.html)

<table>
  <thead><tr><th><b>Property Name</b></th><th><b>Default</b></th><th><b>Meaning</b></th><th><b>Scope</b></th></tr></thead>
  <tr>
    <td><code>colspecs</code></td>
    <td>infer</td>
    <td>A comma-separated list of half-open <code>from-to</code> character intervals, one per field (e.g. <code>0-5,5-10,10-20</code>), or <code>infer</code> to detect them from a sample of the data. At most one of <code>colspecs</code> and <code>widths</code> may be given; required (and must not be <code>infer</code>) when writing.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>widths</code></td>
    <td>(none)</td>
    <td>A comma-separated list of contiguous field widths (e.g. <code>5,5,10</code>), used instead of <code>colspecs</code> when the fields have no gaps between them.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>inferNrows</code></td>
    <td>100</td>
    <td>The number of rows sampled when <code>colspecs</code> is <code>infer</code>.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>header</code></td>
    <td>false</td>
    <td>For reading, uses the first non-skipped line as names of columns. For writing, writes out names of columns as the first line.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>comment</code></td>
    <td></td>
    <td>Sets a single character; the rest of a line from that character onward is ignored, both when detecting <code>colspecs</code> and when reading. By default, it is disabled.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>delimiter</code></td>
    <td></td>
    <td>Extra character(s) treated as blank when detecting <code>colspecs</code> and when trimming each sliced field. Replaces the default whitespace blank set rather than adding to it.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>skipRows</code></td>
    <td>(none)</td>
    <td>0-indexed row numbers to skip, before <code>header</code> is applied: either an integer (skip the first N rows) or a comma-separated list of specific row numbers, anywhere in the file. Reading a file with <code>skipRows</code> set disables splitting that file across multiple tasks.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>nullValue</code></td>
    <td></td>
    <td>Sets the string representation of a null value.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>dateFormat</code></td>
    <td>yyyy-MM-dd</td>
    <td>Sets the string that indicates a date format. Custom date formats follow the formats at <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">Datetime Patterns</a>.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>timestampFormat</code></td>
    <td>yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]</td>
    <td>Sets the string that indicates a timestamp format. Custom date formats follow the formats at <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">Datetime Patterns</a>.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>timeZone</code></td>
    <td>(value of <code>spark.sql.session.timeZone</code> configuration)</td>
    <td>Sets the string that indicates a time zone ID used when formatting/parsing timestamps.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>locale</code></td>
    <td>en-US</td>
    <td>Sets a locale as language tag in IETF BCP 47 format. For instance, this is used while parsing dates, timestamps, and <code>DECIMAL</code> values.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>mode</code></td>
    <td>PERMISSIVE</td>
    <td>Allows a mode for dealing with corrupt records during parsing.<br>
    <ul>
      <li><code>PERMISSIVE</code>: when it meets a corrupted record, sets malformed fields to <code>null</code>. If the schema has a <code>columnNameOfCorruptRecord</code> field, puts the malformed line into it.</li>
      <li><code>DROPMALFORMED</code>: ignores the whole corrupted record.</li>
      <li><code>FAILFAST</code>: throws an exception when it meets a corrupted record.</li>
    </ul>
    </td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>columnNameOfCorruptRecord</code></td>
    <td>(value of <code>spark.sql.columnNameOfCorruptRecord</code> configuration)</td>
    <td>Allows renaming the new field having the malformed line created by <code>PERMISSIVE</code> mode.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>encoding</code><br><code>charset</code></td>
    <td>UTF-8</td>
    <td>For reading, decodes the files by the given encoding type. For writing, specifies encoding (charset) of saved files.</td>
    <td>read/write</td>
  </tr>
</table>

Other generic options can be found in <a href="https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html">Generic File Source Options</a>.
