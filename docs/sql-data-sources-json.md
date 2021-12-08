---
layout: global
title: JSON Files
displayTitle: JSON Files
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

<div class="codetabs">

<div data-lang="scala"  markdown="1">
Spark SQL can automatically infer the schema of a JSON dataset and load it as a `Dataset[Row]`.
This conversion can be done using `SparkSession.read.json()` on either a `Dataset[String]`,
or a JSON file.

Note that the file that is offered as _a json file_ is not a typical JSON file. Each
line must contain a separate, self-contained valid JSON object. For more information, please see
[JSON Lines text format, also called newline-delimited JSON](http://jsonlines.org/).

For a regular multi-line JSON file, set the `multiLine` option to `true`.

{% include_example json_dataset scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala %}
</div>

<div data-lang="java"  markdown="1">
Spark SQL can automatically infer the schema of a JSON dataset and load it as a `Dataset<Row>`.
This conversion can be done using `SparkSession.read().json()` on either a `Dataset<String>`,
or a JSON file.

Note that the file that is offered as _a json file_ is not a typical JSON file. Each
line must contain a separate, self-contained valid JSON object. For more information, please see
[JSON Lines text format, also called newline-delimited JSON](http://jsonlines.org/).

For a regular multi-line JSON file, set the `multiLine` option to `true`.

{% include_example json_dataset java/org/apache/spark/examples/sql/JavaSQLDataSourceExample.java %}
</div>

<div data-lang="python"  markdown="1">
Spark SQL can automatically infer the schema of a JSON dataset and load it as a DataFrame.
This conversion can be done using `SparkSession.read.json` on a JSON file.

Note that the file that is offered as _a json file_ is not a typical JSON file. Each
line must contain a separate, self-contained valid JSON object. For more information, please see
[JSON Lines text format, also called newline-delimited JSON](http://jsonlines.org/).

For a regular multi-line JSON file, set the `multiLine` parameter to `True`.

{% include_example json_dataset python/sql/datasource.py %}
</div>

<div data-lang="r"  markdown="1">
Spark SQL can automatically infer the schema of a JSON dataset and load it as a DataFrame. using
the `read.json()` function, which loads data from a directory of JSON files where each line of the
files is a JSON object.

Note that the file that is offered as _a json file_ is not a typical JSON file. Each
line must contain a separate, self-contained valid JSON object. For more information, please see
[JSON Lines text format, also called newline-delimited JSON](http://jsonlines.org/).

For a regular multi-line JSON file, set a named parameter `multiLine` to `TRUE`.

{% include_example json_dataset r/RSparkSQLExample.R %}

</div>

<div data-lang="SQL"  markdown="1">

{% highlight sql %}

CREATE TEMPORARY VIEW jsonTable
USING org.apache.spark.sql.json
OPTIONS (
  path "examples/src/main/resources/people.json"
)

SELECT * FROM jsonTable

{% endhighlight %}

</div>

</div>

## Data Source Option

Data source options of JSON can be set via:
* the `.option`/`.options` methods of
  * `DataFrameReader`
  * `DataFrameWriter`
  * `DataStreamReader`
  * `DataStreamWriter`
* the built-in functions below
  * `from_json`
  * `to_json`
  * `schema_of_json`
* `OPTIONS` clause at [CREATE TABLE USING DATA_SOURCE](sql-ref-syntax-ddl-create-table-datasource.html)

<table class="table">
  <tr><th><b>Property Name</b></th><th><b>Default</b></th><th><b>Meaning</b></th><th><b>Scope</b></th></tr>
  <tr>
    <!-- TODO(SPARK-35433): Add timeZone to Data Source Option for CSV, too. -->
    <td><code>timeZone</code></td>
    <td>(value of <code>spark.sql.session.timeZone</code> configuration)</td>
    <td>Sets the string that indicates a time zone ID to be used to format timestamps in the JSON datasources or partition values. The following formats of <code>timeZone</code> are supported:<br>
    <ul>
      <li>Region-based zone ID: It should have the form 'area/city', such as 'America/Los_Angeles'.</li>
      <li>Zone offset: It should be in the format '(+|-)HH:mm', for example '-08:00' or '+01:00'. Also 'UTC' and 'Z' are supported as aliases of '+00:00'.</li>
    </ul>
    Other short names like 'CST' are not recommended to use because they can be ambiguous.
    </td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>primitivesAsString</code></td>
    <td><code>false</code></td>
    <td>Infers all primitive values as a string type.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>prefersDecimal</code></td>
    <td><code>false</code></td>
    <td>Infers all floating-point values as a decimal type. If the values do not fit in decimal, then it infers them as doubles.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>allowComments</code></td>
    <td><code>false</code></td>
    <td>Ignores Java/C++ style comment in JSON records.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>allowUnquotedFieldNames</code></td>
    <td><code>false</code></td>
    <td>Allows unquoted JSON field names.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>allowSingleQuotes</code></td>
    <td><code>true</code></td>
    <td>Allows single quotes in addition to double quotes.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>allowNumericLeadingZero</code></td>
    <td><code>false</code></td>
    <td>Allows leading zeros in numbers (e.g. 00012).</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>allowBackslashEscapingAnyCharacter</code></td>
    <td><code>false</code></td>
    <td>Allows accepting quoting of all character using backslash quoting mechanism.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>mode</code></td>
    <td><code>PERMISSIVE</code></td>
    <td>Allows a mode for dealing with corrupt records during parsing.<br>
    <ul>
      <li><code>PERMISSIVE</code>: when it meets a corrupted record, puts the malformed string into a field configured by <code>columnNameOfCorruptRecord</code>, and sets malformed fields to <code>null</code>. To keep corrupt records, an user can set a string type field named <code>columnNameOfCorruptRecord</code> in an user-defined schema. If a schema does not have the field, it drops corrupt records during parsing. When inferring a schema, it implicitly adds a <code>columnNameOfCorruptRecord</code> field in an output schema.</li>
      <li><code>DROPMALFORMED</code>: ignores the whole corrupted records.</li>
      <li><code>FAILFAST</code>: throws an exception when it meets corrupted records.</li>
    </ul>
    </td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>columnNameOfCorruptRecord</code></td>
    <td>(value of <code>spark.sql.columnNameOfCorruptRecord</code> configuration)</td>
    <td>Allows renaming the new field having malformed string created by <code>PERMISSIVE</code> mode. This overrides spark.sql.columnNameOfCorruptRecord.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>dateFormat</code></td>
    <td><code>yyyy-MM-dd</code></td>
    <td>Sets the string that indicates a date format. Custom date formats follow the formats at <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html"> datetime pattern</a>. This applies to date type.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>timestampFormat</code></td>
    <td><code>yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]</code></td>
    <td>Sets the string that indicates a timestamp format. Custom date formats follow the formats at <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html"> datetime pattern</a>. This applies to timestamp type.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>timestampNTZFormat</code></td>
    <td>yyyy-MM-dd'T'HH:mm:ss[.SSS]</td>
    <td>Sets the string that indicates a timestamp without timezone format. Custom date formats follow the formats at <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">Datetime Patterns</a>. This applies to timestamp without timezone type, note that zone-offset and time-zone components are not supported when writing or reading this data type.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>multiLine</code></td>
    <td><code>false</code></td>
    <td>Parse one record, which may span multiple lines, per file.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>allowUnquotedControlChars</code></td>
    <td><code>false</code></td>
    <td>Allows JSON Strings to contain unquoted control characters (ASCII characters with value less than 32, including tab and line feed characters) or not.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>encoding</code></td>
    <td>Detected automatically when <code>multiLine</code> is set to <code>true</code> (for reading), <code>UTF-8</code> (for writing)</td>
    <td>For reading, allows to forcibly set one of standard basic or extended encoding for the JSON files. For example UTF-16BE, UTF-32LE. For writing, Specifies encoding (charset) of saved json files.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>lineSep</code></td>
    <td><code>\r</code>, <code>\r\n</code>, <code>\n</code> (for reading), <code>\n</code> (for writing)</td>
    <td>Defines the line separator that should be used for parsing.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>samplingRatio</code></td>
    <td><code>1.0</code></td>
    <td>Defines fraction of input JSON objects used for schema inferring.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>dropFieldIfAllNull</code></td>
    <td><code>false</code></td>
    <td>Whether to ignore column of all null values or empty array/struct during schema inference.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>locale</code></td>
    <td><code>en-US</code></td>
    <td>Sets a locale as language tag in IETF BCP 47 format. For instance, <code>locale</code> is used while parsing dates and timestamps.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>allowNonNumericNumbers</code></td>
    <td><code>true</code></td>
    <td>Allows JSON parser to recognize set of “Not-a-Number” (NaN) tokens as legal floating number values.<br>
    <ul>
      <li><code>+INF</code>: for positive infinity, as well as alias of <code>+Infinity</code> and <code>Infinity</code>.</li>
      <li><code>-INF</code>: for negative infinity, alias <code>-Infinity</code>.</li>
      <li><code>NaN</code>: for other not-a-numbers, like result of division by zero.</li>
    </ul>
    </td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>compression</code></td>
    <td>(none)</td>
    <td>Compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, bzip2, gzip, lz4, snappy and deflate).</td>
    <td>write</td>
  </tr>
  <tr>
    <td><code>ignoreNullFields</code></td>
    <td>(value of <code>spark.sql.jsonGenerator.ignoreNullFields</code> configuration)</td>
    <td>Whether to ignore null fields when generating JSON objects.</td>
    <td>write</td>
  </tr>
</table>
Other generic options can be found in <a href="https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html"> Generic File Source Options</a>.
