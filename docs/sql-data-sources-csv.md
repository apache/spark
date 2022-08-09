---
layout: global
title: CSV Files
displayTitle: CSV Files
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

Spark SQL provides `spark.read().csv("file_name")` to read a file or directory of files in CSV format into Spark DataFrame, and `dataframe.write().csv("path")` to write to a CSV file. Function `option()` can be used to customize the behavior of reading or writing, such as controlling behavior of the header, delimiter character, character set, and so on.

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% include_example csv_dataset scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala %}
</div>

<div data-lang="java"  markdown="1">
{% include_example csv_dataset java/org/apache/spark/examples/sql/JavaSQLDataSourceExample.java %}
</div>

<div data-lang="python"  markdown="1">
{% include_example csv_dataset python/sql/datasource.py %}
</div>

</div>

## Data Source Option

Data source options of CSV can be set via:
* the `.option`/`.options` methods of
  * `DataFrameReader`
  * `DataFrameWriter`
  * `DataStreamReader`
  * `DataStreamWriter`
* the built-in functions below
  * `from_csv`
  * `to_csv`
  * `schema_of_csv`
* `OPTIONS` clause at [CREATE TABLE USING DATA_SOURCE](sql-ref-syntax-ddl-create-table-datasource.html)


<table class="table">
  <tr><th><b>Property Name</b></th><th><b>Default</b></th><th><b>Meaning</b></th><th><b>Scope</b></th></tr>
  <tr>
    <td><code>sep</code></td>
    <td>,</td>
    <td>Sets a separator for each field and value. This separator can be one or more characters.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>encoding</code></td>
    <td>UTF-8</td>
    <td>For reading, decodes the CSV files by the given encoding type. For writing, specifies encoding (charset) of saved CSV files. CSV built-in functions ignore this option.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>quote</code></td>
    <td>"</td>
    <td>Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you need to set not <code>null</code> but an empty string. For writing, if an empty string is set, it uses <code>u0000</code> (null character).</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>quoteAll</code></td>
    <td>false</td>
    <td>A flag indicating whether all values should always be enclosed in quotes. Default is to only escape values containing a quote character.</td>
    <td>write</td>
  </tr>
  <tr>
    <td><code>escape</code></td>
    <td>\</td>
    <td>Sets a single character used for escaping quotes inside an already quoted value.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>escapeQuotes</code></td>
    <td>true</td>
    <td>A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character.</td>
    <td>write</td>
  </tr>
  <tr>
    <td><code>comment</code></td>
    <td></td>
    <td>Sets a single character used for skipping lines beginning with this character. By default, it is disabled.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>header</code></td>
    <td>false</td>
    <td>For reading, uses the first line as names of columns. For writing, writes the names of columns as the first line. Note that if the given path is a RDD of Strings, this header option will remove all lines same with the header if exists. CSV built-in functions ignore this option.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>inferSchema</code></td>
    <td>false</td>
    <td>Infers the input schema automatically from data. It requires one extra pass over the data. CSV built-in functions ignore this option.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>prefersDate</code></td>
    <td>false</td>
    <td>During schema inference (<code>inferSchema</code>), attempts to infer string columns that contain dates or timestamps as <code>Date</code> if the values satisfy the <code>dateFormat</code> option and failed to be parsed by the respective formatter. With a user-provided schema, attempts to parse timestamp columns as dates using <code>dateFormat</code> if they fail to conform to <code>timestampFormat</code>, in this case the parsed values will be cast to timestamp type afterwards.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>enforceSchema</code></td>
    <td>true</td>
    <td>If it is set to <code>true</code>, the specified or inferred schema will be forcibly applied to datasource files, and headers in CSV files will be ignored. If the option is set to <code>false</code>, the schema will be validated against all headers in CSV files in the case when the <code>header</code> option is set to <code>true</code>. Field names in the schema and column names in CSV headers are checked by their positions taking into account <code>spark.sql.caseSensitive</code>. Though the default value is true, it is recommended to disable the <code>enforceSchema</code> option to avoid incorrect results. CSV built-in functions ignore this option.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>ignoreLeadingWhiteSpace</code></td>
    <td><code>false</code> (for reading), <code>true</code> (for writing)</td>
    <td>A flag indicating whether or not leading whitespaces from values being read/written should be skipped.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>ignoreTrailingWhiteSpace</code></td>
    <td><code>false</code> (for reading), <code>true</code> (for writing)</td>
    <td>A flag indicating whether or not trailing whitespaces from values being read/written should be skipped.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>nullValue</code></td>
    <td></td>
    <td>Sets the string representation of a null value. Since 2.0.1, this <code>nullValue</code> param applies to all supported types including the string type.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>nanValue</code></td>
    <td>NaN</td>
    <td>Sets the string representation of a non-number value.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>positiveInf</code></td>
    <td>Inf</td>
    <td>Sets the string representation of a positive infinity value.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>negativeInf</code></td>
    <td>-Inf</td>
    <td>Sets the string representation of a negative infinity value.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>dateFormat</code></td>
    <td>yyyy-MM-dd</td>
    <td>Sets the string that indicates a date format. Custom date formats follow the formats at <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">Datetime Patterns</a>. This applies to date type.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>timestampFormat</code></td>
    <td>yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]</td>
    <td>Sets the string that indicates a timestamp format. Custom date formats follow the formats at <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">Datetime Patterns</a>. This applies to timestamp type.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>timestampNTZFormat</code></td>
    <td>yyyy-MM-dd'T'HH:mm:ss[.SSS]</td>
    <td>Sets the string that indicates a timestamp without timezone format. Custom date formats follow the formats at <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">Datetime Patterns</a>. This applies to timestamp without timezone type, note that zone-offset and time-zone components are not supported when writing or reading this data type.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>enableDateTimeParsingFallback</code></td>
    <td>Enabled if the time parser policy has legacy settings or if no custom date or timestamp pattern was provided.</td>
    <td>Allows falling back to the backward compatible (Spark 1.x and 2.0) behavior of parsing dates and timestamps if values do not match the set patterns.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>maxColumns</code></td>
    <td>20480</td>
    <td>Defines a hard limit of how many columns a record can have.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>maxCharsPerColumn</code></td>
    <td>-1</td>
    <td>Defines the maximum number of characters allowed for any given value being read. By default, it is -1 meaning unlimited length</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>mode</code></td>
    <td>PERMISSIVE</td>
    <td>Allows a mode for dealing with corrupt records during parsing. It supports the following case-insensitive modes. Note that Spark tries to parse only required columns in CSV under column pruning. Therefore, corrupt records can be different based on required set of fields. This behavior can be controlled by <code>spark.sql.csv.parser.columnPruning.enabled</code> (enabled by default).<br>
    <ul>
      <li><code>PERMISSIVE</code>: when it meets a corrupted record, puts the malformed string into a field configured by <code>columnNameOfCorruptRecord</code>, and sets malformed fields to <code>null</code>. To keep corrupt records, an user can set a string type field named <code>columnNameOfCorruptRecord</code> in an user-defined schema. If a schema does not have the field, it drops corrupt records during parsing. A record with less/more tokens than schema is not a corrupted record to CSV. When it meets a record having fewer tokens than the length of the schema, sets <code>null</code> to extra fields. When the record has more tokens than the length of the schema, it drops extra tokens.</li>
      <li><code>DROPMALFORMED</code>: ignores the whole corrupted records. This mode is unsupported in the CSV built-in functions.</li>
      <li><code>FAILFAST</code>: throws an exception when it meets corrupted records.</li>
    </ul>
    </td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>columnNameOfCorruptRecord</code></td>
    <td>(value of <code>spark.sql.columnNameOfCorruptRecord</code> configuration)</td>
    <td>Allows renaming the new field having malformed string created by <code>PERMISSIVE</code> mode. This overrides <code>spark.sql.columnNameOfCorruptRecord</code>.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>multiLine</code></td>
    <td>false</td>
    <td>Parse one record, which may span multiple lines, per file. CSV built-in functions ignore this option.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>charToEscapeQuoteEscaping</code></td>
    <td><code>escape</code> or <code>\0</code></td>
    <td>Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, <code>\0</code> otherwise.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>samplingRatio</code></td>
    <td>1.0</td>
    <td>Defines fraction of rows used for schema inferring. CSV built-in functions ignore this option.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>emptyValue</code></td>
    <td><code></code> (for reading), <code>""</code> (for writing)</td>
    <td>Sets the string representation of an empty value.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>locale</code></td>
    <td>en-US</td>
    <td>Sets a locale as language tag in IETF BCP 47 format. For instance, this is used while parsing dates and timestamps.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>lineSep</code></td>
    <td><code>\r</code>, <code>\r\n</code> and <code>\n</code> (for reading), <code>\n</code> (for writing)</td>
    <td>Defines the line separator that should be used for parsing/writing. Maximum length is 1 character. CSV built-in functions ignore this option.</td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>unescapedQuoteHandling</code></td>
    <td>STOP_AT_DELIMITER</td>
    <td>Defines how the CsvParser will handle values with unescaped quotes.<br>
    <ul>
      <li><code>STOP_AT_CLOSING_QUOTE</code>: If unescaped quotes are found in the input, accumulate the quote character and proceed parsing the value as a quoted value, until a closing quote is found.</li>
      <li><code>BACK_TO_DELIMITER</code>: If unescaped quotes are found in the input, consider the value as an unquoted value. This will make the parser accumulate all characters of the current parsed value until the delimiter is found. If no delimiter is found in the value, the parser will continue accumulating characters from the input until a delimiter or line ending is found.</li>
      <li><code>STOP_AT_DELIMITER</code>: If unescaped quotes are found in the input, consider the value as an unquoted value. This will make the parser accumulate all characters until the delimiter or a line ending is found in the input.</li>
      <li><code>SKIP_VALUE</code>: If unescaped quotes are found in the input, the content parsed for the given value will be skipped and the value set in nullValue will be produced instead.</li>
      <li><code>RAISE_ERROR</code>: If unescaped quotes are found in the input, a TextParsingException will be thrown.</li>
    </ul>
    </td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>compression</code></td>
    <td>(none)</td>
    <td>Compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (<code>none</code>, <code>bzip2</code>, <code>gzip</code>, <code>lz4</code>, <code>snappy</code> and <code>deflate</code>). CSV built-in functions ignore this option.</td>
    <td>write</td>
  </tr>
</table>
Other generic options can be found in <a href="https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html">Generic File Source Options</a>.
