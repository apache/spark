---
layout: global
title: XML Files
displayTitle: XML Files
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

Spark SQL provides `spark.read().xml("file_1_path","file_2_path")` to read a file or directory of files in XML format into a Spark DataFrame, and `dataframe.write().xml("path")` to write to a xml file. The `rowTag` option must be specified to indicate the XML element that maps to a `DataFrame row`. The option() function can be used to customize the behavior of reading or writing, such as controlling behavior of the XML attributes, XSD validation, compression, and so on.

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% include_example xml_dataset python/sql/datasource.py %}
</div>

<div data-lang="scala"  markdown="1">
{% include_example xml_dataset scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala %}
</div>

<div data-lang="java"  markdown="1">
{% include_example xml_dataset java/org/apache/spark/examples/sql/JavaSQLDataSourceExample.java %}
</div>

</div>

## Data Source Option

Data source options of XML can be set via:

* the `.option`/`.options` methods of
    * `DataFrameReader`
    * `DataFrameWriter`
    * `DataStreamReader`
    * `DataStreamWriter`
* the built-in functions below
    * `from_xml`
    * `to_xml`
    * `schema_of_xml`
* `OPTIONS` clause at [CREATE TABLE USING DATA_SOURCE](sql-ref-syntax-ddl-create-table-datasource.html)

<table>
  <thead><tr><th><b>Property Name</b></th><th><b>Default</b></th><th><b>Meaning</b></th><th><b>Scope</b></th></tr></thead>
  <tr>
    <td><code>rowTag</code></td>
    <td></td>
    <td>The row tag of your xml files to treat as a row. For example, in this xml: 
        <code><xmp><books><book></book>...</books></xmp></code>
        the appropriate value would be book. This is a required option for both read and write.
    </td>
    <td>read/write</td>
  </tr>

  <tr>
    <td><code>samplingRatio</code></td>
    <td><code>1.0</code></td>
    <td>Defines fraction of rows used for schema inferring. XML built-in functions ignore this option.</td>
    <td>read</td>
  </tr>

  <tr>
    <td><code>excludeAttribute</code></td>
    <td><code>false</code></td>
    <td>Whether to exclude attributes in elements.</td>
    <td>read</td>
  </tr>

  <tr>
    <td><code>mode</code></td>
    <td><code>PERMISSIVE</code></td>
    <td>Allows a mode for dealing with corrupt records during parsing.<br>
    <ul>
      <li><code>PERMISSIVE</code>: when it meets a corrupted record, puts the malformed string into a field configured by columnNameOfCorruptRecord, and sets malformed fields to null. To keep corrupt records, an user can set a string type field named columnNameOfCorruptRecord in an user-defined schema. If a schema does not have the field, it drops corrupt records during parsing. When inferring a schema, it implicitly adds a columnNameOfCorruptRecord field in an output schema.</li>
      <li><code>DROPMALFORMED</code>: ignores the whole corrupted records. This mode is unsupported in the XML built-in functions.</li>
      <li><code>FAILFAST</code>: throws an exception when it meets corrupted records.</li>
    </ul>
    </td>
    <td>read</td>
  </tr>

  <tr>
      <td><code>inferSchema</code></td>
      <td><code>true</code></td>
      <td>If true, attempts to infer an appropriate type for each resulting DataFrame column. If false, all resulting columns are of string type.</td>
      <td>read</td>
  </tr>

  <tr>
      <td><code>columnNameOfCorruptRecord</code></td>
      <td><code>spark.sql.columnNameOfCorruptRecord</code></td>
      <td>Allows renaming the new field having a malformed string created by PERMISSIVE mode.</td>
      <td>read</td>
  </tr>

  <tr>
    <td><code>attributePrefix</code></td>
    <td><code>_</code></td>
    <td>The prefix for attributes to differentiate attributes from elements. This will be the prefix for field names. Can be empty for reading XML, but not for writing.</td>
    <td>read/write</td>
  </tr>

  <tr>
    <td><code>valueTag</code></td>
    <td><code>_VALUE</code></td>
    <td>The tag used for the value when there are attributes in the element having no child.</td>
    <td>read/write</td>
  </tr>

  <tr>
    <td><code>encoding</code></td>
    <td><code>UTF-8</code></td>
    <td>For reading, decodes the XML files by the given encoding type. For writing, specifies encoding (charset) of saved XML files. XML built-in functions ignore this option. </td>
    <td>read/write</td>
  </tr>

  <tr>
    <td><code>ignoreSurroundingSpaces</code></td>
    <td><code>true</code></td>
    <td>Defines whether surrounding whitespaces from values being read should be skipped.</td>
    <td>read</td>
  </tr>

  <tr>
      <td><code>rowValidationXSDPath</code></td>
      <td><code>null</code></td>
      <td>Path to an optional XSD file that is used to validate the XML for each row individually. Rows that fail to validate are treated like parse errors as above. The XSD does not otherwise affect the schema provided, or inferred.</td>
      <td>read</td>
  </tr>

  <tr>
      <td><code>ignoreNamespace</code></td>
      <td><code>false</code></td>
      <td>If true, namespaces prefixes on XML elements and attributes are ignored. Tags &lt;abc:author> and &lt;def:author> would, for example, be treated as if both are just &lt;author>. Note that, at the moment, namespaces cannot be ignored on the rowTag element, only its children. Note that XML parsing is in general not namespace-aware even if false.</td>
      <td>read</td>
  </tr>

  <tr>
    <td><code>timeZone</code></td>
    <td>(value of <code>spark.sql.session.timeZone</code> configuration)</td>
    <td>Sets the string that indicates a time zone ID to be used to format timestamps in the XML datasources or partition values. The following formats of timeZone are supported:<br>
    <ul>
      <li>Region-based zone ID: It should have the form 'area/city', such as 'America/Los_Angeles'.</li>
      <li>Zone offset: It should be in the format '(+|-)HH:mm', for example '-08:00' or '+01:00', also 'UTC' and 'Z' are supported as aliases of '+00:00'.</li>
    </ul>
    Other short names like 'CST' are not recommended to use because they can be ambiguous.
    </td>
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
    <td><code>dateFormat</code></td>
    <td><code>yyyy-MM-dd</code></td>
    <td>Sets the string that indicates a date format. Custom date formats follow the formats at <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html"> datetime pattern</a>. This applies to date type.</td>
    <td>read/write</td>
  </tr>

  <tr>
    <td><code>locale</code></td>
    <td><code>en-US</code></td>
    <td>Sets a locale as a language tag in IETF BCP 47 format. For instance, locale is used while parsing dates and timestamps. </td>
    <td>read/write</td>
  </tr>

  <tr>
      <td><code>rootTag</code></td>
      <td><code>ROWS</code></td>
      <td>Root tag of the xml files. For example, in this xml:
        <code><xmp><books><book></book>...</books></xmp></code>
        the appropriate value would be books. It can include basic attributes by specifying a value like 'books'
      </td>
      <td>write</td>
  </tr>

  <tr>
      <td><code>declaration</code></td>
      <td>version="1.0"
          <code>encoding="UTF-8"</code>
          standalone="yes"</td>
      <td>Content of XML declaration to write at the start of every output XML file, before the rootTag. For example, a value of foo causes <?xml foo?> to be written. Set to empty string to suppress</td>
      <td>write</td>
  </tr>

  <tr>
    <td><code>arrayElementName</code></td>
    <td><code>item</code></td>
    <td>Name of XML element that encloses each element of an array-valued column when writing.</td>
    <td>write</td>
  </tr>

  <tr>
    <td><code>nullValue</code></td>
    <td>null</td>
    <td>Sets the string representation of a null value. Default is string null. When this is null, it does not write attributes and elements for fields.</td>
    <td>read/write</td>
  </tr>

  <tr>
    <td><code>wildcardColName</code></td>
    <td><code>xs_any</code></td>
    <td>Name of a column existing in the provided schema which is interpreted as a 'wildcard'. It must have type string or array of strings. It will match any XML child element that is not otherwise matched by the schema. The XML of the child becomes the string value of the column. If an array, then all unmatched elements will be returned as an array of strings. As its name implies, it is meant to emulate XSD's xs:any type.</td>
    <td>read</td>
  </tr>

  <tr>
    <td><code>compression</code></td>
    <td><code>none</code></td>
    <td>Compression codec to use when saving to file. This can be one of the known case-insensitive shortened names (none, bzip2, gzip, lz4, snappy and deflate). XML built-in functions ignore this option.</td>
    <td>write</td>
  </tr>

  <tr>
      <td><code>validateName</code></td>
      <td><code>true</code></td>
      <td>If true, throws error on XML element name validation failure. For example, SQL field names can have spaces, but XML element names cannot.</td>
      <td>write</td>
  </tr>

</table>
Other generic options can be found in <a href="https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html"> Generic File Source Options</a>.
