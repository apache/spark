---
layout: global
title: Built-in JSON Functions
displayTitle: Built-in JSON Functions
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

JSON functions are used for operating on JSON strings. Table lists all the JSON function supported
by Spark.

<table class="table">
   <thead>
     <tr><th style="width:25%">Function</th><th>Arguments</th><th>Description</th></tr>
   </thead>
   <tbody>
     <tr>
       <td><b>get_json_object</b>(<i>json, path</i>)</td>
       <td>a JSON string; JSON path</td>
       <td>Extracts json object from a json string based on json path specified, and returns json
       string of the extracted json object. It will return null if the input json string is invalid.
       </td>
     </tr>
     <tr>
       <td><b>json_tuple</b>(<i>json, fields*</i>)</td>
       <td>a JSON string; field names as string</td>
       <td>Creates a new row for a json column according to the given field names. At least one field
           name must be given.</td>
     </tr>
     <tr>
       <td><b>from_json</b>(<i>expression, schema, options</i>)</td>
       <td>expression contains a JSON string; schema to use when parsing the JSON string; options to
           to control how JSON is parsed</td>
       <td>Parses an expression containing a JSON string into a `MapType` with `StringType` as keys
           type, `StructType` or `ArrayType` of `StructType`s with the specified schema. Returns
           `null`, in the case of an unparseable string.</td>
     </tr>
     <tr>
       <td><b>to_json</b>(<i>expression, options</i>)</td>
       <td>expression contains a struct, an array or a map; options to control how the struct column
           is converted into a json string.</td>
       <td>Converts a column containing a `StructType`, `ArrayType` or a `MapType` into a JSON
           string with the specified schema. Throws an exception, in the case of an unsupported
           type. The function supports the `pretty` option which enables pretty JSON generation.
       </td>
     </tr>
     <tr>
       <td><b>schema_of_json</b>(<i>json, options</i>)</td>
       <td>JSON string; options to control how JSON is parsed</td>
       <td>Parses a JSON string and infers its schema in DDL format using options.</td>
     </tr>
   </tbody>
</table>

### Examples

{% highlight sql %}

-- get_json_object

SELECT get_json_object('{"id" : 1, "name" : "joy"}', '$.name');
+---------------------------------------------------+
|get_json_object({"id" : 1, "name" : "joy"}, $.name)|
+---------------------------------------------------+
|                                                joy|
+---------------------------------------------------+

SELECT get_json_object('[1, 2, 3]', '$.1');
+-------------------------------+
|get_json_object([1, 2, 3], $.1)|
+-------------------------------+
|                           null|
+-------------------------------+

SELECT get_json_object('{"id" : 1, "name" : "joy"}', '$.age');
+--------------------------------------------------+
|get_json_object({"id" : 1, "name" : "joy"}, $.age)|
+--------------------------------------------------+
|                                              null|
+--------------------------------------------------+

-- json_tuple

SELECT json_tuple('{"id" : 1, "name" : "joy"}', 'id', 'name');
+---+---+
| c0| c1|
+---+---+
|  1|joy|
+---+---+

SELECT json_tuple('{"id" : 1, "name" : "joy"}', 'id', 'name', 'age');
+---+---+----+
| c0| c1|  c2|
+---+---+----+
|  1|joy|null|
+---+---+----+

SELECT json_tuple('{"a" : 1, "b" : 2}', CAST(NULL AS STRING), 'b', CAST(NULL AS STRING), 'a');
+----+---+----+---+
|  c0| c1|  c2| c3|
+----+---+----+---+
|null|  2|null|  1|
+----+---+----+---+

-- from_json

SELECT from_json('{"id":1}', 'id INT');
+------------------+
|from_json({"id":1})|
+------------------+
|               [1]|
+------------------+

SELECT from_json('[1, 2, 3]', 'array<int>');
+--------------------+
|from_json([1, 2, 3])|
+--------------------+
|           [1, 2, 3]|
+--------------------+

SELECT from_json('[{"a": 1}, {"a":2}]', 'array<struct<a:int>>');
+------------------------------+
|from_json([{"a": 1}, {"a":2}])|
+------------------------------+
|                    [[1], [2]]|
+------------------------------+

SELECT from_json('{"a":1, "b":2}', 'map<string, int>');
+----------------+
|         entries|
+----------------+
|[a -> 1, b -> 2]|
+----------------+

SELECT from_json('[1, "2", 3]', 'array<int>');
+----------------------+
|from_json([1, "2", 3])|
+----------------------+
|                  null|
+----------------------+

-- to_json

SELECT to_json(array('1', '2', '3'));
+-----------------------+
|to_json(array(1, 2, 3))|
+-----------------------+
|          ["1","2","3"]|
+-----------------------+

SELECT to_json(array(map('a',1)));
+-------------------------+
|to_json(array(map(a, 1)))|
+-------------------------+
|                [{"a":1}]|
+-------------------------+

SELECT to_json(array(array(1, 2, 3), array(4)));
+----------------------------------------+
|to_json(array(array(1, 2, 3), array(4)))|
+----------------------------------------+
|                           [[1,2,3],[4]]|
+----------------------------------------+

SELECT to_json(map('id', 1, 'name', 'joy'), map('pretty', 'true'));
+----------------------------------------------+
|to_json(map(id, CAST(1 AS STRING), name, joy))|
+----------------------------------------------+
|{                                             |
|  "id" : "1",                                 |
|  "name" : "joy"                              |
|}                                             |
+----------------------------------------------+

-- schema_of_json

SELECT schema_of_json('{"id" : 1, "name" : "joy"}');
+------------------------------------------+
|schema_of_json({"id" : 1, "name" : "joy"})|
+------------------------------------------+
|struct<id:bigint,name:string>             |
+------------------------------------------+

SELECT schema_of_json('[1, 2]');
+----------------------+
|schema_of_json([1, 2])|
+----------------------+
|array<bigint>         |
+----------------------+

SELECT schema_of_json('{"id" : 1, "name" : "joy", "age" : null}', map('dropFieldIfAllNull', 'true'));
+--------------------------------------------------------+
|schema_of_json({"id" : 1, "name" : "joy", "age" : null})|
+--------------------------------------------------------+
|struct<id:bigint,name:string>                           |
+--------------------------------------------------------+

{% endhighlight %}