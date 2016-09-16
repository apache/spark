/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.catalyst.parser

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier

class TableIdentifierParserSuite extends SparkFunSuite {
  import CatalystSqlParser._

  // Add "$elem$", "$value$" & "$key$"
  val hiveNonReservedKeyword = Array("add", "admin", "after", "analyze", "archive", "asc", "before",
    "bucket", "buckets", "cascade", "change", "cluster", "clustered", "clusterstatus", "collection",
    "columns", "comment", "compact", "compactions", "compute", "concatenate", "continue", "data",
    "day", "databases", "datetime", "dbproperties", "deferred", "defined", "delimited",
    "dependency", "desc", "directories", "directory", "disable", "distribute",
    "enable", "escaped", "exclusive", "explain", "export", "fields", "file", "fileformat", "first",
    "format", "formatted", "functions", "hold_ddltime", "hour", "idxproperties", "ignore", "index",
    "indexes", "inpath", "inputdriver", "inputformat", "items", "jar", "keys", "key_type", "last",
    "limit", "offset", "lines", "load", "location", "lock", "locks", "logical", "long", "mapjoin",
    "materialized", "metadata", "minus", "minute", "month", "msck", "noscan", "no_drop", "nulls",
    "offline", "option", "outputdriver", "outputformat", "overwrite", "owner", "partitioned",
    "partitions", "plus", "pretty", "principals", "protection", "purge", "read", "readonly",
    "rebuild", "recordreader", "recordwriter", "reload", "rename", "repair", "replace",
    "replication", "restrict", "rewrite", "role", "roles", "schemas", "second",
    "serde", "serdeproperties", "server", "sets", "shared", "show", "show_database", "skewed",
    "sort", "sorted", "ssl", "statistics", "stored", "streamtable", "string", "struct", "tables",
    "tblproperties", "temporary", "terminated", "tinyint", "touch", "transactions", "unarchive",
    "undo", "uniontype", "unlock", "unset", "unsigned", "uri", "use", "utc", "utctimestamp",
    "view", "while", "year", "work", "transaction", "write", "isolation", "level",
    "snapshot", "autocommit", "all", "alter", "array", "as", "authorization", "between", "bigint",
    "binary", "boolean", "both", "by", "create", "cube", "current_date", "current_timestamp",
    "cursor", "date", "decimal", "delete", "describe", "double", "drop", "exists", "external",
    "false", "fetch", "float", "for", "grant", "group", "grouping", "import", "in",
    "insert", "int", "into", "is", "lateral", "like", "local", "none", "null",
    "of", "order", "out", "outer", "partition", "percent", "procedure", "range", "reads", "revoke",
    "rollup", "row", "rows", "set", "smallint", "table", "timestamp", "to", "trigger",
    "true", "truncate", "update", "user", "using", "values", "with", "regexp", "rlike",
    "bigint", "binary", "boolean", "current_date", "current_timestamp", "date", "double", "float",
    "int", "smallint", "timestamp", "at")

  val hiveStrictNonReservedKeyword = Seq("anti", "full", "inner", "left", "semi", "right",
    "natural", "union", "intersect", "except", "database", "on", "join", "cross", "select", "from",
    "where", "having", "from", "to", "table", "with", "not")

  test("table identifier") {
    // Regular names.
    assert(TableIdentifier("q") === parseTableIdentifier("q"))
    assert(TableIdentifier("q", Option("d")) === parseTableIdentifier("d.q"))

    // Illegal names.
    Seq("", "d.q.g", "t:", "${some.var.x}", "tab:1").foreach { identifier =>
      intercept[ParseException](parseTableIdentifier(identifier))
    }
  }

  test("quoted identifiers") {
    assert(TableIdentifier("z", Some("x.y")) === parseTableIdentifier("`x.y`.z"))
    assert(TableIdentifier("y.z", Some("x")) === parseTableIdentifier("x.`y.z`"))
    assert(TableIdentifier("z", Some("`x.y`")) === parseTableIdentifier("```x.y```.z"))
    assert(TableIdentifier("`y.z`", Some("x")) === parseTableIdentifier("x.```y.z```"))
    assert(TableIdentifier("x.y.z", None) === parseTableIdentifier("`x.y.z`"))
  }

  test("table identifier - strict keywords") {
    // SQL Keywords.
    hiveStrictNonReservedKeyword.foreach { keyword =>
      assert(TableIdentifier(keyword) === parseTableIdentifier(keyword))
      assert(TableIdentifier(keyword) === parseTableIdentifier(s"`$keyword`"))
      assert(TableIdentifier(keyword, Option("db")) === parseTableIdentifier(s"db.`$keyword`"))
    }
  }

  test("table identifier - non reserved keywords") {
    // Hive keywords are allowed.
    hiveNonReservedKeyword.foreach { nonReserved =>
      assert(TableIdentifier(nonReserved) === parseTableIdentifier(nonReserved))
    }
  }

  test("SPARK-17364 table identifier - contains number") {
    assert(parseTableIdentifier("123_") == TableIdentifier("123_"))
    assert(parseTableIdentifier("1a.123_") == TableIdentifier("123_", Some("1a")))
    // ".123" should not be treated as token of type DECIMAL_VALUE
    assert(parseTableIdentifier("a.123A") == TableIdentifier("123A", Some("a")))
    // ".123E3" should not be treated as token of type SCIENTIFIC_DECIMAL_VALUE
    assert(parseTableIdentifier("a.123E3_LIST") == TableIdentifier("123E3_LIST", Some("a")))
    // ".123D" should not be treated as token of type DOUBLE_LITERAL
    assert(parseTableIdentifier("a.123D_LIST") == TableIdentifier("123D_LIST", Some("a")))
    // ".123BD" should not be treated as token of type BIGDECIMAL_LITERAL
    assert(parseTableIdentifier("a.123BD_LIST") == TableIdentifier("123BD_LIST", Some("a")))
  }
}
