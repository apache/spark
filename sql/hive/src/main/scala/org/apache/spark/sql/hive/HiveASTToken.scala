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

package org.apache.spark.sql.hive

private[hive] object HiveASTToken {
  protected val nativeCommands = Seq(
    "TOK_ALTERDATABASE_OWNER",
    "TOK_ALTERDATABASE_PROPERTIES",
    "TOK_ALTERINDEX_PROPERTIES",
    "TOK_ALTERINDEX_REBUILD",
    "TOK_ALTERTABLE_ADDCOLS",
    "TOK_ALTERTABLE_ADDPARTS",
    "TOK_ALTERTABLE_ALTERPARTS",
    "TOK_ALTERTABLE_ARCHIVE",
    "TOK_ALTERTABLE_CLUSTER_SORT",
    "TOK_ALTERTABLE_DROPPARTS",
    "TOK_ALTERTABLE_PARTITION",
    "TOK_ALTERTABLE_PROPERTIES",
    "TOK_ALTERTABLE_RENAME",
    "TOK_ALTERTABLE_RENAMECOL",
    "TOK_ALTERTABLE_REPLACECOLS",
    "TOK_ALTERTABLE_SKEWED",
    "TOK_ALTERTABLE_TOUCH",
    "TOK_ALTERTABLE_UNARCHIVE",
    "TOK_ALTERVIEW_ADDPARTS",
    "TOK_ALTERVIEW_AS",
    "TOK_ALTERVIEW_DROPPARTS",
    "TOK_ALTERVIEW_PROPERTIES",
    "TOK_ALTERVIEW_RENAME",

    "TOK_CREATEDATABASE",
    "TOK_CREATEFUNCTION",
    "TOK_CREATEINDEX",
    "TOK_CREATEROLE",
    "TOK_CREATEVIEW",

    "TOK_DESCDATABASE",
    "TOK_DESCFUNCTION",

    "TOK_DROPDATABASE",
    "TOK_DROPFUNCTION",
    "TOK_DROPINDEX",
    "TOK_DROPROLE",
    "TOK_DROPTABLE_PROPERTIES",
    "TOK_DROPVIEW",
    "TOK_DROPVIEW_PROPERTIES",

    "TOK_EXPORT",

    "TOK_GRANT",
    "TOK_GRANT_ROLE",

    "TOK_IMPORT",

    "TOK_LOAD",

    "TOK_LOCKTABLE",

    "TOK_MSCK",

    "TOK_REVOKE",

    "TOK_SHOW_COMPACTIONS",
    "TOK_SHOW_CREATETABLE",
    "TOK_SHOW_GRANT",
    "TOK_SHOW_ROLE_GRANT",
    "TOK_SHOW_ROLE_PRINCIPALS",
    "TOK_SHOW_ROLES",
    "TOK_SHOW_SET_ROLE",
    "TOK_SHOW_TABLESTATUS",
    "TOK_SHOW_TBLPROPERTIES",
    "TOK_SHOW_TRANSACTIONS",
    "TOK_SHOWCOLUMNS",
    "TOK_SHOWDATABASES",
    "TOK_SHOWFUNCTIONS",
    "TOK_SHOWINDEXES",
    "TOK_SHOWLOCKS",
    "TOK_SHOWPARTITIONS",

    "TOK_SWITCHDATABASE",

    "TOK_UNLOCKTABLE"
  )

  // Commands that we do not need to explain.
  protected val noExplainCommands = Seq(
    "TOK_DESCTABLE",
    "TOK_SHOWTABLES",
    "TOK_TRUNCATETABLE"     // truncate table" is a NativeCommand, does not need to explain.
  ) ++ nativeCommands
}
