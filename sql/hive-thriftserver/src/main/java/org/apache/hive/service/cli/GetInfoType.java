/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.cli;

import org.apache.hive.service.rpc.thrift.TGetInfoType;

/**
 * GetInfoType.
 *
 */
public enum GetInfoType {
  CLI_MAX_DRIVER_CONNECTIONS(TGetInfoType.CLI_MAX_DRIVER_CONNECTIONS),
  CLI_MAX_CONCURRENT_ACTIVITIES(TGetInfoType.CLI_MAX_CONCURRENT_ACTIVITIES),
  CLI_DATA_SOURCE_NAME(TGetInfoType.CLI_DATA_SOURCE_NAME),
  CLI_FETCH_DIRECTION(TGetInfoType.CLI_FETCH_DIRECTION),
  CLI_SERVER_NAME(TGetInfoType.CLI_SERVER_NAME),
  CLI_SEARCH_PATTERN_ESCAPE(TGetInfoType.CLI_SEARCH_PATTERN_ESCAPE),
  CLI_DBMS_NAME(TGetInfoType.CLI_DBMS_NAME),
  CLI_DBMS_VER(TGetInfoType.CLI_DBMS_VER),
  CLI_ACCESSIBLE_TABLES(TGetInfoType.CLI_ACCESSIBLE_TABLES),
  CLI_ACCESSIBLE_PROCEDURES(TGetInfoType.CLI_ACCESSIBLE_PROCEDURES),
  CLI_CURSOR_COMMIT_BEHAVIOR(TGetInfoType.CLI_CURSOR_COMMIT_BEHAVIOR),
  CLI_DATA_SOURCE_READ_ONLY(TGetInfoType.CLI_DATA_SOURCE_READ_ONLY),
  CLI_DEFAULT_TXN_ISOLATION(TGetInfoType.CLI_DEFAULT_TXN_ISOLATION),
  CLI_IDENTIFIER_CASE(TGetInfoType.CLI_IDENTIFIER_CASE),
  CLI_IDENTIFIER_QUOTE_CHAR(TGetInfoType.CLI_IDENTIFIER_QUOTE_CHAR),
  CLI_MAX_COLUMN_NAME_LEN(TGetInfoType.CLI_MAX_COLUMN_NAME_LEN),
  CLI_MAX_CURSOR_NAME_LEN(TGetInfoType.CLI_MAX_CURSOR_NAME_LEN),
  CLI_MAX_SCHEMA_NAME_LEN(TGetInfoType.CLI_MAX_SCHEMA_NAME_LEN),
  CLI_MAX_CATALOG_NAME_LEN(TGetInfoType.CLI_MAX_CATALOG_NAME_LEN),
  CLI_MAX_TABLE_NAME_LEN(TGetInfoType.CLI_MAX_TABLE_NAME_LEN),
  CLI_SCROLL_CONCURRENCY(TGetInfoType.CLI_SCROLL_CONCURRENCY),
  CLI_TXN_CAPABLE(TGetInfoType.CLI_TXN_CAPABLE),
  CLI_USER_NAME(TGetInfoType.CLI_USER_NAME),
  CLI_TXN_ISOLATION_OPTION(TGetInfoType.CLI_TXN_ISOLATION_OPTION),
  CLI_INTEGRITY(TGetInfoType.CLI_INTEGRITY),
  CLI_GETDATA_EXTENSIONS(TGetInfoType.CLI_GETDATA_EXTENSIONS),
  CLI_NULL_COLLATION(TGetInfoType.CLI_NULL_COLLATION),
  CLI_ALTER_TABLE(TGetInfoType.CLI_ALTER_TABLE),
  CLI_ORDER_BY_COLUMNS_IN_SELECT(TGetInfoType.CLI_ORDER_BY_COLUMNS_IN_SELECT),
  CLI_SPECIAL_CHARACTERS(TGetInfoType.CLI_SPECIAL_CHARACTERS),
  CLI_MAX_COLUMNS_IN_GROUP_BY(TGetInfoType.CLI_MAX_COLUMNS_IN_GROUP_BY),
  CLI_MAX_COLUMNS_IN_INDEX(TGetInfoType.CLI_MAX_COLUMNS_IN_INDEX),
  CLI_MAX_COLUMNS_IN_ORDER_BY(TGetInfoType.CLI_MAX_COLUMNS_IN_ORDER_BY),
  CLI_MAX_COLUMNS_IN_SELECT(TGetInfoType.CLI_MAX_COLUMNS_IN_SELECT),
  CLI_MAX_COLUMNS_IN_TABLE(TGetInfoType.CLI_MAX_COLUMNS_IN_TABLE),
  CLI_MAX_INDEX_SIZE(TGetInfoType.CLI_MAX_INDEX_SIZE),
  CLI_MAX_ROW_SIZE(TGetInfoType.CLI_MAX_ROW_SIZE),
  CLI_MAX_STATEMENT_LEN(TGetInfoType.CLI_MAX_STATEMENT_LEN),
  CLI_MAX_TABLES_IN_SELECT(TGetInfoType.CLI_MAX_TABLES_IN_SELECT),
  CLI_MAX_USER_NAME_LEN(TGetInfoType.CLI_MAX_USER_NAME_LEN),
  CLI_OJ_CAPABILITIES(TGetInfoType.CLI_OJ_CAPABILITIES),

  CLI_XOPEN_CLI_YEAR(TGetInfoType.CLI_XOPEN_CLI_YEAR),
  CLI_CURSOR_SENSITIVITY(TGetInfoType.CLI_CURSOR_SENSITIVITY),
  CLI_DESCRIBE_PARAMETER(TGetInfoType.CLI_DESCRIBE_PARAMETER),
  CLI_CATALOG_NAME(TGetInfoType.CLI_CATALOG_NAME),
  CLI_COLLATION_SEQ(TGetInfoType.CLI_COLLATION_SEQ),
  CLI_MAX_IDENTIFIER_LEN(TGetInfoType.CLI_MAX_IDENTIFIER_LEN),
  CLI_ODBC_KEYWORDS(TGetInfoType.CLI_ODBC_KEYWORDS);

  private final TGetInfoType tInfoType;

  GetInfoType(TGetInfoType tInfoType) {
    this.tInfoType = tInfoType;
  }

  public static GetInfoType getGetInfoType(TGetInfoType tGetInfoType) {
    for (GetInfoType infoType : values()) {
      if (tGetInfoType.equals(infoType.tInfoType)) {
        return infoType;
      }
    }
    throw new IllegalArgumentException("Unrecognized Thrift TGetInfoType value: " + tGetInfoType);
  }

  public TGetInfoType toTGetInfoType() {
    return tInfoType;
  }

}
