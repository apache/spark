/**
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

   This file is an adaptation of Hive's org/apache/hadoop/hive/ql/HiveLexer.g grammar.
*/
lexer grammar SparkSqlLexer;

@lexer::header {
package org.apache.spark.sql.catalyst.parser;

}

@lexer::members {
  private ParserConf parserConf;
  private ParseErrorReporter reporter;
  
  public void configure(ParserConf parserConf, ParseErrorReporter reporter) {
    this.parserConf = parserConf;
    this.reporter = reporter;
  }
  
  protected boolean allowQuotedId() {
    if (parserConf == null) {
      return true;
    }
    return parserConf.supportQuotedId();
  }

  @Override
  public void displayRecognitionError(String[] tokenNames, RecognitionException e) {
    if (reporter != null) {
      reporter.report(this, e, tokenNames);
    }
  }
}

// Keywords

KW_TRUE : 'TRUE';
KW_FALSE : 'FALSE';
KW_ALL : 'ALL';
KW_NONE: 'NONE';
KW_AND : 'AND';
KW_OR : 'OR';
KW_NOT : 'NOT' | '!';
KW_LIKE : 'LIKE';

KW_IF : 'IF';
KW_EXISTS : 'EXISTS';

KW_ASC : 'ASC';
KW_DESC : 'DESC';
KW_ORDER : 'ORDER';
KW_GROUP : 'GROUP';
KW_BY : 'BY';
KW_HAVING : 'HAVING';
KW_WHERE : 'WHERE';
KW_FROM : 'FROM';
KW_AS : 'AS';
KW_SELECT : 'SELECT';
KW_DISTINCT : 'DISTINCT';
KW_INSERT : 'INSERT';
KW_OVERWRITE : 'OVERWRITE';
KW_OUTER : 'OUTER';
KW_UNIQUEJOIN : 'UNIQUEJOIN';
KW_PRESERVE : 'PRESERVE';
KW_JOIN : 'JOIN';
KW_LEFT : 'LEFT';
KW_RIGHT : 'RIGHT';
KW_FULL : 'FULL';
KW_ANTI : 'ANTI';
KW_ON : 'ON';
KW_PARTITION : 'PARTITION';
KW_PARTITIONS : 'PARTITIONS';
KW_TABLE: 'TABLE';
KW_TABLES: 'TABLES';
KW_COLUMNS: 'COLUMNS';
KW_INDEX: 'INDEX';
KW_INDEXES: 'INDEXES';
KW_REBUILD: 'REBUILD';
KW_FUNCTIONS: 'FUNCTIONS';
KW_SHOW: 'SHOW';
KW_MSCK: 'MSCK';
KW_REPAIR: 'REPAIR';
KW_DIRECTORY: 'DIRECTORY';
KW_LOCAL: 'LOCAL';
KW_TRANSFORM : 'TRANSFORM';
KW_USING: 'USING';
KW_CLUSTER: 'CLUSTER';
KW_DISTRIBUTE: 'DISTRIBUTE';
KW_SORT: 'SORT';
KW_UNION: 'UNION';
KW_EXCEPT: 'EXCEPT';
KW_LOAD: 'LOAD';
KW_EXPORT: 'EXPORT';
KW_IMPORT: 'IMPORT';
KW_REPLICATION: 'REPLICATION';
KW_METADATA: 'METADATA';
KW_DATA: 'DATA';
KW_INPATH: 'INPATH';
KW_IS: 'IS';
KW_NULL: 'NULL';
KW_CREATE: 'CREATE';
KW_EXTERNAL: 'EXTERNAL';
KW_ALTER: 'ALTER';
KW_CHANGE: 'CHANGE';
KW_COLUMN: 'COLUMN';
KW_FIRST: 'FIRST';
KW_AFTER: 'AFTER';
KW_DESCRIBE: 'DESCRIBE';
KW_DROP: 'DROP';
KW_RENAME: 'RENAME';
KW_TO: 'TO';
KW_COMMENT: 'COMMENT';
KW_BOOLEAN: 'BOOLEAN';
KW_TINYINT: 'TINYINT';
KW_SMALLINT: 'SMALLINT';
KW_INT: 'INT';
KW_BIGINT: 'BIGINT';
KW_FLOAT: 'FLOAT';
KW_DOUBLE: 'DOUBLE';
KW_DATE: 'DATE';
KW_DATETIME: 'DATETIME';
KW_TIMESTAMP: 'TIMESTAMP';
KW_INTERVAL: 'INTERVAL';
KW_DECIMAL: 'DECIMAL';
KW_STRING: 'STRING';
KW_CHAR: 'CHAR';
KW_VARCHAR: 'VARCHAR';
KW_ARRAY: 'ARRAY';
KW_STRUCT: 'STRUCT';
KW_MAP: 'MAP';
KW_UNIONTYPE: 'UNIONTYPE';
KW_REDUCE: 'REDUCE';
KW_PARTITIONED: 'PARTITIONED';
KW_CLUSTERED: 'CLUSTERED';
KW_SORTED: 'SORTED';
KW_INTO: 'INTO';
KW_BUCKETS: 'BUCKETS';
KW_ROW: 'ROW';
KW_ROWS: 'ROWS';
KW_FORMAT: 'FORMAT';
KW_DELIMITED: 'DELIMITED';
KW_FIELDS: 'FIELDS';
KW_TERMINATED: 'TERMINATED';
KW_ESCAPED: 'ESCAPED';
KW_COLLECTION: 'COLLECTION';
KW_ITEMS: 'ITEMS';
KW_KEYS: 'KEYS';
KW_KEY_TYPE: '$KEY$';
KW_LINES: 'LINES';
KW_STORED: 'STORED';
KW_FILEFORMAT: 'FILEFORMAT';
KW_INPUTFORMAT: 'INPUTFORMAT';
KW_OUTPUTFORMAT: 'OUTPUTFORMAT';
KW_INPUTDRIVER: 'INPUTDRIVER';
KW_OUTPUTDRIVER: 'OUTPUTDRIVER';
KW_ENABLE: 'ENABLE';
KW_DISABLE: 'DISABLE';
KW_LOCATION: 'LOCATION';
KW_TABLESAMPLE: 'TABLESAMPLE';
KW_BUCKET: 'BUCKET';
KW_OUT: 'OUT';
KW_OF: 'OF';
KW_PERCENT: 'PERCENT';
KW_CAST: 'CAST';
KW_ADD: 'ADD';
KW_REPLACE: 'REPLACE';
KW_RLIKE: 'RLIKE';
KW_REGEXP: 'REGEXP';
KW_TEMPORARY: 'TEMPORARY';
KW_FUNCTION: 'FUNCTION';
KW_MACRO: 'MACRO';
KW_FILE: 'FILE';
KW_JAR: 'JAR';
KW_EXPLAIN: 'EXPLAIN';
KW_EXTENDED: 'EXTENDED';
KW_FORMATTED: 'FORMATTED';
KW_PRETTY: 'PRETTY';
KW_DEPENDENCY: 'DEPENDENCY';
KW_LOGICAL: 'LOGICAL';
KW_SERDE: 'SERDE';
KW_WITH: 'WITH';
KW_DEFERRED: 'DEFERRED';
KW_SERDEPROPERTIES: 'SERDEPROPERTIES';
KW_DBPROPERTIES: 'DBPROPERTIES';
KW_LIMIT: 'LIMIT';
KW_SET: 'SET';
KW_UNSET: 'UNSET';
KW_TBLPROPERTIES: 'TBLPROPERTIES';
KW_IDXPROPERTIES: 'IDXPROPERTIES';
KW_VALUE_TYPE: '$VALUE$';
KW_ELEM_TYPE: '$ELEM$';
KW_DEFINED: 'DEFINED';
KW_CASE: 'CASE';
KW_WHEN: 'WHEN';
KW_THEN: 'THEN';
KW_ELSE: 'ELSE';
KW_END: 'END';
KW_MAPJOIN: 'MAPJOIN';
KW_STREAMTABLE: 'STREAMTABLE';
KW_CLUSTERSTATUS: 'CLUSTERSTATUS';
KW_UTC: 'UTC';
KW_UTCTIMESTAMP: 'UTC_TMESTAMP';
KW_LONG: 'LONG';
KW_DELETE: 'DELETE';
KW_PLUS: 'PLUS';
KW_MINUS: 'MINUS';
KW_FETCH: 'FETCH';
KW_INTERSECT: 'INTERSECT';
KW_VIEW: 'VIEW';
KW_IN: 'IN';
KW_DATABASE: 'DATABASE';
KW_DATABASES: 'DATABASES';
KW_MATERIALIZED: 'MATERIALIZED';
KW_SCHEMA: 'SCHEMA';
KW_SCHEMAS: 'SCHEMAS';
KW_GRANT: 'GRANT';
KW_REVOKE: 'REVOKE';
KW_SSL: 'SSL';
KW_UNDO: 'UNDO';
KW_LOCK: 'LOCK';
KW_LOCKS: 'LOCKS';
KW_UNLOCK: 'UNLOCK';
KW_SHARED: 'SHARED';
KW_EXCLUSIVE: 'EXCLUSIVE';
KW_PROCEDURE: 'PROCEDURE';
KW_UNSIGNED: 'UNSIGNED';
KW_WHILE: 'WHILE';
KW_READ: 'READ';
KW_READS: 'READS';
KW_PURGE: 'PURGE';
KW_RANGE: 'RANGE';
KW_ANALYZE: 'ANALYZE';
KW_BEFORE: 'BEFORE';
KW_BETWEEN: 'BETWEEN';
KW_BOTH: 'BOTH';
KW_BINARY: 'BINARY';
KW_CROSS: 'CROSS';
KW_CONTINUE: 'CONTINUE';
KW_CURSOR: 'CURSOR';
KW_TRIGGER: 'TRIGGER';
KW_RECORDREADER: 'RECORDREADER';
KW_RECORDWRITER: 'RECORDWRITER';
KW_SEMI: 'SEMI';
KW_LATERAL: 'LATERAL';
KW_TOUCH: 'TOUCH';
KW_ARCHIVE: 'ARCHIVE';
KW_UNARCHIVE: 'UNARCHIVE';
KW_COMPUTE: 'COMPUTE';
KW_STATISTICS: 'STATISTICS';
KW_USE: 'USE';
KW_OPTION: 'OPTION';
KW_CONCATENATE: 'CONCATENATE';
KW_SHOW_DATABASE: 'SHOW_DATABASE';
KW_UPDATE: 'UPDATE';
KW_RESTRICT: 'RESTRICT';
KW_CASCADE: 'CASCADE';
KW_SKEWED: 'SKEWED';
KW_ROLLUP: 'ROLLUP';
KW_CUBE: 'CUBE';
KW_DIRECTORIES: 'DIRECTORIES';
KW_FOR: 'FOR';
KW_WINDOW: 'WINDOW';
KW_UNBOUNDED: 'UNBOUNDED';
KW_PRECEDING: 'PRECEDING';
KW_FOLLOWING: 'FOLLOWING';
KW_CURRENT: 'CURRENT';
KW_CURRENT_DATE: 'CURRENT_DATE';
KW_CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
KW_LESS: 'LESS';
KW_MORE: 'MORE';
KW_OVER: 'OVER';
KW_GROUPING: 'GROUPING';
KW_SETS: 'SETS';
KW_TRUNCATE: 'TRUNCATE';
KW_NOSCAN: 'NOSCAN';
KW_PARTIALSCAN: 'PARTIALSCAN';
KW_USER: 'USER';
KW_ROLE: 'ROLE';
KW_ROLES: 'ROLES';
KW_INNER: 'INNER';
KW_EXCHANGE: 'EXCHANGE';
KW_URI: 'URI';
KW_SERVER : 'SERVER';
KW_ADMIN: 'ADMIN';
KW_OWNER: 'OWNER';
KW_PRINCIPALS: 'PRINCIPALS';
KW_COMPACT: 'COMPACT';
KW_COMPACTIONS: 'COMPACTIONS';
KW_TRANSACTIONS: 'TRANSACTIONS';
KW_REWRITE : 'REWRITE';
KW_AUTHORIZATION: 'AUTHORIZATION';
KW_CONF: 'CONF';
KW_VALUES: 'VALUES';
KW_RELOAD: 'RELOAD';
KW_YEAR: 'YEAR'|'YEARS';
KW_MONTH: 'MONTH'|'MONTHS';
KW_DAY: 'DAY'|'DAYS';
KW_HOUR: 'HOUR'|'HOURS';
KW_MINUTE: 'MINUTE'|'MINUTES';
KW_SECOND: 'SECOND'|'SECONDS';
KW_START: 'START';
KW_TRANSACTION: 'TRANSACTION';
KW_COMMIT: 'COMMIT';
KW_ROLLBACK: 'ROLLBACK';
KW_WORK: 'WORK';
KW_ONLY: 'ONLY';
KW_WRITE: 'WRITE';
KW_ISOLATION: 'ISOLATION';
KW_LEVEL: 'LEVEL';
KW_SNAPSHOT: 'SNAPSHOT';
KW_AUTOCOMMIT: 'AUTOCOMMIT';
KW_REFRESH: 'REFRESH';
KW_OPTIONS: 'OPTIONS';
KW_WEEK: 'WEEK'|'WEEKS';
KW_MILLISECOND: 'MILLISECOND'|'MILLISECONDS';
KW_MICROSECOND: 'MICROSECOND'|'MICROSECONDS';
KW_CLEAR: 'CLEAR';
KW_LAZY: 'LAZY';
KW_CACHE: 'CACHE';
KW_UNCACHE: 'UNCACHE';
KW_DFS: 'DFS';

KW_NATURAL: 'NATURAL';

// Operators
// NOTE: if you add a new function/operator, add it to sysFuncNames so that describe function _FUNC_ will work.

DOT : '.'; // generated as a part of Number rule
COLON : ':' ;
COMMA : ',' ;
SEMICOLON : ';' ;

LPAREN : '(' ;
RPAREN : ')' ;
LSQUARE : '[' ;
RSQUARE : ']' ;
LCURLY : '{';
RCURLY : '}';

EQUAL : '=' | '==';
EQUAL_NS : '<=>';
NOTEQUAL : '<>' | '!=';
LESSTHANOREQUALTO : '<=';
LESSTHAN : '<';
GREATERTHANOREQUALTO : '>=';
GREATERTHAN : '>';

DIVIDE : '/';
PLUS : '+';
MINUS : '-';
STAR : '*';
MOD : '%';
DIV : 'DIV';

AMPERSAND : '&';
TILDE : '~';
BITWISEOR : '|';
BITWISEXOR : '^';
QUESTION : '?';
DOLLAR : '$';

// LITERALS
fragment
Letter
    : 'a'..'z' | 'A'..'Z'
    ;

fragment
HexDigit
    : 'a'..'f' | 'A'..'F'
    ;

fragment
Digit
    :
    '0'..'9'
    ;

fragment
Exponent
    :
    ('e' | 'E') ( PLUS|MINUS )? (Digit)+
    ;

fragment
RegexComponent
    : 'a'..'z' | 'A'..'Z' | '0'..'9' | '_'
    | PLUS | STAR | QUESTION | MINUS | DOT
    | LPAREN | RPAREN | LSQUARE | RSQUARE | LCURLY | RCURLY
    | BITWISEXOR | BITWISEOR | DOLLAR | '!'
    ;

StringLiteral
    :
    ( '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '\"' ( ~('\"'|'\\') | ('\\' .) )* '\"'
    )+
    ;

BigintLiteral
    :
    (Digit)+ 'L'
    ;

SmallintLiteral
    :
    (Digit)+ 'S'
    ;

TinyintLiteral
    :
    (Digit)+ 'Y'
    ;

DoubleLiteral
    :
    Number 'D'
    ;

ByteLengthLiteral
    :
    (Digit)+ ('b' | 'B' | 'k' | 'K' | 'm' | 'M' | 'g' | 'G')
    ;

Number
    :
    ((Digit+ (DOT Digit*)?) | (DOT Digit+)) Exponent?
    ;

/*
An Identifier can be:
- tableName
- columnName
- select expr alias
- lateral view aliases
- database name
- view name
- subquery alias
- function name
- ptf argument identifier
- index name
- property name for: db,tbl,partition...
- fileFormat
- role name
- privilege name
- principal name
- macro name
- hint name
- window name
*/
Identifier
    :
    (Letter | Digit | '_')+
    | {allowQuotedId()}? QuotedIdentifier  /* though at the language level we allow all Identifiers to be QuotedIdentifiers; 
                                              at the API level only columns are allowed to be of this form */
    | '`' RegexComponent+ '`'
    ;

fragment    
QuotedIdentifier 
    :
    '`'  ( '``' | ~('`') )* '`' { setText(getText().replaceAll("``", "`")); }
    ;

WS  :  (' '|'\r'|'\t'|'\n') {$channel=HIDDEN;}
    ;

COMMENT
  : '--' (~('\n'|'\r'))*
    { $channel=HIDDEN; }
  ;

/* Prevent that the lexer swallows unknown characters. */
ANY
 :.
 ;
