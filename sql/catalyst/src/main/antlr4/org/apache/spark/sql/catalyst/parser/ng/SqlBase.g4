/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file is an adaptation of Presto's presto-parser/src/main/antlr4/com/facebook/presto/sql/parser/SqlBase.g4 grammar.
 */

grammar SqlBase;

tokens {
    DELIMITER
}

singleStatement
    : statement EOF
    ;

singleExpression
    : namedExpression EOF
    ;

singleTableIdentifier
    : tableIdentifier EOF
    ;

singleDataType
    : dataType EOF
    ;

statement
    : query                                                            #statementDefault
    | USE db=identifier                                                #use
    | CREATE DATABASE (IF NOT EXISTS)? identifier
        (COMMENT comment=STRING)? (LOCATION location=STRING)?
        (WITH DBPROPERTIES tablePropertyList)?                         #createDatabase
    | createTable ('(' colTypeList ')')? tableProvider tableProperties #createTableUsing
    | createTable tableProvider tableProperties? AS? query             #createTableUsingAsSelect
    | CREATE TEMPORARY? FUNCTION qualifiedName AS className=STRING
        (USING resource (',' resource)*)?                              #createFunction
    | DROP TABLE (IF EXISTS)? tableIdentifier                          #dropTable                   // TODO HIVE - add purge/replication clause
    | DELETE FROM tableIdentifier (WHERE booleanExpression)?           #delete                      // TODO HIVE
    | ALTER TABLE from=tableIdentifier RENAME TO to=tableIdentifier    #renameTable
    | ALTER TABLE tableIdentifier
        SET TBLPROPERTIES tablePropertyList                            #setTableProperties
    | ALTER TABLE tableIdentifier
        UNSET TBLPROPERTIES (IF EXISTS)? tablePropertyList             #unsetTableProperties
    | ALTER TABLE tableIdentifier (partitionSpec)?
        SET SERDE STRING (WITH SERDEPROPERTIES tablePropertyList)?     #setTableSerDe
    | ALTER TABLE tableIdentifier (partitionSpec)?
            SET SERDEPROPERTIES tablePropertyList                      #setTableSerDe
    | ALTER TABLE tableIdentifier CLUSTERED BY identifierList
            (SORTED BY orderedIdentifierList)?
            INTO INTEGER_VALUE BUCKETS                                 #bucketTable
    | ALTER TABLE tableIdentifier NOT CLUSTERED                        #unclusterTable
    | ALTER TABLE tableIdentifier NOT SORTED                           #unsortTable
    | ALTER TABLE tableIdentifier SKEWED BY identifierList
       ON (constantList | nestedConstantList)
       (STORED AS DIRECTORIES)?                                        #skewTable
    | ALTER TABLE tableIdentifier NOT SKEWED                           #unskewTable
    | ALTER TABLE tableIdentifier NOT STORED AS DIRECTORIES            #unstoreTable
    | ALTER TABLE tableIdentifier
         SET SKEWED LOCATION skewedLocationList                        #setTableSkewLocations
    | ALTER TABLE tableIdentifier ADD (IF NOT EXISTS)?
         partitionSpecLocation+                                        #addTablePartition
    | ALTER TABLE tableIdentifier
         from=partitionSpec RENAME TO to=partitionSpec                 #renameTablePartition
    | ALTER TABLE from=tableIdentifier
        EXCHANGE partitionSpec WITH TABLE to=tableIdentifier           #exchangeTablePartition
    | ALTER TABLE tableIdentifier
        DROP (IF EXISTS)? partitionSpec (',' partitionSpec)* PURGE?    #dropTablePartitions
    | ALTER TABLE tableIdentifier ARCHIVE partitionSpec                #archiveTablePartition
    | ALTER TABLE tableIdentifier UNARCHIVE partitionSpec              #unarchiveTablePartition
    | ALTER TABLE tableIdentifier partitionSpec?
        SET FILEFORMAT fileFormat                                      #setTableFileFormat
    | ALTER TABLE tableIdentifier partitionSpec? SET LOCATION STRING   #setTableLocation
    | ALTER TABLE tableIdentifier TOUCH partitionSpec?                 #touchTable
    | ALTER TABLE tableIdentifier partitionSpec? COMPACT STRING        #compactTable
    | ALTER TABLE tableIdentifier partitionSpec? CONCATENATE           #concatenateTable
    | ALTER TABLE tableIdentifier partitionSpec?
        CHANGE COLUMN? oldName=identifier colType
        (FIRST | AFTER after=identifier)? (CASCADE | RESTRICT)?        #changeColumn
    | ALTER TABLE tableIdentifier partitionSpec?
        ADD COLUMNS '(' colTypeList ')' (CASCADE | RESTRICT)?          #addColumns
    | ALTER TABLE tableIdentifier partitionSpec?
        REPLACE COLUMNS '(' colTypeList ')' (CASCADE | RESTRICT)?      #replaceColumns
    | CREATE (OR REPLACE)? VIEW qualifiedName AS query                 #createView                  // TODO HIVE
    | DROP VIEW (IF EXISTS)? qualifiedName                             #dropView                    // TODO HIVE
    | EXPLAIN explainOption* statement                                 #explain
    | SHOW TABLES ((FROM | IN) db=identifier)?
        (LIKE (qualifiedName | pattern=STRING))?                       #showTables
    | SHOW COLUMNS (FROM | IN) qualifiedName                           #showColumns                 // TODO HIVE
    | SHOW FUNCTIONS (LIKE? (qualifiedName | pattern=STRING))?         #showFunctions
    | (DESC | DESCRIBE) FUNCTION EXTENDED? qualifiedName               #describeFunction
    | (DESC | DESCRIBE) option=(EXTENDED | FORMATTED)?
        tableIdentifier partitionSpec? describeColName?                #describeTable
    | START TRANSACTION (transactionMode (',' transactionMode)*)?      #startTransaction            // TODO HIVE
    | COMMIT WORK?                                                     #commit                      // TODO HIVE
    | ROLLBACK WORK?                                                   #rollback                    // TODO HIVE
    | SHOW PARTITIONS tableIdentifier partitionSpec?                   #showPartitions              // TODO HIVE
    | REFRESH TABLE tableIdentifier                                    #refreshTable
    | CACHE LAZY? TABLE identifier (AS? query)?                        #cacheTable
    | UNCACHE TABLE identifier                                         #uncacheTable
    | CLEAR CACHE                                                      #clearCache
    | SET .*?                                                          #setConfiguration
    ;

createTable
    : CREATE TEMPORARY? TABLE (IF NOT EXISTS)? tableIdentifier
    ;

query
    : ctes? queryNoWith
    ;

insertInto
    : INSERT OVERWRITE TABLE tableIdentifier partitionSpec? (IF NOT EXISTS)?
    | INSERT INTO TABLE? tableIdentifier partitionSpec?
    ;

partitionSpecLocation
    : partitionSpec (LOCATION STRING)?
    ;

partitionSpec
    : PARTITION '(' partitionVal (',' partitionVal)* ')'
    ;

partitionVal
    : identifier (EQ constant)?
    ;

describeColName
    : identifier ('.' (identifier | STRING))*
    ;

ctes
    : WITH namedQuery (',' namedQuery)*
    ;

namedQuery
    : name=identifier AS? '(' queryNoWith ')'
    ;

tableProvider
    : USING qualifiedName
    ;

tableProperties
    :(OPTIONS | WITH) tablePropertyList
    ;

tablePropertyList
    : '(' tableProperty (',' tableProperty)* ')'
    ;

tableProperty
    : key=tablePropertyKey (EQ? value=STRING)?
    ;

tablePropertyKey
    : looseIdentifier ('.' looseIdentifier)*
    | STRING
    ;

constantList
    : '(' constant (',' constant)* ')'
    ;

nestedConstantList
    : '(' constantList (',' constantList)* ')'
    ;

skewedLocation
    : (constant | constantList) EQ STRING
    ;

skewedLocationList
    : '(' skewedLocation (',' skewedLocation)* ')'
    ;

fileFormat
    : INPUTFORMAT inFmt=STRING OUTPUTFORMAT outFmt=STRING SERDE serdeCls=STRING
      (INPUTDRIVER inDriver=STRING OUTPUTDRIVER outDriver=STRING)?                         #tableFileFormat
    | identifier                                                                           #genericFileFormat
    ;

resource
    : identifier STRING
    ;

queryNoWith
    : insertInto? queryTerm queryOrganization                                              #singleInsertQuery
    | fromClause multiInsertQueryBody+                                                     #multiInsertQuery
    ;

queryOrganization
    : (ORDER BY order+=sortItem (',' order+=sortItem)*)?
      (CLUSTER BY clusterBy+=expression (',' clusterBy+=expression)*)?
      (DISTRIBUTE BY distributeBy+=expression (',' distributeBy+=expression)*)?
      (SORT BY sort+=sortItem (',' sort+=sortItem)*)?
      windows?
      (LIMIT limit=expression)?
    ;

multiInsertQueryBody
    : insertInto?
      querySpecification
      queryOrganization
    ;

queryTerm
    : queryPrimary                                                                         #queryTermDefault
    | left=queryTerm operator=(INTERSECT | UNION | EXCEPT) setQuantifier? right=queryTerm  #setOperation
    ;

queryPrimary
    : querySpecification                                                    #queryPrimaryDefault
    | TABLE tableIdentifier                                                 #table
    | inlineTable                                                           #inlineTableDefault1
    | '(' queryNoWith  ')'                                                  #subquery
    ;

sortItem
    : expression ordering=(ASC | DESC)?
    ;

querySpecification
    : (((SELECT kind=TRANSFORM | kind=MAP | kind=REDUCE)) '(' namedExpression (',' namedExpression)* ')'
       inRowFormat=rowFormat?
       USING script=STRING
       (AS (identifierSeq | colTypeList | ('(' (identifierSeq | colTypeList) ')')))?
       outRowFormat=rowFormat?
       (RECORDREADER outRecordReader=STRING)?
       fromClause?
       (WHERE where=booleanExpression)?)
    | (kind=SELECT setQuantifier? namedExpression (',' namedExpression)*
       fromClause?
       lateralView*
       (WHERE where=booleanExpression)?
       aggregation?
       (HAVING having=booleanExpression)?
       windows?)
    ;

fromClause
    : FROM relation (',' relation)* lateralView*
    ;

aggregation
    : GROUP BY groupingExpressions+=expression (',' groupingExpressions+=expression)* (
      WITH kind=ROLLUP
    | WITH kind=CUBE
    | kind=GROUPING SETS '(' groupingSet (',' groupingSet)* ')')?
    ;

groupingSet
    : '(' (expression (',' expression)*)? ')'
    | expression
    ;

lateralView
    : LATERAL VIEW (OUTER)? qualifiedName '(' (expression (',' expression)*)? ')' tblName=identifier (AS? colName+=identifier (',' colName+=identifier)*)
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

relation
    : left=relation
      ( CROSS JOIN right=sampledRelation
      | joinType JOIN rightRelation=relation joinCriteria?
      | NATURAL joinType JOIN right=sampledRelation
      )                                           #joinRelation
    | sampledRelation                             #relationDefault
    ;

joinType
    : INNER?
    | LEFT OUTER?
    | LEFT SEMI
    | RIGHT OUTER?
    | FULL OUTER?
    ;

joinCriteria
    : ON booleanExpression
    | USING '(' identifier (',' identifier)* ')'
    ;

sampledRelation
    : relationPrimary (
        TABLESAMPLE '('
         ( (percentage=(INTEGER_VALUE | DECIMAL_VALUE) sampleType=PERCENTLIT)
         | (expression sampleType=ROWS)
         | (sampleType=BUCKET numerator=INTEGER_VALUE OUT OF denominator=INTEGER_VALUE (ON identifier)?))
         ')'
      )?
    ;

identifierList
    : '(' identifierSeq ')'
    ;

identifierSeq
    : identifier (',' identifier)*
    ;

orderedIdentifierList
    : '(' orderedIdentifier (',' orderedIdentifier)* ')'
    ;

orderedIdentifier
    : identifier ordering=(ASC | DESC)?
    ;

relationPrimary
    : tableIdentifier (AS? identifier)?                             #tableName
    | '(' queryNoWith ')' (AS? identifier)?                         #aliasedQuery
    | '(' relation ')'  (AS? identifier)?                           #aliasedRelation
    | inlineTable                                                   #inlineTableDefault2
    ;

inlineTable
    : VALUES expression (',' expression)*  (AS? identifier identifierList?)?
    ;

rowFormat
    : rowFormatSerde
    | rowFormatDelimited
    ;

rowFormatSerde
    : ROW FORMAT SERDE name=STRING (WITH SERDEPROPERTIES props=tableProperties)?
    ;

rowFormatDelimited
    : ROW FORMAT DELIMITED
      (FIELDS TERMINATED BY fieldsTerminatedBy=STRING)?
      (COLLECTION ITEMS TERMINATED BY collectionItemsTerminatedBy=STRING)?
      (MAP KEYS TERMINATED BY keysTerminatedBy=STRING)?
      (ESCAPED BY escapedBy=STRING)?
      (LINES SEPARATED BY linesSeparatedBy=STRING)?
    ;

tableIdentifier
    : (db=identifier '.')? table=identifier
    ;

namedExpression
    : expression (AS? (identifier | identifierList))?
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : predicated                                                   #booleanDefault
    | NOT booleanExpression                                        #logicalNot
    | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
    | EXISTS '(' query ')'                                         #exists
    ;

// workaround for:
//  https://github.com/antlr/antlr4/issues/780
//  https://github.com/antlr/antlr4/issues/781
predicated
    : valueExpression predicate[$valueExpression.ctx]?
    ;

predicate[ParserRuleContext value]
    : comparisonOperator right=valueExpression                            #comparison
    | NOT? BETWEEN lower=valueExpression AND upper=valueExpression        #between
    | NOT? IN '(' expression (',' expression)* ')'                        #inList
    | NOT? IN '(' query ')'                                               #inSubquery
    | NOT? like=(RLIKE | LIKE) pattern=valueExpression                    #like
    | IS NOT? NULL                                                        #nullPredicate
    ;

valueExpression
    : primaryExpression                                                                      #valueExpressionDefault
    | operator=(MINUS | PLUS | TILDE) valueExpression                                        #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT | DIV) right=valueExpression #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression                     #arithmeticBinary
    | left=valueExpression operator=AMPERSAND right=valueExpression                          #arithmeticBinary
    | left=valueExpression operator=HAT right=valueExpression                                #arithmeticBinary
    | left=valueExpression operator=PIPE right=valueExpression                               #arithmeticBinary
    ;

primaryExpression
    : constant                                                                                 #constantDefault
    | ASTERISK                                                                                 #star
    | qualifiedName '.' ASTERISK                                                               #star
    | '(' expression (',' expression)+ ')'                                                     #rowConstructor
    | qualifiedName '(' (ASTERISK) ')' (OVER windowSpec)?                                      #functionCall
    | qualifiedName '(' (setQuantifier? expression (',' expression)*)? ')' (OVER windowSpec)?  #functionCall
    | '(' query ')'                                                                            #subqueryExpression
    | CASE valueExpression whenClause+ (ELSE elseExpression=expression)? END                   #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END                                   #searchedCase
    | CAST '(' expression AS dataType ')'                                                      #cast
    | value=primaryExpression '[' index=valueExpression ']'                                    #subscript
    | identifier                                                                               #columnReference
    | base=primaryExpression '.' fieldName=identifier                                          #dereference
    | '(' expression ')'                                                                       #parenthesizedExpression
    ;

constant
    : NULL                                                                                     #nullLiteral
    | interval                                                                                 #intervalLiteral
    | identifier STRING                                                                        #typeConstructor
    | number                                                                                   #numericLiteral
    | booleanValue                                                                             #booleanLiteral
    | STRING+                                                                                  #stringLiteral
    ;

comparisonOperator
    : EQ | NEQ | NEQJ | LT | LTE | GT | GTE | NSEQ
    ;

booleanValue
    : TRUE | FALSE
    ;

interval
    : INTERVAL intervalField*
    ;

intervalField
    : value=intervalValue unit=identifier (TO to=identifier)?
    ;

intervalValue
    : (PLUS | MINUS)? (INTEGER_VALUE | DECIMAL_VALUE)
    | STRING
    ;

dataType
    : complex=ARRAY '<' dataType '>'                            #complexDataType
    | complex=MAP '<' dataType ',' dataType '>'                 #complexDataType
    | complex=STRUCT ('<' colTypeList? '>' | NEQ)               #complexDataType
    | identifier ('(' INTEGER_VALUE (',' INTEGER_VALUE)* ')')?  #primitiveDataType
    ;

colTypeList
    : colType (',' colType)*
    ;

colType
    : identifier ':'? dataType (COMMENT STRING)?
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

windows
    : WINDOW namedWindow (',' namedWindow)*
    ;

namedWindow
    : identifier AS windowSpec
    ;

windowSpec
    : name=identifier  #windowRef
    | '('
      (PARTITION BY partition+=expression (',' partition+=expression)*)?
      (ORDER BY sortItem (',' sortItem)* windowFrame?)?
      ')'              #windowDef
    ;

windowFrame
    : frameType=RANGE start=frameBound
    | frameType=ROWS start=frameBound
    | frameType=RANGE BETWEEN start=frameBound AND end=frameBound
    | frameType=ROWS BETWEEN start=frameBound AND end=frameBound
    ;

frameBound
    : UNBOUNDED boundType=(PRECEDING | FOLLOWING)
    | boundType=CURRENT ROW
    | expression boundType=(PRECEDING | FOLLOWING)
    ;


explainOption
    : LOGICAL | FORMATTED | EXTENDED
    ;

transactionMode
    : ISOLATION LEVEL levelOfIsolation    #isolationLevel
    | READ accessMode=(ONLY | WRITE)      #transactionAccessMode
    ;

levelOfIsolation
    : READ UNCOMMITTED                    #readUncommitted
    | READ COMMITTED                      #readCommitted
    | REPEATABLE READ                     #repeatableRead
    | SERIALIZABLE                        #serializable
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

// Identifier that also allows the use of a number of SQL keywords (mainly for backwards compatibility).
looseIdentifier
    : identifier
    | FROM
    | TO
    | TABLE
    | WITH
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | quotedIdentifier       #quotedIdentifierAlternative
    | nonReserved            #unquotedIdentifier
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

number
    : DECIMAL_VALUE            #decimalLiteral
    | SCIENTIFIC_DECIMAL_VALUE #scientificDecimalLiteral
    | INTEGER_VALUE            #integerLiteral
    | BIGINT_LITERAL           #bigIntLiteral
    | SMALLINT_LITERAL         #smallIntLiteral
    | TINYINT_LITERAL          #tinyIntLiteral
    | DOUBLE_LITERAL           #doubleLiteral
    ;

nonReserved
    : SHOW | TABLES | COLUMNS | COLUMN | PARTITIONS | FUNCTIONS
    | ADD
    | OVER | PARTITION | RANGE | ROWS | PRECEDING | FOLLOWING | CURRENT | ROW | MAP | ARRAY | STRUCT
    | LATERAL | WINDOW | REDUCE | TRANSFORM | USING | SERDE | SERDEPROPERTIES | RECORDREADER
    | DELIMITED | FIELDS | TERMINATED | COLLECTION | ITEMS | KEYS | ESCAPED | LINES | SEPARATED
    | EXTENDED | REFRESH | CLEAR | CACHE | UNCACHE | LAZY | TEMPORARY | OPTIONS
    | GROUPING | CUBE | ROLLUP
    | EXPLAIN | FORMAT | LOGICAL | FORMATTED
    | TABLESAMPLE | USE | TO | BUCKET | PERCENTLIT | OUT | OF
    | SET
    | VIEW | REPLACE
    | IF
    | NO | DATA
    | START | TRANSACTION | COMMIT | ROLLBACK | WORK | ISOLATION | LEVEL
    | SERIALIZABLE | REPEATABLE | COMMITTED | UNCOMMITTED | READ | WRITE | ONLY
    | SORT | CLUSTER | DISTRIBUTE UNSET | TBLPROPERTIES | SKEWED | STORED | DIRECTORIES | LOCATION
    | EXCHANGE | ARCHIVE | UNARCHIVE | FILEFORMAT | TOUCH | COMPACT | CONCATENATE | CHANGE | FIRST
    | AFTER | CASCADE | RESTRICT | BUCKETS | CLUSTERED | SORTED | PURGE | INPUTFORMAT | OUTPUTFORMAT
    | INPUTDRIVER | OUTPUTDRIVER | DBPROPERTIES
    ;

SELECT: 'SELECT';
FROM: 'FROM';
ADD: 'ADD';
AS: 'AS';
ALL: 'ALL';
DISTINCT: 'DISTINCT';
WHERE: 'WHERE';
GROUP: 'GROUP';
BY: 'BY';
GROUPING: 'GROUPING';
SETS: 'SETS';
CUBE: 'CUBE';
ROLLUP: 'ROLLUP';
ORDER: 'ORDER';
HAVING: 'HAVING';
LIMIT: 'LIMIT';
AT: 'AT';
OR: 'OR';
AND: 'AND';
IN: 'IN';
NOT: 'NOT' | '!';
NO: 'NO';
EXISTS: 'EXISTS';
BETWEEN: 'BETWEEN';
LIKE: 'LIKE';
RLIKE: 'RLIKE' | 'REGEXP';
IS: 'IS';
NULL: 'NULL';
TRUE: 'TRUE';
FALSE: 'FALSE';
NULLS: 'NULLS';
ASC: 'ASC';
DESC: 'DESC';
FOR: 'FOR';
INTERVAL: 'INTERVAL';
CASE: 'CASE';
WHEN: 'WHEN';
THEN: 'THEN';
ELSE: 'ELSE';
END: 'END';
JOIN: 'JOIN';
CROSS: 'CROSS';
OUTER: 'OUTER';
INNER: 'INNER';
LEFT: 'LEFT';
SEMI: 'SEMI';
RIGHT: 'RIGHT';
FULL: 'FULL';
NATURAL: 'NATURAL';
ON: 'ON';
LATERAL: 'LATERAL';
WINDOW: 'WINDOW';
OVER: 'OVER';
PARTITION: 'PARTITION';
RANGE: 'RANGE';
ROWS: 'ROWS';
UNBOUNDED: 'UNBOUNDED';
PRECEDING: 'PRECEDING';
FOLLOWING: 'FOLLOWING';
CURRENT: 'CURRENT';
ROW: 'ROW';
WITH: 'WITH';
VALUES: 'VALUES';
CREATE: 'CREATE';
TABLE: 'TABLE';
VIEW: 'VIEW';
REPLACE: 'REPLACE';
INSERT: 'INSERT';
DELETE: 'DELETE';
INTO: 'INTO';
DESCRIBE: 'DESCRIBE';
EXPLAIN: 'EXPLAIN';
FORMAT: 'FORMAT';
LOGICAL: 'LOGICAL';
CAST: 'CAST';
SHOW: 'SHOW';
TABLES: 'TABLES';
COLUMNS: 'COLUMNS';
COLUMN: 'COLUMN';
USE: 'USE';
PARTITIONS: 'PARTITIONS';
FUNCTIONS: 'FUNCTIONS';
DROP: 'DROP';
UNION: 'UNION';
EXCEPT: 'EXCEPT';
INTERSECT: 'INTERSECT';
TO: 'TO';
TABLESAMPLE: 'TABLESAMPLE';
STRATIFY: 'STRATIFY';
ALTER: 'ALTER';
RENAME: 'RENAME';
ARRAY: 'ARRAY';
MAP: 'MAP';
STRUCT: 'STRUCT';
COMMENT: 'COMMENT';
SET: 'SET';
DATA: 'DATA';
START: 'START';
TRANSACTION: 'TRANSACTION';
COMMIT: 'COMMIT';
ROLLBACK: 'ROLLBACK';
WORK: 'WORK';
ISOLATION: 'ISOLATION';
LEVEL: 'LEVEL';
SERIALIZABLE: 'SERIALIZABLE';
REPEATABLE: 'REPEATABLE';
COMMITTED: 'COMMITTED';
UNCOMMITTED: 'UNCOMMITTED';
READ: 'READ';
WRITE: 'WRITE';
ONLY: 'ONLY';

IF: 'IF';

EQ  : '=' | '==';
NSEQ: '<=>';
NEQ : '<>';
NEQJ: '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
DIV: 'DIV';
TILDE: '~';
AMPERSAND: '&';
PIPE: '|';
HAT: '^';

PERCENTLIT: 'PERCENT';
BUCKET: 'BUCKET';
OUT: 'OUT';
OF: 'OF';

SORT: 'SORT';
CLUSTER: 'CLUSTER';
DISTRIBUTE: 'DISTRIBUTE';
OVERWRITE: 'OVERWRITE';
TRANSFORM: 'TRANSFORM';
REDUCE: 'REDUCE';
USING: 'USING';
SERDE: 'SERDE';
SERDEPROPERTIES: 'SERDEPROPERTIES';
RECORDREADER: 'RECORDREADER';
DELIMITED: 'DELIMITED';
FIELDS: 'FIELDS';
TERMINATED: 'TERMINATED';
COLLECTION: 'COLLECTION';
ITEMS: 'ITEMS';
KEYS: 'KEYS';
ESCAPED: 'ESCAPED';
LINES: 'LINES';
SEPARATED: 'SEPARATED';
FUNCTION: 'FUNCTION';
EXTENDED: 'EXTENDED';
REFRESH: 'REFRESH';
CLEAR: 'CLEAR';
CACHE: 'CACHE';
UNCACHE: 'UNCACHE';
LAZY: 'LAZY';
FORMATTED: 'FORMATTED';
TEMPORARY: 'TEMPORARY' | 'TEMP';
OPTIONS: 'OPTIONS';
UNSET: 'UNSET';
TBLPROPERTIES: 'TBLPROPERTIES';
DBPROPERTIES: 'DBPROPERTIES';
BUCKETS: 'BUCKETS';
SKEWED: 'SKEWED';
STORED: 'STORED';
DIRECTORIES: 'DIRECTORIES';
LOCATION: 'LOCATION';
EXCHANGE: 'EXCHANGE';
ARCHIVE: 'ARCHIVE';
UNARCHIVE: 'UNARCHIVE';
FILEFORMAT: 'FILEFORMAT';
TOUCH: 'TOUCH';
COMPACT: 'COMPACT';
CONCATENATE: 'CONCATENATE';
CHANGE: 'CHANGE';
FIRST: 'FIRST';
AFTER: 'AFTER';
CASCADE: 'CASCADE';
RESTRICT: 'RESTRICT';
CLUSTERED: 'CLUSTERED';
SORTED: 'SORTED';
PURGE: 'PURGE';
INPUTFORMAT: 'INPUTFORMAT';
OUTPUTFORMAT: 'OUTPUTFORMAT';
INPUTDRIVER: 'INPUTDRIVER';
OUTPUTDRIVER: 'OUTPUTDRIVER';
DATABASE: 'DATABASE';

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '\"' ( ~('\"'|'\\') | ('\\' .) )* '\"'
    ;

BIGINT_LITERAL
    : DIGIT+ 'L'
    ;

SMALLINT_LITERAL
    : DIGIT+ 'S'
    ;

TINYINT_LITERAL
    : DIGIT+ 'Y'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

SCIENTIFIC_DECIMAL_VALUE
    : DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

DOUBLE_LITERAL
    :
    (INTEGER_VALUE | DECIMAL_VALUE | SCIENTIFIC_DECIMAL_VALUE) 'D'
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
