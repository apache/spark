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

statement
    : query                                                            #statementDefault
    | USE db=identifier                                                #use
    | createTable ('(' colTypeList ')')? tableProvider tableProperties #createTableUsing
    | createTable tableProvider tableProperties? AS? query             #createTableUsingAsSelect
    | DROP TABLE (IF EXISTS)? qualifiedName                            #dropTable
    | DELETE FROM qualifiedName (WHERE booleanExpression)?             #delete
    | ALTER TABLE from=qualifiedName RENAME TO to=qualifiedName        #renameTable
    | ALTER TABLE tableName=qualifiedName
        RENAME COLUMN from=identifier TO to=identifier                 #renameColumn
    | ALTER TABLE tableName=qualifiedName
        ADD COLUMN column=colType                                      #addColumn
    | CREATE (OR REPLACE)? VIEW qualifiedName AS query                 #createView
    | DROP VIEW (IF EXISTS)? qualifiedName                             #dropView
    | CALL qualifiedName '(' (callArgument (',' callArgument)*)? ')'   #call
    | EXPLAIN explainOption* statement                                 #explain
    | SHOW TABLES ((FROM | IN) db=identifier)?
        (LIKE (qualifiedName | pattern=STRING))?                       #showTables
    | SHOW SCHEMAS ((FROM | IN) identifier)?                           #showSchemas
    | SHOW CATALOGS                                                    #showCatalogs
    | SHOW COLUMNS (FROM | IN) qualifiedName                           #showColumns
    | SHOW FUNCTIONS (LIKE? (qualifiedName | pattern=STRING))?         #showFunctions
    | (DESC | DESCRIBE) FUNCTION EXTENDED? qualifiedName               #describeFunction
    | (DESC | DESCRIBE) option=(EXTENDED | FORMATTED)?
        tableIdentifier partitionSpec? describeColName?                #describeTable
    | SHOW SESSION                                                     #showSession
    | SET SESSION qualifiedName EQ expression                          #setSession
    | RESET SESSION qualifiedName                                      #resetSession
    | START TRANSACTION (transactionMode (',' transactionMode)*)?      #startTransaction
    | COMMIT WORK?                                                     #commit
    | ROLLBACK WORK?                                                   #rollback
    | SHOW PARTITIONS (FROM | IN) qualifiedName
        (WHERE booleanExpression)?
        (ORDER BY sortItem (',' sortItem)*)?
        (LIMIT limit=(INTEGER_VALUE | ALL))?                           #showPartitions
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
    :(OPTIONS | WITH) '(' tableProperty (',' tableProperty)* ')'
    ;

tableProperty
    : key=tablePropertyKey (EQ? value=STRING)?
    ;

tablePropertyKey
    : tablePropertyKeyElement ('.' tablePropertyKeyElement)*
    | STRING
    ;

tablePropertyKeyElement
    : (identifier | FROM | TO)
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
    : ((SELECT kind=(TRANSFORM | MAP | REDUCE)) '(' selectItem (',' selectItem)* ')'
       inRowFormat=rowFormat?
       USING script=STRING
       (AS (columnAliasList | colTypeList | ('(' (columnAliasList | colTypeList) ')')))?
       outRowFormat=rowFormat?
       (RECORDREADER outRecordReader=STRING)?
       fromClause?
       (WHERE where=booleanExpression)?)
    | (kind=SELECT setQuantifier? selectItem (',' selectItem)*
       fromClause?
       lateralView*
       (WHERE where=booleanExpression)?
       (aggregation (HAVING having=booleanExpression)?)?
       windows?)
    ;

fromClause
    : FROM relation (',' relation)*
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

selectItem
    : namedExpression               #selectSingle
    | qualifiedName '.' ASTERISK    #selectAll
    | ASTERISK                      #selectAll
    ;

relation
    : left=relation
      ( CROSS JOIN right=sampledRelation
      | joinType JOIN rightRelation=relation (ON booleanExpression)?
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

sampledRelation
    : relationPrimary (
        TABLESAMPLE '('
         ( (percentage=(INTEGER_VALUE | DECIMAL_VALUE) sampleType=PERCENTLIT)
         | (expression sampleType=ROWS)
         | (sampleType=BUCKET numerator=INTEGER_VALUE OUT OF denominator=INTEGER_VALUE (ON identifier)?))
         ')'
      )?
    ;

columnAliases
    : '(' columnAliasList ')'
    ;

columnAliasList
    : identifier (',' identifier)*
    ;

relationPrimary
    : tableIdentifier (AS? identifier)?                             #tableName
    | '(' queryNoWith ')' (AS? identifier)?                         #aliasedQuery
    | '(' relation ')'  (AS? identifier)?                           #aliasedRelation
    | inlineTable                                                   #inlineTableDefault2
    ;

inlineTable
    : VALUES expression (',' expression)*  (AS? identifier columnAliases?)?
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
    : expression (AS? (identifier | columnAliases))?
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
    | '(' expression (',' expression)+ ')'                                                     #rowConstructor
    | qualifiedName '(' (ASTERISK) ')' (OVER windowSpec)?                                      #functionCall
    | qualifiedName '(' (setQuantifier? expression (',' expression)*)? ')' (OVER windowSpec)?  #functionCall
    | '(' query ')'                                                                            #subqueryExpression
    | CASE valueExpression whenClause+ (ELSE elseExpression=expression)? END                   #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END                                   #searchedCase
    | CAST '(' expression AS dataType ')'                                                      #cast
    | ARRAY '[' (expression (',' expression)*)? ']'                                            #arrayConstructor
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
    : EQ | NEQ | LT | LTE | GT | GTE | NSEQ
    ;

booleanValue
    : TRUE | FALSE
    ;

interval
    : INTERVAL intervalField+
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
    | complex=STRUCT '<' colTypeList '>'                        #complexDataType
    | identifier ('(' INTEGER_VALUE (',' typeParameter)* ')')?  #primitiveDatatype
    ;

colTypeList
    : colType (',' colType)*
    ;

colType
    : identifier ':'? dataType (COMMENT STRING)?
    ;

typeParameter
    : INTEGER_VALUE | dataType
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
      (ORDER BY sortItem (',' sortItem)*)?
      windowFrame?
      ')'              #windowDef
    ;

windowFrame
    : frameType=RANGE start=frameBound
    | frameType=ROWS start=frameBound
    | frameType=RANGE BETWEEN start=frameBound AND end=frameBound
    | frameType=ROWS BETWEEN start=frameBound AND end=frameBound
    ;

frameBound
    : UNBOUNDED boundType=PRECEDING
    | UNBOUNDED boundType=FOLLOWING
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

callArgument
    : expression                    #positionalArgument
    | identifier '=>' expression    #namedArgument
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | quotedIdentifier       #quotedIdentifierAlternative
    | nonReserved            #unquotedIdentifier
    | DIGIT_IDENTIFIER       #digitIdentifier
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
    : SHOW | TABLES | COLUMNS | COLUMN | PARTITIONS | FUNCTIONS | SCHEMAS | CATALOGS | SESSION
    | ADD
    | OVER | PARTITION | RANGE | ROWS | PRECEDING | FOLLOWING | CURRENT | ROW | MAP | ARRAY
    | LATERAL | WINDOW | REDUCE | TRANSFORM | USING | SERDE | SERDEPROPERTIES | RECORDREADER
    | DELIMITED | FIELDS | TERMINATED | COLLECTION | ITEMS | KEYS | ESCAPED | LINES | SEPARATED
    | EXTENDED | REFRESH | CLEAR | CACHE | UNCACHE | LAZY | TEMPORARY | OPTIONS
    | INTERVAL
    | GROUPING | CUBE | ROLLUP
    | EXPLAIN | FORMAT | LOGICAL | FORMATTED
    | TABLESAMPLE | USE | TO | BUCKET | PERCENTLIT | OUT | OF
    | SET | RESET
    | VIEW | REPLACE
    | IF
    | NO | DATA
    | START | TRANSACTION | COMMIT | ROLLBACK | WORK | ISOLATION | LEVEL
    | SERIALIZABLE | REPEATABLE | COMMITTED | UNCOMMITTED | READ | WRITE | ONLY
    | CALL
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
SCHEMAS: 'SCHEMAS';
CATALOGS: 'CATALOGS';
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
RESET: 'RESET';
SESSION: 'SESSION';
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
CALL: 'CALL';

IF: 'IF';

EQ  : '=' | '==';
NSEQ: '<=>';
NEQ : '<>' | '!=';
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
    : (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

DIGIT_IDENTIFIER
    : DIGIT (LETTER | DIGIT | '_')+
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
