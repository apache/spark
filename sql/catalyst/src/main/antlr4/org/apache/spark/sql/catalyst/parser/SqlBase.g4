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

@members {
  /**
   * When false, INTERSECT is given the greater precedence over the other set
   * operations (UNION, EXCEPT and MINUS) as per the SQL standard.
   */
  public boolean legacy_setops_precedence_enbled = false;

  /**
   * Verify whether current token is a valid decimal token (which contains dot).
   * Returns true if the character that follows the token is not a digit or letter or underscore.
   *
   * For example:
   * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
   * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
   * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
   * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is followed
   * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
   * which is not a digit or letter or underscore.
   */
  public boolean isValidDecimal() {
    int nextChar = _input.LA(1);
    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
      nextChar == '_') {
      return false;
    } else {
      return true;
    }
  }
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

singleFunctionIdentifier
    : functionIdentifier EOF
    ;

singleDataType
    : dataType EOF
    ;

singleTableSchema
    : colTypeList EOF
    ;

statement
    : query                                                            #statementDefault
    | USE db=identifier                                                #use
    | CREATE DATABASE (IF NOT EXISTS)? identifier
        (COMMENT comment=STRING)? locationSpec?
        (WITH DBPROPERTIES tablePropertyList)?                         #createDatabase
    | ALTER DATABASE identifier SET DBPROPERTIES tablePropertyList     #setDatabaseProperties
    | DROP DATABASE (IF EXISTS)? identifier (RESTRICT | CASCADE)?      #dropDatabase
    | createTableHeader ('(' colTypeList ')')? tableProvider
        ((OPTIONS options=tablePropertyList) |
        (PARTITIONED BY partitionColumnNames=identifierList) |
        bucketSpec |
        locationSpec |
        (COMMENT comment=STRING) |
        (TBLPROPERTIES tableProps=tablePropertyList))*
        (AS? query)?                                                   #createTable
    | createTableHeader ('(' columns=colTypeList ')')?
        ((COMMENT comment=STRING) |
        (PARTITIONED BY '(' partitionColumns=colTypeList ')') |
        bucketSpec |
        skewSpec |
        rowFormat |
        createFileFormat |
        locationSpec |
        (TBLPROPERTIES tableProps=tablePropertyList))*
        (AS? query)?                                                   #createHiveTable
    | CREATE TABLE (IF NOT EXISTS)? target=tableIdentifier
        LIKE source=tableIdentifier locationSpec?                      #createTableLike
    | ANALYZE TABLE tableIdentifier partitionSpec? COMPUTE STATISTICS
        (identifier | FOR COLUMNS identifierSeq)?                      #analyze
    | ALTER TABLE tableIdentifier
        ADD COLUMNS '(' columns=colTypeList ')'                        #addTableColumns
    | ALTER (TABLE | VIEW) from=tableIdentifier
        RENAME TO to=tableIdentifier                                   #renameTable
    | ALTER (TABLE | VIEW) tableIdentifier
        SET TBLPROPERTIES tablePropertyList                            #setTableProperties
    | ALTER (TABLE | VIEW) tableIdentifier
        UNSET TBLPROPERTIES (IF EXISTS)? tablePropertyList             #unsetTableProperties
    | ALTER TABLE tableIdentifier partitionSpec?
        CHANGE COLUMN? identifier colType colPosition?                 #changeColumn
    | ALTER TABLE tableIdentifier (partitionSpec)?
        SET SERDE STRING (WITH SERDEPROPERTIES tablePropertyList)?     #setTableSerDe
    | ALTER TABLE tableIdentifier (partitionSpec)?
        SET SERDEPROPERTIES tablePropertyList                          #setTableSerDe
    | ALTER TABLE tableIdentifier ADD (IF NOT EXISTS)?
        partitionSpecLocation+                                         #addTablePartition
    | ALTER VIEW tableIdentifier ADD (IF NOT EXISTS)?
        partitionSpec+                                                 #addTablePartition
    | ALTER TABLE tableIdentifier
        from=partitionSpec RENAME TO to=partitionSpec                  #renameTablePartition
    | ALTER TABLE tableIdentifier
        DROP (IF EXISTS)? partitionSpec (',' partitionSpec)* PURGE?    #dropTablePartitions
    | ALTER VIEW tableIdentifier
        DROP (IF EXISTS)? partitionSpec (',' partitionSpec)*           #dropTablePartitions
    | ALTER TABLE tableIdentifier partitionSpec? SET locationSpec      #setTableLocation
    | ALTER TABLE tableIdentifier RECOVER PARTITIONS                   #recoverPartitions
    | DROP TABLE (IF EXISTS)? tableIdentifier PURGE?                   #dropTable
    | DROP VIEW (IF EXISTS)? tableIdentifier                           #dropTable
    | CREATE (OR REPLACE)? (GLOBAL? TEMPORARY)?
        VIEW (IF NOT EXISTS)? tableIdentifier
        identifierCommentList? (COMMENT STRING)?
        (PARTITIONED ON identifierList)?
        (TBLPROPERTIES tablePropertyList)? AS query                    #createView
    | CREATE (OR REPLACE)? GLOBAL? TEMPORARY VIEW
        tableIdentifier ('(' colTypeList ')')? tableProvider
        (OPTIONS tablePropertyList)?                                   #createTempViewUsing
    | ALTER VIEW tableIdentifier AS? query                             #alterViewQuery
    | CREATE (OR REPLACE)? TEMPORARY? FUNCTION (IF NOT EXISTS)?
        qualifiedName AS className=STRING
        (USING resource (',' resource)*)?                              #createFunction
    | DROP TEMPORARY? FUNCTION (IF EXISTS)? qualifiedName              #dropFunction
    | EXPLAIN (LOGICAL | FORMATTED | EXTENDED | CODEGEN | COST)?
        statement                                                      #explain
    | SHOW TABLES ((FROM | IN) db=identifier)?
        (LIKE? pattern=STRING)?                                        #showTables
    | SHOW TABLE EXTENDED ((FROM | IN) db=identifier)?
        LIKE pattern=STRING partitionSpec?                             #showTable
    | SHOW DATABASES (LIKE? pattern=STRING)?                           #showDatabases
    | SHOW TBLPROPERTIES table=tableIdentifier
        ('(' key=tablePropertyKey ')')?                                #showTblProperties
    | SHOW COLUMNS (FROM | IN) tableIdentifier
        ((FROM | IN) db=identifier)?                                   #showColumns
    | SHOW PARTITIONS tableIdentifier partitionSpec?                   #showPartitions
    | SHOW identifier? FUNCTIONS
        (LIKE? (qualifiedName | pattern=STRING))?                      #showFunctions
    | SHOW CREATE TABLE tableIdentifier                                #showCreateTable
    | (DESC | DESCRIBE) FUNCTION EXTENDED? describeFuncName            #describeFunction
    | (DESC | DESCRIBE) DATABASE EXTENDED? identifier                  #describeDatabase
    | (DESC | DESCRIBE) TABLE? option=(EXTENDED | FORMATTED)?
        tableIdentifier partitionSpec? describeColName?                #describeTable
    | REFRESH TABLE tableIdentifier                                    #refreshTable
    | REFRESH (STRING | .*?)                                           #refreshResource
    | CACHE LAZY? TABLE tableIdentifier (AS? query)?                   #cacheTable
    | UNCACHE TABLE (IF EXISTS)? tableIdentifier                       #uncacheTable
    | CLEAR CACHE                                                      #clearCache
    | LOAD DATA LOCAL? INPATH path=STRING OVERWRITE? INTO TABLE
        tableIdentifier partitionSpec?                                 #loadData
    | TRUNCATE TABLE tableIdentifier partitionSpec?                    #truncateTable
    | MSCK REPAIR TABLE tableIdentifier                                #repairTable
    | op=(ADD | LIST) identifier .*?                                   #manageResource
    | SET ROLE .*?                                                     #failNativeCommand
    | SET .*?                                                          #setConfiguration
    | RESET                                                            #resetConfiguration
    | unsupportedHiveNativeCommands .*?                                #failNativeCommand
    ;

unsupportedHiveNativeCommands
    : kw1=CREATE kw2=ROLE
    | kw1=DROP kw2=ROLE
    | kw1=GRANT kw2=ROLE?
    | kw1=REVOKE kw2=ROLE?
    | kw1=SHOW kw2=GRANT
    | kw1=SHOW kw2=ROLE kw3=GRANT?
    | kw1=SHOW kw2=PRINCIPALS
    | kw1=SHOW kw2=ROLES
    | kw1=SHOW kw2=CURRENT kw3=ROLES
    | kw1=EXPORT kw2=TABLE
    | kw1=IMPORT kw2=TABLE
    | kw1=SHOW kw2=COMPACTIONS
    | kw1=SHOW kw2=CREATE kw3=TABLE
    | kw1=SHOW kw2=TRANSACTIONS
    | kw1=SHOW kw2=INDEXES
    | kw1=SHOW kw2=LOCKS
    | kw1=CREATE kw2=INDEX
    | kw1=DROP kw2=INDEX
    | kw1=ALTER kw2=INDEX
    | kw1=LOCK kw2=TABLE
    | kw1=LOCK kw2=DATABASE
    | kw1=UNLOCK kw2=TABLE
    | kw1=UNLOCK kw2=DATABASE
    | kw1=CREATE kw2=TEMPORARY kw3=MACRO
    | kw1=DROP kw2=TEMPORARY kw3=MACRO
    | kw1=ALTER kw2=TABLE tableIdentifier kw3=NOT kw4=CLUSTERED
    | kw1=ALTER kw2=TABLE tableIdentifier kw3=CLUSTERED kw4=BY
    | kw1=ALTER kw2=TABLE tableIdentifier kw3=NOT kw4=SORTED
    | kw1=ALTER kw2=TABLE tableIdentifier kw3=SKEWED kw4=BY
    | kw1=ALTER kw2=TABLE tableIdentifier kw3=NOT kw4=SKEWED
    | kw1=ALTER kw2=TABLE tableIdentifier kw3=NOT kw4=STORED kw5=AS kw6=DIRECTORIES
    | kw1=ALTER kw2=TABLE tableIdentifier kw3=SET kw4=SKEWED kw5=LOCATION
    | kw1=ALTER kw2=TABLE tableIdentifier kw3=EXCHANGE kw4=PARTITION
    | kw1=ALTER kw2=TABLE tableIdentifier kw3=ARCHIVE kw4=PARTITION
    | kw1=ALTER kw2=TABLE tableIdentifier kw3=UNARCHIVE kw4=PARTITION
    | kw1=ALTER kw2=TABLE tableIdentifier kw3=TOUCH
    | kw1=ALTER kw2=TABLE tableIdentifier partitionSpec? kw3=COMPACT
    | kw1=ALTER kw2=TABLE tableIdentifier partitionSpec? kw3=CONCATENATE
    | kw1=ALTER kw2=TABLE tableIdentifier partitionSpec? kw3=SET kw4=FILEFORMAT
    | kw1=ALTER kw2=TABLE tableIdentifier partitionSpec? kw3=REPLACE kw4=COLUMNS
    | kw1=START kw2=TRANSACTION
    | kw1=COMMIT
    | kw1=ROLLBACK
    | kw1=DFS
    | kw1=DELETE kw2=FROM
    ;

createTableHeader
    : CREATE TEMPORARY? EXTERNAL? TABLE (IF NOT EXISTS)? tableIdentifier
    ;

bucketSpec
    : CLUSTERED BY identifierList
      (SORTED BY orderedIdentifierList)?
      INTO INTEGER_VALUE BUCKETS
    ;

skewSpec
    : SKEWED BY identifierList
      ON (constantList | nestedConstantList)
      (STORED AS DIRECTORIES)?
    ;

locationSpec
    : LOCATION STRING
    ;

query
    : ctes? queryNoWith
    ;

insertInto
    : INSERT OVERWRITE TABLE tableIdentifier (partitionSpec (IF NOT EXISTS)?)?                              #insertOverwriteTable
    | INSERT INTO TABLE? tableIdentifier partitionSpec?                                                     #insertIntoTable
    | INSERT OVERWRITE LOCAL? DIRECTORY path=STRING rowFormat? createFileFormat?                            #insertOverwriteHiveDir
    | INSERT OVERWRITE LOCAL? DIRECTORY (path=STRING)? tableProvider (OPTIONS options=tablePropertyList)?   #insertOverwriteDir
    ;

partitionSpecLocation
    : partitionSpec locationSpec?
    ;

partitionSpec
    : PARTITION '(' partitionVal (',' partitionVal)* ')'
    ;

partitionVal
    : identifier (EQ constant)?
    ;

describeFuncName
    : qualifiedName
    | STRING
    | comparisonOperator
    | arithmeticOperator
    | predicateOperator
    ;

describeColName
    : nameParts+=identifier ('.' nameParts+=identifier)*
    ;

ctes
    : WITH namedQuery (',' namedQuery)*
    ;

namedQuery
    : name=identifier AS? '(' query ')'
    ;

tableProvider
    : USING qualifiedName
    ;

tablePropertyList
    : '(' tableProperty (',' tableProperty)* ')'
    ;

tableProperty
    : key=tablePropertyKey (EQ? value=tablePropertyValue)?
    ;

tablePropertyKey
    : identifier ('.' identifier)*
    | STRING
    ;

tablePropertyValue
    : INTEGER_VALUE
    | DECIMAL_VALUE
    | booleanValue
    | STRING
    ;

constantList
    : '(' constant (',' constant)* ')'
    ;

nestedConstantList
    : '(' constantList (',' constantList)* ')'
    ;

createFileFormat
    : STORED AS fileFormat
    | STORED BY storageHandler
    ;

fileFormat
    : INPUTFORMAT inFmt=STRING OUTPUTFORMAT outFmt=STRING    #tableFileFormat
    | identifier                                             #genericFileFormat
    ;

storageHandler
    : STRING (WITH SERDEPROPERTIES tablePropertyList)?
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
      (LIMIT (ALL | limit=expression))?
    ;

multiInsertQueryBody
    : insertInto?
      querySpecification
      queryOrganization
    ;

queryTerm
    : queryPrimary                                                                       #queryTermDefault
    | left=queryTerm {legacy_setops_precedence_enbled}?
        operator=(INTERSECT | UNION | EXCEPT | SETMINUS) setQuantifier? right=queryTerm  #setOperation
    | left=queryTerm {!legacy_setops_precedence_enbled}?
        operator=INTERSECT setQuantifier? right=queryTerm                                #setOperation
    | left=queryTerm {!legacy_setops_precedence_enbled}?
        operator=(UNION | EXCEPT | SETMINUS) setQuantifier? right=queryTerm              #setOperation
    ;

queryPrimary
    : querySpecification                                                    #queryPrimaryDefault
    | TABLE tableIdentifier                                                 #table
    | inlineTable                                                           #inlineTableDefault1
    | '(' queryNoWith  ')'                                                  #subquery
    ;

sortItem
    : expression ordering=(ASC | DESC)? (NULLS nullOrder=(LAST | FIRST))?
    ;

querySpecification
    : (((SELECT kind=TRANSFORM '(' namedExpressionSeq ')'
        | kind=MAP namedExpressionSeq
        | kind=REDUCE namedExpressionSeq))
       inRowFormat=rowFormat?
       (RECORDWRITER recordWriter=STRING)?
       USING script=STRING
       (AS (identifierSeq | colTypeList | ('(' (identifierSeq | colTypeList) ')')))?
       outRowFormat=rowFormat?
       (RECORDREADER recordReader=STRING)?
       fromClause?
       (WHERE where=booleanExpression)?)
    | ((kind=SELECT (hints+=hint)* setQuantifier? namedExpressionSeq fromClause?
       | fromClause (kind=SELECT setQuantifier? namedExpressionSeq)?)
       lateralView*
       (WHERE where=booleanExpression)?
       aggregation?
       (HAVING having=booleanExpression)?
       windows?)
    ;

hint
    : '/*+' hintStatements+=hintStatement (','? hintStatements+=hintStatement)* '*/'
    ;

hintStatement
    : hintName=identifier
    | hintName=identifier '(' parameters+=primaryExpression (',' parameters+=primaryExpression)* ')'
    ;

fromClause
    : FROM relation (',' relation)* lateralView* pivotClause?
    ;

aggregation
    : GROUP BY groupingExpressions+=expression (',' groupingExpressions+=expression)* (
      WITH kind=ROLLUP
    | WITH kind=CUBE
    | kind=GROUPING SETS '(' groupingSet (',' groupingSet)* ')')?
    | GROUP BY kind=GROUPING SETS '(' groupingSet (',' groupingSet)* ')'
    ;

groupingSet
    : '(' (expression (',' expression)*)? ')'
    | expression
    ;

pivotClause
    : PIVOT '(' aggregates=namedExpressionSeq FOR pivotColumn IN '(' pivotValues+=pivotValue (',' pivotValues+=pivotValue)* ')' ')'
    ;

pivotColumn
    : identifiers+=identifier
    | '(' identifiers+=identifier (',' identifiers+=identifier)* ')'
    ;

pivotValue
    : expression (AS? identifier)?
    ;

lateralView
    : LATERAL VIEW (OUTER)? qualifiedName '(' (expression (',' expression)*)? ')' tblName=identifier (AS? colName+=identifier (',' colName+=identifier)*)?
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

relation
    : relationPrimary joinRelation*
    ;

joinRelation
    : (joinType) JOIN right=relationPrimary joinCriteria?
    | NATURAL joinType JOIN right=relationPrimary
    ;

joinType
    : INNER?
    | CROSS
    | LEFT OUTER?
    | LEFT SEMI
    | RIGHT OUTER?
    | FULL OUTER?
    | LEFT? ANTI
    ;

joinCriteria
    : ON booleanExpression
    | USING '(' identifier (',' identifier)* ')'
    ;

sample
    : TABLESAMPLE '(' sampleMethod? ')'
    ;

sampleMethod
    : negativeSign=MINUS? percentage=(INTEGER_VALUE | DECIMAL_VALUE) PERCENTLIT   #sampleByPercentile
    | expression ROWS                                                             #sampleByRows
    | sampleType=BUCKET numerator=INTEGER_VALUE OUT OF denominator=INTEGER_VALUE
        (ON (identifier | qualifiedName '(' ')'))?                                #sampleByBucket
    | bytes=expression                                                            #sampleByBytes
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

identifierCommentList
    : '(' identifierComment (',' identifierComment)* ')'
    ;

identifierComment
    : identifier (COMMENT STRING)?
    ;

relationPrimary
    : tableIdentifier sample? tableAlias      #tableName
    | '(' queryNoWith ')' sample? tableAlias  #aliasedQuery
    | '(' relation ')' sample? tableAlias     #aliasedRelation
    | inlineTable                             #inlineTableDefault2
    | functionTable                           #tableValuedFunction
    ;

inlineTable
    : VALUES expression (',' expression)* tableAlias
    ;

functionTable
    : identifier '(' (expression (',' expression)*)? ')' tableAlias
    ;

tableAlias
    : (AS? strictIdentifier identifierList?)?
    ;

rowFormat
    : ROW FORMAT SERDE name=STRING (WITH SERDEPROPERTIES props=tablePropertyList)?  #rowFormatSerde
    | ROW FORMAT DELIMITED
      (FIELDS TERMINATED BY fieldsTerminatedBy=STRING (ESCAPED BY escapedBy=STRING)?)?
      (COLLECTION ITEMS TERMINATED BY collectionItemsTerminatedBy=STRING)?
      (MAP KEYS TERMINATED BY keysTerminatedBy=STRING)?
      (LINES TERMINATED BY linesSeparatedBy=STRING)?
      (NULL DEFINED AS nullDefinedAs=STRING)?                                       #rowFormatDelimited
    ;

tableIdentifier
    : (db=identifier '.')? table=identifier
    ;

functionIdentifier
    : (db=identifier '.')? function=identifier
    ;

namedExpression
    : expression (AS? (identifier | identifierList))?
    ;

namedExpressionSeq
    : namedExpression (',' namedExpression)*
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : NOT booleanExpression                                        #logicalNot
    | EXISTS '(' query ')'                                         #exists
    | valueExpression predicate?                                   #predicated
    | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
    ;

predicate
    : NOT? kind=BETWEEN lower=valueExpression AND upper=valueExpression
    | NOT? kind=IN '(' expression (',' expression)* ')'
    | NOT? kind=IN '(' query ')'
    | NOT? kind=(RLIKE | LIKE) pattern=valueExpression
    | IS NOT? kind=NULL
    | IS NOT? kind=DISTINCT FROM right=valueExpression
    ;

valueExpression
    : primaryExpression                                                                      #valueExpressionDefault
    | operator=(MINUS | PLUS | TILDE) valueExpression                                        #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT | DIV) right=valueExpression #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS | CONCAT_PIPE) right=valueExpression       #arithmeticBinary
    | left=valueExpression operator=AMPERSAND right=valueExpression                          #arithmeticBinary
    | left=valueExpression operator=HAT right=valueExpression                                #arithmeticBinary
    | left=valueExpression operator=PIPE right=valueExpression                               #arithmeticBinary
    | left=valueExpression comparisonOperator right=valueExpression                          #comparison
    ;

primaryExpression
    : CASE whenClause+ (ELSE elseExpression=expression)? END                                   #searchedCase
    | CASE value=expression whenClause+ (ELSE elseExpression=expression)? END                  #simpleCase
    | CAST '(' expression AS dataType ')'                                                      #cast
    | STRUCT '(' (argument+=namedExpression (',' argument+=namedExpression)*)? ')'             #struct
    | FIRST '(' expression (IGNORE NULLS)? ')'                                                 #first
    | LAST '(' expression (IGNORE NULLS)? ')'                                                  #last
    | POSITION '(' substr=valueExpression IN str=valueExpression ')'                           #position
    | constant                                                                                 #constantDefault
    | ASTERISK                                                                                 #star
    | qualifiedName '.' ASTERISK                                                               #star
    | '(' namedExpression (',' namedExpression)+ ')'                                           #rowConstructor
    | '(' query ')'                                                                            #subqueryExpression
    | qualifiedName '(' (setQuantifier? argument+=expression (',' argument+=expression)*)? ')'
       (OVER windowSpec)?                                                                      #functionCall
    | qualifiedName '(' trimOption=(BOTH | LEADING | TRAILING) argument+=expression
      FROM argument+=expression ')'                                                            #functionCall
    | IDENTIFIER '->' expression                                                               #lambda
    | '(' IDENTIFIER (',' IDENTIFIER)+ ')' '->' expression                                     #lambda
    | value=primaryExpression '[' index=valueExpression ']'                                    #subscript
    | identifier                                                                               #columnReference
    | base=primaryExpression '.' fieldName=identifier                                          #dereference
    | '(' expression ')'                                                                       #parenthesizedExpression
    | EXTRACT '(' field=identifier FROM source=valueExpression ')'                             #extract
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

arithmeticOperator
    : PLUS | MINUS | ASTERISK | SLASH | PERCENT | DIV | TILDE | AMPERSAND | PIPE | CONCAT_PIPE | HAT
    ;

predicateOperator
    : OR | AND | IN | NOT
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

colPosition
    : FIRST | AFTER identifier
    ;

dataType
    : complex=ARRAY '<' dataType '>'                            #complexDataType
    | complex=MAP '<' dataType ',' dataType '>'                 #complexDataType
    | complex=STRUCT ('<' complexColTypeList? '>' | NEQ)        #complexDataType
    | identifier ('(' INTEGER_VALUE (',' INTEGER_VALUE)* ')')?  #primitiveDataType
    ;

colTypeList
    : colType (',' colType)*
    ;

colType
    : identifier dataType (COMMENT STRING)?
    ;

complexColTypeList
    : complexColType (',' complexColType)*
    ;

complexColType
    : identifier ':' dataType (COMMENT STRING)?
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
      ( CLUSTER BY partition+=expression (',' partition+=expression)*
      | ((PARTITION | DISTRIBUTE) BY partition+=expression (',' partition+=expression)*)?
        ((ORDER | SORT) BY sortItem (',' sortItem)*)?)
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
    : UNBOUNDED boundType=(PRECEDING | FOLLOWING)
    | boundType=CURRENT ROW
    | expression boundType=(PRECEDING | FOLLOWING)
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

identifier
    : strictIdentifier
    | ANTI | FULL | INNER | LEFT | SEMI | RIGHT | NATURAL | JOIN | CROSS | ON
    | UNION | INTERSECT | EXCEPT | SETMINUS
    ;

strictIdentifier
    : IDENTIFIER             #unquotedIdentifier
    | quotedIdentifier       #quotedIdentifierAlternative
    | nonReserved            #unquotedIdentifier
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

number
    : MINUS? DECIMAL_VALUE            #decimalLiteral
    | MINUS? INTEGER_VALUE            #integerLiteral
    | MINUS? BIGINT_LITERAL           #bigIntLiteral
    | MINUS? SMALLINT_LITERAL         #smallIntLiteral
    | MINUS? TINYINT_LITERAL          #tinyIntLiteral
    | MINUS? DOUBLE_LITERAL           #doubleLiteral
    | MINUS? BIGDECIMAL_LITERAL       #bigDecimalLiteral
    ;

nonReserved
    : SHOW | TABLES | COLUMNS | COLUMN | PARTITIONS | FUNCTIONS | DATABASES
    | ADD
    | OVER | PARTITION | RANGE | ROWS | PRECEDING | FOLLOWING | CURRENT | ROW | LAST | FIRST | AFTER
    | MAP | ARRAY | STRUCT
    | PIVOT | LATERAL | WINDOW | REDUCE | TRANSFORM | SERDE | SERDEPROPERTIES | RECORDREADER
    | DELIMITED | FIELDS | TERMINATED | COLLECTION | ITEMS | KEYS | ESCAPED | LINES | SEPARATED
    | EXTENDED | REFRESH | CLEAR | CACHE | UNCACHE | LAZY | GLOBAL | TEMPORARY | OPTIONS
    | GROUPING | CUBE | ROLLUP
    | EXPLAIN | FORMAT | LOGICAL | FORMATTED | CODEGEN | COST
    | TABLESAMPLE | USE | TO | BUCKET | PERCENTLIT | OUT | OF
    | SET | RESET
    | VIEW | REPLACE
    | IF
    | POSITION
    | EXTRACT
    | NO | DATA
    | START | TRANSACTION | COMMIT | ROLLBACK | IGNORE
    | SORT | CLUSTER | DISTRIBUTE | UNSET | TBLPROPERTIES | SKEWED | STORED | DIRECTORIES | LOCATION
    | EXCHANGE | ARCHIVE | UNARCHIVE | FILEFORMAT | TOUCH | COMPACT | CONCATENATE | CHANGE
    | CASCADE | RESTRICT | BUCKETS | CLUSTERED | SORTED | PURGE | INPUTFORMAT | OUTPUTFORMAT
    | DBPROPERTIES | DFS | TRUNCATE | COMPUTE | LIST
    | STATISTICS | ANALYZE | PARTITIONED | EXTERNAL | DEFINED | RECORDWRITER
    | REVOKE | GRANT | LOCK | UNLOCK | MSCK | REPAIR | RECOVER | EXPORT | IMPORT | LOAD | VALUES | COMMENT | ROLE
    | ROLES | COMPACTIONS | PRINCIPALS | TRANSACTIONS | INDEX | INDEXES | LOCKS | OPTION | LOCAL | INPATH
    | ASC | DESC | LIMIT | RENAME | SETS
    | AT | NULLS | OVERWRITE | ALL | ANY | ALTER | AS | BETWEEN | BY | CREATE | DELETE
    | DESCRIBE | DROP | EXISTS | FALSE | FOR | GROUP | IN | INSERT | INTO | IS |LIKE
    | NULL | ORDER | OUTER | TABLE | TRUE | WITH | RLIKE
    | AND | CASE | CAST | DISTINCT | DIV | ELSE | END | FUNCTION | INTERVAL | MACRO | OR | STRATIFY | THEN
    | UNBOUNDED | WHEN
    | DATABASE | SELECT | FROM | WHERE | HAVING | TO | TABLE | WITH | NOT
    | DIRECTORY
    | BOTH | LEADING | TRAILING
    ;

SELECT: 'SELECT';
FROM: 'FROM';
ADD: 'ADD';
AS: 'AS';
ALL: 'ALL';
ANY: 'ANY';
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
PIVOT: 'PIVOT';
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
FIRST: 'FIRST';
AFTER: 'AFTER';
LAST: 'LAST';
ROW: 'ROW';
WITH: 'WITH';
VALUES: 'VALUES';
CREATE: 'CREATE';
TABLE: 'TABLE';
DIRECTORY: 'DIRECTORY';
VIEW: 'VIEW';
REPLACE: 'REPLACE';
INSERT: 'INSERT';
DELETE: 'DELETE';
INTO: 'INTO';
DESCRIBE: 'DESCRIBE';
EXPLAIN: 'EXPLAIN';
FORMAT: 'FORMAT';
LOGICAL: 'LOGICAL';
CODEGEN: 'CODEGEN';
COST: 'COST';
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
SETMINUS: 'MINUS';
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
DATA: 'DATA';
START: 'START';
TRANSACTION: 'TRANSACTION';
COMMIT: 'COMMIT';
ROLLBACK: 'ROLLBACK';
MACRO: 'MACRO';
IGNORE: 'IGNORE';
BOTH: 'BOTH';
LEADING: 'LEADING';
TRAILING: 'TRAILING';

IF: 'IF';
POSITION: 'POSITION';
EXTRACT: 'EXTRACT';

EQ  : '=' | '==';
NSEQ: '<=>';
NEQ : '<>';
NEQJ: '!=';
LT  : '<';
LTE : '<=' | '!>';
GT  : '>';
GTE : '>=' | '!<';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
DIV: 'DIV';
TILDE: '~';
AMPERSAND: '&';
PIPE: '|';
CONCAT_PIPE: '||';
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
RECORDWRITER: 'RECORDWRITER';
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
GLOBAL: 'GLOBAL';
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
CASCADE: 'CASCADE';
RESTRICT: 'RESTRICT';
CLUSTERED: 'CLUSTERED';
SORTED: 'SORTED';
PURGE: 'PURGE';
INPUTFORMAT: 'INPUTFORMAT';
OUTPUTFORMAT: 'OUTPUTFORMAT';
DATABASE: 'DATABASE' | 'SCHEMA';
DATABASES: 'DATABASES' | 'SCHEMAS';
DFS: 'DFS';
TRUNCATE: 'TRUNCATE';
ANALYZE: 'ANALYZE';
COMPUTE: 'COMPUTE';
LIST: 'LIST';
STATISTICS: 'STATISTICS';
PARTITIONED: 'PARTITIONED';
EXTERNAL: 'EXTERNAL';
DEFINED: 'DEFINED';
REVOKE: 'REVOKE';
GRANT: 'GRANT';
LOCK: 'LOCK';
UNLOCK: 'UNLOCK';
MSCK: 'MSCK';
REPAIR: 'REPAIR';
RECOVER: 'RECOVER';
EXPORT: 'EXPORT';
IMPORT: 'IMPORT';
LOAD: 'LOAD';
ROLE: 'ROLE';
ROLES: 'ROLES';
COMPACTIONS: 'COMPACTIONS';
PRINCIPALS: 'PRINCIPALS';
TRANSACTIONS: 'TRANSACTIONS';
INDEX: 'INDEX';
INDEXES: 'INDEXES';
LOCKS: 'LOCKS';
OPTION: 'OPTION';
ANTI: 'ANTI';
LOCAL: 'LOCAL';
INPATH: 'INPATH';

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
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
    : DIGIT+ EXPONENT
    | DECIMAL_DIGITS EXPONENT? {isValidDecimal()}?
    ;

DOUBLE_LITERAL
    : DIGIT+ EXPONENT? 'D'
    | DECIMAL_DIGITS EXPONENT? 'D' {isValidDecimal()}?
    ;

BIGDECIMAL_LITERAL
    : DIGIT+ EXPONENT? 'BD'
    | DECIMAL_DIGITS EXPONENT? 'BD' {isValidDecimal()}?
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
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

BRACKETED_EMPTY_COMMENT
    : '/**/' -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' ~[+] .*? '*/' -> channel(HIDDEN)
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
