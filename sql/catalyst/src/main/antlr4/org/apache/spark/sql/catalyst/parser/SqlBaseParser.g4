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

parser grammar SqlBaseParser;

options { tokenVocab = SqlBaseLexer; }

@members {
  /**
   * When false, INTERSECT is given the greater precedence over the other set
   * operations (UNION, EXCEPT and MINUS) as per the SQL standard.
   */
  public boolean legacy_setops_precedence_enabled = false;

  /**
   * When false, a literal with an exponent would be converted into
   * double type rather than decimal type.
   */
  public boolean legacy_exponent_literal_as_decimal_enabled = false;

  /**
   * When true, the behavior of keywords follows ANSI SQL standard.
   */
  public boolean SQL_standard_keyword_behavior = false;
}

singleStatement
    : statement SEMICOLON* EOF
    ;

singleExpression
    : namedExpression EOF
    ;

singleTableIdentifier
    : tableIdentifier EOF
    ;

singleMultipartIdentifier
    : multipartIdentifier EOF
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
    | ctes? dmlStatementNoWith                                         #dmlStatement
    | USE multipartIdentifier                                          #use
    | USE namespace multipartIdentifier                                #useNamespace
    | SET CATALOG (identifier | STRING)                                #setCatalog
    | CREATE namespace (IF NOT EXISTS)? multipartIdentifier
        (commentSpec |
         locationSpec |
         (WITH (DBPROPERTIES | PROPERTIES) propertyList))*             #createNamespace
    | ALTER namespace multipartIdentifier
        SET (DBPROPERTIES | PROPERTIES) propertyList                   #setNamespaceProperties
    | ALTER namespace multipartIdentifier
        SET locationSpec                                               #setNamespaceLocation
    | DROP namespace (IF EXISTS)? multipartIdentifier
        (RESTRICT | CASCADE)?                                          #dropNamespace
    | SHOW namespaces ((FROM | IN) multipartIdentifier)?
        (LIKE? pattern=STRING)?                                        #showNamespaces
    | createTableHeader (LEFT_PAREN createOrReplaceTableColTypeList RIGHT_PAREN)? tableProvider?
        createTableClauses
        (AS? query)?                                                   #createTable
    | CREATE TABLE (IF NOT EXISTS)? target=tableIdentifier
        LIKE source=tableIdentifier
        (tableProvider |
        rowFormat |
        createFileFormat |
        locationSpec |
        (TBLPROPERTIES tableProps=propertyList))*                      #createTableLike
    | replaceTableHeader (LEFT_PAREN createOrReplaceTableColTypeList RIGHT_PAREN)? tableProvider?
        createTableClauses
        (AS? query)?                                                   #replaceTable
    | ANALYZE TABLE multipartIdentifier partitionSpec? COMPUTE STATISTICS
        (identifier | FOR COLUMNS identifierSeq | FOR ALL COLUMNS)?    #analyze
    | ANALYZE TABLES ((FROM | IN) multipartIdentifier)? COMPUTE STATISTICS
        (identifier)?                                                  #analyzeTables
    | ALTER TABLE multipartIdentifier
        ADD (COLUMN | COLUMNS)
        columns=qualifiedColTypeWithPositionList                       #addTableColumns
    | ALTER TABLE multipartIdentifier
        ADD (COLUMN | COLUMNS)
        LEFT_PAREN columns=qualifiedColTypeWithPositionList RIGHT_PAREN #addTableColumns
    | ALTER TABLE table=multipartIdentifier
        RENAME COLUMN
        from=multipartIdentifier TO to=errorCapturingIdentifier        #renameTableColumn
    | ALTER TABLE multipartIdentifier
        DROP (COLUMN | COLUMNS) (IF EXISTS)?
        LEFT_PAREN columns=multipartIdentifierList RIGHT_PAREN         #dropTableColumns
    | ALTER TABLE multipartIdentifier
        DROP (COLUMN | COLUMNS) (IF EXISTS)?
        columns=multipartIdentifierList                                #dropTableColumns
    | ALTER (TABLE | VIEW) from=multipartIdentifier
        RENAME TO to=multipartIdentifier                               #renameTable
    | ALTER (TABLE | VIEW) multipartIdentifier
        SET TBLPROPERTIES propertyList                                 #setTableProperties
    | ALTER (TABLE | VIEW) multipartIdentifier
        UNSET TBLPROPERTIES (IF EXISTS)? propertyList                  #unsetTableProperties
    | ALTER TABLE table=multipartIdentifier
        (ALTER | CHANGE) COLUMN? column=multipartIdentifier
        alterColumnAction?                                             #alterTableAlterColumn
    | ALTER TABLE table=multipartIdentifier partitionSpec?
        CHANGE COLUMN?
        colName=multipartIdentifier colType colPosition?               #hiveChangeColumn
    | ALTER TABLE table=multipartIdentifier partitionSpec?
        REPLACE COLUMNS
        LEFT_PAREN columns=qualifiedColTypeWithPositionList
        RIGHT_PAREN                                                    #hiveReplaceColumns
    | ALTER TABLE multipartIdentifier (partitionSpec)?
        SET SERDE STRING (WITH SERDEPROPERTIES propertyList)?          #setTableSerDe
    | ALTER TABLE multipartIdentifier (partitionSpec)?
        SET SERDEPROPERTIES propertyList                               #setTableSerDe
    | ALTER (TABLE | VIEW) multipartIdentifier ADD (IF NOT EXISTS)?
        partitionSpecLocation+                                         #addTablePartition
    | ALTER TABLE multipartIdentifier
        from=partitionSpec RENAME TO to=partitionSpec                  #renameTablePartition
    | ALTER (TABLE | VIEW) multipartIdentifier
        DROP (IF EXISTS)? partitionSpec (COMMA partitionSpec)* PURGE?  #dropTablePartitions
    | ALTER TABLE multipartIdentifier
        (partitionSpec)? SET locationSpec                              #setTableLocation
    | ALTER TABLE multipartIdentifier RECOVER PARTITIONS               #recoverPartitions
    | DROP TABLE (IF EXISTS)? multipartIdentifier PURGE?               #dropTable
    | DROP VIEW (IF EXISTS)? multipartIdentifier                       #dropView
    | CREATE (OR REPLACE)? (GLOBAL? TEMPORARY)?
        VIEW (IF NOT EXISTS)? multipartIdentifier
        identifierCommentList?
        (commentSpec |
         (PARTITIONED ON identifierList) |
         (TBLPROPERTIES propertyList))*
        AS query                                                       #createView
    | CREATE (OR REPLACE)? GLOBAL? TEMPORARY VIEW
        tableIdentifier (LEFT_PAREN colTypeList RIGHT_PAREN)? tableProvider
        (OPTIONS propertyList)?                                        #createTempViewUsing
    | ALTER VIEW multipartIdentifier AS? query                         #alterViewQuery
    | CREATE (OR REPLACE)? TEMPORARY? FUNCTION (IF NOT EXISTS)?
        multipartIdentifier AS className=STRING
        (USING resource (COMMA resource)*)?                            #createFunction
    | DROP TEMPORARY? FUNCTION (IF EXISTS)? multipartIdentifier        #dropFunction
    | EXPLAIN (LOGICAL | FORMATTED | EXTENDED | CODEGEN | COST)?
        statement                                                      #explain
    | SHOW TABLES ((FROM | IN) multipartIdentifier)?
        (LIKE? pattern=STRING)?                                        #showTables
    | SHOW TABLE EXTENDED ((FROM | IN) ns=multipartIdentifier)?
        LIKE pattern=STRING partitionSpec?                             #showTableExtended
    | SHOW TBLPROPERTIES table=multipartIdentifier
        (LEFT_PAREN key=propertyKey RIGHT_PAREN)?                      #showTblProperties
    | SHOW COLUMNS (FROM | IN) table=multipartIdentifier
        ((FROM | IN) ns=multipartIdentifier)?                          #showColumns
    | SHOW VIEWS ((FROM | IN) multipartIdentifier)?
        (LIKE? pattern=STRING)?                                        #showViews
    | SHOW PARTITIONS multipartIdentifier partitionSpec?               #showPartitions
    | SHOW identifier? FUNCTIONS ((FROM | IN) ns=multipartIdentifier)?
        (LIKE? (legacy=multipartIdentifier | pattern=STRING))?         #showFunctions
    | SHOW CREATE TABLE multipartIdentifier (AS SERDE)?                #showCreateTable
    | SHOW CURRENT namespace                                           #showCurrentNamespace
    | SHOW CATALOGS (LIKE? pattern=STRING)?                            #showCatalogs
    | (DESC | DESCRIBE) FUNCTION EXTENDED? describeFuncName            #describeFunction
    | (DESC | DESCRIBE) namespace EXTENDED?
        multipartIdentifier                                            #describeNamespace
    | (DESC | DESCRIBE) TABLE? option=(EXTENDED | FORMATTED)?
        multipartIdentifier partitionSpec? describeColName?            #describeRelation
    | (DESC | DESCRIBE) QUERY? query                                   #describeQuery
    | COMMENT ON namespace multipartIdentifier IS
        comment=(STRING | NULL)                                        #commentNamespace
    | COMMENT ON TABLE multipartIdentifier IS comment=(STRING | NULL)  #commentTable
    | REFRESH TABLE multipartIdentifier                                #refreshTable
    | REFRESH FUNCTION multipartIdentifier                             #refreshFunction
    | REFRESH (STRING | .*?)                                           #refreshResource
    | CACHE LAZY? TABLE multipartIdentifier
        (OPTIONS options=propertyList)? (AS? query)?                   #cacheTable
    | UNCACHE TABLE (IF EXISTS)? multipartIdentifier                   #uncacheTable
    | CLEAR CACHE                                                      #clearCache
    | LOAD DATA LOCAL? INPATH path=STRING OVERWRITE? INTO TABLE
        multipartIdentifier partitionSpec?                             #loadData
    | TRUNCATE TABLE multipartIdentifier partitionSpec?                #truncateTable
    | MSCK REPAIR TABLE multipartIdentifier
        (option=(ADD|DROP|SYNC) PARTITIONS)?                           #repairTable
    | op=(ADD | LIST) identifier .*?                                   #manageResource
    | SET ROLE .*?                                                     #failNativeCommand
    | SET TIME ZONE interval                                           #setTimeZone
    | SET TIME ZONE timezone=(STRING | LOCAL)                          #setTimeZone
    | SET TIME ZONE .*?                                                #setTimeZone
    | SET configKey EQ configValue                                     #setQuotedConfiguration
    | SET configKey (EQ .*?)?                                          #setConfiguration
    | SET .*? EQ configValue                                           #setQuotedConfiguration
    | SET .*?                                                          #setConfiguration
    | RESET configKey                                                  #resetQuotedConfiguration
    | RESET .*?                                                        #resetConfiguration
    | CREATE INDEX (IF NOT EXISTS)? identifier ON TABLE?
        multipartIdentifier (USING indexType=identifier)?
        LEFT_PAREN columns=multipartIdentifierPropertyList RIGHT_PAREN
        (OPTIONS options=propertyList)?                                #createIndex
    | DROP INDEX (IF EXISTS)? identifier ON TABLE? multipartIdentifier #dropIndex
    | unsupportedHiveNativeCommands .*?                                #failNativeCommand
    ;

configKey
    : quotedIdentifier
    ;

configValue
    : quotedIdentifier
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
    ;

createTableHeader
    : CREATE TEMPORARY? EXTERNAL? TABLE (IF NOT EXISTS)? multipartIdentifier
    ;

replaceTableHeader
    : (CREATE OR)? REPLACE TABLE multipartIdentifier
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

commentSpec
    : COMMENT STRING
    ;

query
    : ctes? queryTerm queryOrganization
    ;

insertInto
    : INSERT OVERWRITE TABLE? multipartIdentifier (partitionSpec (IF NOT EXISTS)?)?  identifierList?        #insertOverwriteTable
    | INSERT INTO TABLE? multipartIdentifier partitionSpec? (IF NOT EXISTS)? identifierList?                #insertIntoTable
    | INSERT OVERWRITE LOCAL? DIRECTORY path=STRING rowFormat? createFileFormat?                            #insertOverwriteHiveDir
    | INSERT OVERWRITE LOCAL? DIRECTORY (path=STRING)? tableProvider (OPTIONS options=propertyList)?        #insertOverwriteDir
    ;

partitionSpecLocation
    : partitionSpec locationSpec?
    ;

partitionSpec
    : PARTITION LEFT_PAREN partitionVal (COMMA partitionVal)* RIGHT_PAREN
    ;

partitionVal
    : identifier (EQ constant)?
    | identifier EQ DEFAULT
    ;

namespace
    : NAMESPACE
    | DATABASE
    | SCHEMA
    ;

namespaces
    : NAMESPACES
    | DATABASES
    | SCHEMAS
    ;

describeFuncName
    : qualifiedName
    | STRING
    | comparisonOperator
    | arithmeticOperator
    | predicateOperator
    ;

describeColName
    : nameParts+=identifier (DOT nameParts+=identifier)*
    ;

ctes
    : WITH namedQuery (COMMA namedQuery)*
    ;

namedQuery
    : name=errorCapturingIdentifier (columnAliases=identifierList)? AS? LEFT_PAREN query RIGHT_PAREN
    ;

tableProvider
    : USING multipartIdentifier
    ;

createTableClauses
    :((OPTIONS options=propertyList) |
     (PARTITIONED BY partitioning=partitionFieldList) |
     skewSpec |
     bucketSpec |
     rowFormat |
     createFileFormat |
     locationSpec |
     commentSpec |
     (TBLPROPERTIES tableProps=propertyList))*
    ;

propertyList
    : LEFT_PAREN property (COMMA property)* RIGHT_PAREN
    ;

property
    : key=propertyKey (EQ? value=propertyValue)?
    ;

propertyKey
    : identifier (DOT identifier)*
    | STRING
    ;

propertyValue
    : INTEGER_VALUE
    | DECIMAL_VALUE
    | booleanValue
    | STRING
    ;

constantList
    : LEFT_PAREN constant (COMMA constant)* RIGHT_PAREN
    ;

nestedConstantList
    : LEFT_PAREN constantList (COMMA constantList)* RIGHT_PAREN
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
    : STRING (WITH SERDEPROPERTIES propertyList)?
    ;

resource
    : identifier STRING
    ;

dmlStatementNoWith
    : insertInto query                                                             #singleInsertQuery
    | fromClause multiInsertQueryBody+                                             #multiInsertQuery
    | DELETE FROM multipartIdentifier tableAlias whereClause?                      #deleteFromTable
    | UPDATE multipartIdentifier tableAlias setClause whereClause?                 #updateTable
    | MERGE INTO target=multipartIdentifier targetAlias=tableAlias
        USING (source=multipartIdentifier |
          LEFT_PAREN sourceQuery=query RIGHT_PAREN) sourceAlias=tableAlias
        ON mergeCondition=booleanExpression
        matchedClause*
        notMatchedClause*                                                          #mergeIntoTable
    ;

queryOrganization
    : (ORDER BY order+=sortItem (COMMA order+=sortItem)*)?
      (CLUSTER BY clusterBy+=expression (COMMA clusterBy+=expression)*)?
      (DISTRIBUTE BY distributeBy+=expression (COMMA distributeBy+=expression)*)?
      (SORT BY sort+=sortItem (COMMA sort+=sortItem)*)?
      windowClause?
      (LIMIT (ALL | limit=expression))?
      (OFFSET offset=expression)?
    ;

multiInsertQueryBody
    : insertInto fromStatementBody
    ;

queryTerm
    : queryPrimary                                                                       #queryTermDefault
    | left=queryTerm {legacy_setops_precedence_enabled}?
        operator=(INTERSECT | UNION | EXCEPT | SETMINUS) setQuantifier? right=queryTerm  #setOperation
    | left=queryTerm {!legacy_setops_precedence_enabled}?
        operator=INTERSECT setQuantifier? right=queryTerm                                #setOperation
    | left=queryTerm {!legacy_setops_precedence_enabled}?
        operator=(UNION | EXCEPT | SETMINUS) setQuantifier? right=queryTerm              #setOperation
    ;

queryPrimary
    : querySpecification                                                    #queryPrimaryDefault
    | fromStatement                                                         #fromStmt
    | TABLE multipartIdentifier                                             #table
    | inlineTable                                                           #inlineTableDefault1
    | LEFT_PAREN query RIGHT_PAREN                                          #subquery
    ;

sortItem
    : expression ordering=(ASC | DESC)? (NULLS nullOrder=(LAST | FIRST))?
    ;

fromStatement
    : fromClause fromStatementBody+
    ;

fromStatementBody
    : transformClause
      whereClause?
      queryOrganization
    | selectClause
      lateralView*
      whereClause?
      aggregationClause?
      havingClause?
      windowClause?
      queryOrganization
    ;

querySpecification
    : transformClause
      fromClause?
      lateralView*
      whereClause?
      aggregationClause?
      havingClause?
      windowClause?                                                         #transformQuerySpecification
    | selectClause
      fromClause?
      lateralView*
      whereClause?
      aggregationClause?
      havingClause?
      windowClause?                                                         #regularQuerySpecification
    ;

transformClause
    : (SELECT kind=TRANSFORM LEFT_PAREN setQuantifier? expressionSeq RIGHT_PAREN
            | kind=MAP setQuantifier? expressionSeq
            | kind=REDUCE setQuantifier? expressionSeq)
      inRowFormat=rowFormat?
      (RECORDWRITER recordWriter=STRING)?
      USING script=STRING
      (AS (identifierSeq | colTypeList | (LEFT_PAREN (identifierSeq | colTypeList) RIGHT_PAREN)))?
      outRowFormat=rowFormat?
      (RECORDREADER recordReader=STRING)?
    ;

selectClause
    : SELECT (hints+=hint)* setQuantifier? namedExpressionSeq
    ;

setClause
    : SET assignmentList
    ;

matchedClause
    : WHEN MATCHED (AND matchedCond=booleanExpression)? THEN matchedAction
    ;
notMatchedClause
    : WHEN NOT MATCHED (AND notMatchedCond=booleanExpression)? THEN notMatchedAction
    ;

matchedAction
    : DELETE
    | UPDATE SET ASTERISK
    | UPDATE SET assignmentList
    ;

notMatchedAction
    : INSERT ASTERISK
    | INSERT LEFT_PAREN columns=multipartIdentifierList RIGHT_PAREN
        VALUES LEFT_PAREN expression (COMMA expression)* RIGHT_PAREN
    ;

assignmentList
    : assignment (COMMA assignment)*
    ;

assignment
    : key=multipartIdentifier EQ value=expression
    ;

whereClause
    : WHERE booleanExpression
    ;

havingClause
    : HAVING booleanExpression
    ;

hint
    : HENT_START hintStatements+=hintStatement (COMMA? hintStatements+=hintStatement)* HENT_END
    ;

hintStatement
    : hintName=identifier
    | hintName=identifier LEFT_PAREN parameters+=primaryExpression (COMMA parameters+=primaryExpression)* RIGHT_PAREN
    ;

fromClause
    : FROM relation (COMMA relation)* lateralView* pivotClause?
    ;

temporalClause
    : FOR? (SYSTEM_VERSION | VERSION) AS OF version=(INTEGER_VALUE | STRING)
    | FOR? (SYSTEM_TIME | TIMESTAMP) AS OF timestamp=valueExpression
    ;

aggregationClause
    : GROUP BY groupingExpressionsWithGroupingAnalytics+=groupByClause
        (COMMA groupingExpressionsWithGroupingAnalytics+=groupByClause)*
    | GROUP BY groupingExpressions+=expression (COMMA groupingExpressions+=expression)* (
      WITH kind=ROLLUP
    | WITH kind=CUBE
    | kind=GROUPING SETS LEFT_PAREN groupingSet (COMMA groupingSet)* RIGHT_PAREN)?
    ;

groupByClause
    : groupingAnalytics
    | expression
    ;

groupingAnalytics
    : (ROLLUP | CUBE) LEFT_PAREN groupingSet (COMMA groupingSet)* RIGHT_PAREN
    | GROUPING SETS LEFT_PAREN groupingElement (COMMA groupingElement)* RIGHT_PAREN
    ;

groupingElement
    : groupingAnalytics
    | groupingSet
    ;

groupingSet
    : LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN
    | expression
    ;

pivotClause
    : PIVOT LEFT_PAREN aggregates=namedExpressionSeq FOR pivotColumn IN LEFT_PAREN pivotValues+=pivotValue (COMMA pivotValues+=pivotValue)* RIGHT_PAREN RIGHT_PAREN
    ;

pivotColumn
    : identifiers+=identifier
    | LEFT_PAREN identifiers+=identifier (COMMA identifiers+=identifier)* RIGHT_PAREN
    ;

pivotValue
    : expression (AS? identifier)?
    ;

lateralView
    : LATERAL VIEW (OUTER)? qualifiedName LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN tblName=identifier (AS? colName+=identifier (COMMA colName+=identifier)*)?
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

relation
    : LATERAL? relationPrimary joinRelation*
    ;

joinRelation
    : (joinType) JOIN LATERAL? right=relationPrimary joinCriteria?
    | NATURAL joinType JOIN LATERAL? right=relationPrimary
    ;

joinType
    : INNER?
    | CROSS
    | LEFT OUTER?
    | LEFT? SEMI
    | RIGHT OUTER?
    | FULL OUTER?
    | LEFT? ANTI
    ;

joinCriteria
    : ON booleanExpression
    | USING identifierList
    ;

sample
    : TABLESAMPLE LEFT_PAREN sampleMethod? RIGHT_PAREN (REPEATABLE LEFT_PAREN seed=INTEGER_VALUE RIGHT_PAREN)?
    ;

sampleMethod
    : negativeSign=MINUS? percentage=(INTEGER_VALUE | DECIMAL_VALUE) PERCENTLIT   #sampleByPercentile
    | expression ROWS                                                             #sampleByRows
    | sampleType=BUCKET numerator=INTEGER_VALUE OUT OF denominator=INTEGER_VALUE
        (ON (identifier | qualifiedName LEFT_PAREN RIGHT_PAREN))?                 #sampleByBucket
    | bytes=expression                                                            #sampleByBytes
    ;

identifierList
    : LEFT_PAREN identifierSeq RIGHT_PAREN
    ;

identifierSeq
    : ident+=errorCapturingIdentifier (COMMA ident+=errorCapturingIdentifier)*
    ;

orderedIdentifierList
    : LEFT_PAREN orderedIdentifier (COMMA orderedIdentifier)* RIGHT_PAREN
    ;

orderedIdentifier
    : ident=errorCapturingIdentifier ordering=(ASC | DESC)?
    ;

identifierCommentList
    : LEFT_PAREN identifierComment (COMMA identifierComment)* RIGHT_PAREN
    ;

identifierComment
    : identifier commentSpec?
    ;

relationPrimary
    : multipartIdentifier temporalClause?
      sample? tableAlias                                    #tableName
    | LEFT_PAREN query RIGHT_PAREN sample? tableAlias       #aliasedQuery
    | LEFT_PAREN relation RIGHT_PAREN sample? tableAlias    #aliasedRelation
    | inlineTable                                           #inlineTableDefault2
    | functionTable                                         #tableValuedFunction
    ;

inlineTable
    : VALUES expression (COMMA expression)* tableAlias
    ;

functionTable
    : funcName=functionName LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN tableAlias
    ;

tableAlias
    : (AS? strictIdentifier identifierList?)?
    ;

rowFormat
    : ROW FORMAT SERDE name=STRING (WITH SERDEPROPERTIES props=propertyList)?       #rowFormatSerde
    | ROW FORMAT DELIMITED
      (FIELDS TERMINATED BY fieldsTerminatedBy=STRING (ESCAPED BY escapedBy=STRING)?)?
      (COLLECTION ITEMS TERMINATED BY collectionItemsTerminatedBy=STRING)?
      (MAP KEYS TERMINATED BY keysTerminatedBy=STRING)?
      (LINES TERMINATED BY linesSeparatedBy=STRING)?
      (NULL DEFINED AS nullDefinedAs=STRING)?                                       #rowFormatDelimited
    ;

multipartIdentifierList
    : multipartIdentifier (COMMA multipartIdentifier)*
    ;

multipartIdentifier
    : parts+=errorCapturingIdentifier (DOT parts+=errorCapturingIdentifier)*
    ;

multipartIdentifierPropertyList
    : multipartIdentifierProperty (COMMA multipartIdentifierProperty)*
    ;

multipartIdentifierProperty
    : multipartIdentifier (OPTIONS options=propertyList)?
    ;

tableIdentifier
    : (db=errorCapturingIdentifier DOT)? table=errorCapturingIdentifier
    ;

functionIdentifier
    : (db=errorCapturingIdentifier DOT)? function=errorCapturingIdentifier
    ;

namedExpression
    : expression (AS? (name=errorCapturingIdentifier | identifierList))?
    ;

namedExpressionSeq
    : namedExpression (COMMA namedExpression)*
    ;

partitionFieldList
    : LEFT_PAREN fields+=partitionField (COMMA fields+=partitionField)* RIGHT_PAREN
    ;

partitionField
    : transform  #partitionTransform
    | colType    #partitionColumn
    ;

transform
    : qualifiedName                                                                             #identityTransform
    | transformName=identifier
      LEFT_PAREN argument+=transformArgument (COMMA argument+=transformArgument)* RIGHT_PAREN   #applyTransform
    ;

transformArgument
    : qualifiedName
    | constant
    ;

expression
    : booleanExpression
    ;

expressionSeq
    : expression (COMMA expression)*
    ;

booleanExpression
    : NOT booleanExpression                                        #logicalNot
    | EXISTS LEFT_PAREN query RIGHT_PAREN                          #exists
    | valueExpression predicate?                                   #predicated
    | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
    ;

predicate
    : NOT? kind=BETWEEN lower=valueExpression AND upper=valueExpression
    | NOT? kind=IN LEFT_PAREN expression (COMMA expression)* RIGHT_PAREN
    | NOT? kind=IN LEFT_PAREN query RIGHT_PAREN
    | NOT? kind=RLIKE pattern=valueExpression
    | NOT? kind=(LIKE | ILIKE) quantifier=(ANY | SOME | ALL) (LEFT_PAREN RIGHT_PAREN | LEFT_PAREN expression (COMMA expression)* RIGHT_PAREN)
    | NOT? kind=(LIKE | ILIKE) pattern=valueExpression (ESCAPE escapeChar=STRING)?
    | IS NOT? kind=NULL
    | IS NOT? kind=(TRUE | FALSE | UNKNOWN)
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

datetimeUnit
    : YEAR | QUARTER | MONTH
    | WEEK | DAY | DAYOFYEAR
    | HOUR | MINUTE | SECOND | MILLISECOND | MICROSECOND
    ;

primaryExpression
    : name=(CURRENT_DATE | CURRENT_TIMESTAMP | CURRENT_USER | USER)                                   #currentLike
    | name=(TIMESTAMPADD | DATEADD) LEFT_PAREN unit=datetimeUnit COMMA unitsAmount=valueExpression COMMA timestamp=valueExpression RIGHT_PAREN             #timestampadd
    | name=(TIMESTAMPDIFF | DATEDIFF) LEFT_PAREN unit=datetimeUnit COMMA startTimestamp=valueExpression COMMA endTimestamp=valueExpression RIGHT_PAREN    #timestampdiff
    | CASE whenClause+ (ELSE elseExpression=expression)? END                                   #searchedCase
    | CASE value=expression whenClause+ (ELSE elseExpression=expression)? END                  #simpleCase
    | name=(CAST | TRY_CAST) LEFT_PAREN expression AS dataType RIGHT_PAREN                     #cast
    | STRUCT LEFT_PAREN (argument+=namedExpression (COMMA argument+=namedExpression)*)? RIGHT_PAREN #struct
    | FIRST LEFT_PAREN expression (IGNORE NULLS)? RIGHT_PAREN                                  #first
    | ANY_VALUE LEFT_PAREN expression (IGNORE NULLS)? RIGHT_PAREN                              #any_value
    | LAST LEFT_PAREN expression (IGNORE NULLS)? RIGHT_PAREN                                   #last
    | POSITION LEFT_PAREN substr=valueExpression IN str=valueExpression RIGHT_PAREN            #position
    | constant                                                                                 #constantDefault
    | ASTERISK                                                                                 #star
    | qualifiedName DOT ASTERISK                                                               #star
    | LEFT_PAREN namedExpression (COMMA namedExpression)+ RIGHT_PAREN                          #rowConstructor
    | LEFT_PAREN query RIGHT_PAREN                                                             #subqueryExpression
    | functionName LEFT_PAREN (setQuantifier? argument+=expression (COMMA argument+=expression)*)? RIGHT_PAREN
       (FILTER LEFT_PAREN WHERE where=booleanExpression RIGHT_PAREN)?
       (nullsOption=(IGNORE | RESPECT) NULLS)? ( OVER windowSpec)?                             #functionCall
    | identifier ARROW expression                                                              #lambda
    | LEFT_PAREN identifier (COMMA identifier)+ RIGHT_PAREN ARROW expression                   #lambda
    | value=primaryExpression LEFT_BRACKET index=valueExpression RIGHT_BRACKET                 #subscript
    | identifier                                                                               #columnReference
    | base=primaryExpression DOT fieldName=identifier                                          #dereference
    | LEFT_PAREN expression RIGHT_PAREN                                                        #parenthesizedExpression
    | EXTRACT LEFT_PAREN field=identifier FROM source=valueExpression RIGHT_PAREN              #extract
    | (SUBSTR | SUBSTRING) LEFT_PAREN str=valueExpression (FROM | COMMA) pos=valueExpression
      ((FOR | COMMA) len=valueExpression)? RIGHT_PAREN                                         #substring
    | TRIM LEFT_PAREN trimOption=(BOTH | LEADING | TRAILING)? (trimStr=valueExpression)?
       FROM srcStr=valueExpression RIGHT_PAREN                                                 #trim
    | OVERLAY LEFT_PAREN input=valueExpression PLACING replace=valueExpression
      FROM position=valueExpression (FOR length=valueExpression)? RIGHT_PAREN                  #overlay
    | name=(PERCENTILE_CONT | PERCENTILE_DISC) LEFT_PAREN percentage=valueExpression RIGHT_PAREN
        WITHIN GROUP LEFT_PAREN ORDER BY sortItem RIGHT_PAREN
        (FILTER LEFT_PAREN WHERE where=booleanExpression RIGHT_PAREN)? ( OVER windowSpec)?     #percentile
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
    : INTERVAL (errorCapturingMultiUnitsInterval | errorCapturingUnitToUnitInterval)?
    ;

errorCapturingMultiUnitsInterval
    : body=multiUnitsInterval unitToUnitInterval?
    ;

multiUnitsInterval
    : (intervalValue unit+=identifier)+
    ;

errorCapturingUnitToUnitInterval
    : body=unitToUnitInterval (error1=multiUnitsInterval | error2=unitToUnitInterval)?
    ;

unitToUnitInterval
    : value=intervalValue from=identifier TO to=identifier
    ;

intervalValue
    : (PLUS | MINUS)? (INTEGER_VALUE | DECIMAL_VALUE | STRING)
    ;

colPosition
    : position=FIRST | position=AFTER afterCol=errorCapturingIdentifier
    ;

dataType
    : complex=ARRAY LT dataType GT                              #complexDataType
    | complex=MAP LT dataType COMMA dataType GT                 #complexDataType
    | complex=STRUCT (LT complexColTypeList? GT | NEQ)          #complexDataType
    | INTERVAL from=(YEAR | MONTH) (TO to=MONTH)?               #yearMonthIntervalDataType
    | INTERVAL from=(DAY | HOUR | MINUTE | SECOND)
      (TO to=(HOUR | MINUTE | SECOND))?                         #dayTimeIntervalDataType
    | identifier (LEFT_PAREN INTEGER_VALUE
      (COMMA INTEGER_VALUE)* RIGHT_PAREN)?                      #primitiveDataType
    ;

qualifiedColTypeWithPositionList
    : qualifiedColTypeWithPosition (COMMA qualifiedColTypeWithPosition)*
    ;

qualifiedColTypeWithPosition
    : name=multipartIdentifier dataType (NOT NULL)? defaultExpression? commentSpec? colPosition?
    ;

defaultExpression
    : DEFAULT expression
    ;

colTypeList
    : colType (COMMA colType)*
    ;

colType
    : colName=errorCapturingIdentifier dataType (NOT NULL)? commentSpec?
    ;

createOrReplaceTableColTypeList
    : createOrReplaceTableColType (COMMA createOrReplaceTableColType)*
    ;

createOrReplaceTableColType
    : colName=errorCapturingIdentifier dataType (NOT NULL)? defaultExpression? commentSpec?
    ;

complexColTypeList
    : complexColType (COMMA complexColType)*
    ;

complexColType
    : identifier COLON? dataType (NOT NULL)? commentSpec?
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

windowClause
    : WINDOW namedWindow (COMMA namedWindow)*
    ;

namedWindow
    : name=errorCapturingIdentifier AS windowSpec
    ;

windowSpec
    : name=errorCapturingIdentifier                         #windowRef
    | LEFT_PAREN name=errorCapturingIdentifier RIGHT_PAREN  #windowRef
    | LEFT_PAREN
      ( CLUSTER BY partition+=expression (COMMA partition+=expression)*
      | ((PARTITION | DISTRIBUTE) BY partition+=expression (COMMA partition+=expression)*)?
        ((ORDER | SORT) BY sortItem (COMMA sortItem)*)?)
      windowFrame?
      RIGHT_PAREN                                           #windowDef
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

qualifiedNameList
    : qualifiedName (COMMA qualifiedName)*
    ;

functionName
    : qualifiedName
    | FILTER
    | LEFT
    | RIGHT
    ;

qualifiedName
    : identifier (DOT identifier)*
    ;

// this rule is used for explicitly capturing wrong identifiers such as test-table, which should actually be `test-table`
// replace identifier with errorCapturingIdentifier where the immediate follow symbol is not an expression, otherwise
// valid expressions such as "a-b" can be recognized as an identifier
errorCapturingIdentifier
    : identifier errorCapturingIdentifierExtra
    ;

// extra left-factoring grammar
errorCapturingIdentifierExtra
    : (MINUS identifier)+    #errorIdent
    |                        #realIdent
    ;

identifier
    : strictIdentifier
    | {!SQL_standard_keyword_behavior}? strictNonReserved
    ;

strictIdentifier
    : IDENTIFIER              #unquotedIdentifier
    | quotedIdentifier        #quotedIdentifierAlternative
    | {SQL_standard_keyword_behavior}? ansiNonReserved #unquotedIdentifier
    | {!SQL_standard_keyword_behavior}? nonReserved    #unquotedIdentifier
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

number
    : {!legacy_exponent_literal_as_decimal_enabled}? MINUS? EXPONENT_VALUE #exponentLiteral
    | {!legacy_exponent_literal_as_decimal_enabled}? MINUS? DECIMAL_VALUE  #decimalLiteral
    | {legacy_exponent_literal_as_decimal_enabled}? MINUS? (EXPONENT_VALUE | DECIMAL_VALUE) #legacyDecimalLiteral
    | MINUS? INTEGER_VALUE            #integerLiteral
    | MINUS? BIGINT_LITERAL           #bigIntLiteral
    | MINUS? SMALLINT_LITERAL         #smallIntLiteral
    | MINUS? TINYINT_LITERAL          #tinyIntLiteral
    | MINUS? DOUBLE_LITERAL           #doubleLiteral
    | MINUS? FLOAT_LITERAL            #floatLiteral
    | MINUS? BIGDECIMAL_LITERAL       #bigDecimalLiteral
    ;

alterColumnAction
    : TYPE dataType
    | commentSpec
    | colPosition
    | setOrDrop=(SET | DROP) NOT NULL
    | SET defaultExpression
    | dropDefault=DROP DEFAULT
    ;



// When `SQL_standard_keyword_behavior=true`, there are 2 kinds of keywords in Spark SQL.
// - Reserved keywords:
//     Keywords that are reserved and can't be used as identifiers for table, view, column,
//     function, alias, etc.
// - Non-reserved keywords:
//     Keywords that have a special meaning only in particular contexts and can be used as
//     identifiers in other contexts. For example, `EXPLAIN SELECT ...` is a command, but EXPLAIN
//     can be used as identifiers in other places.
// You can find the full keywords list by searching "Start of the keywords list" in this file.
// The non-reserved keywords are listed below. Keywords not in this list are reserved keywords.
ansiNonReserved
//--ANSI-NON-RESERVED-START
    : ADD
    | AFTER
    | ALTER
    | ANALYZE
    | ANTI
    | ANY_VALUE
    | ARCHIVE
    | ARRAY
    | ASC
    | AT
    | BETWEEN
    | BUCKET
    | BUCKETS
    | BY
    | CACHE
    | CASCADE
    | CATALOG
    | CATALOGS
    | CHANGE
    | CLEAR
    | CLUSTER
    | CLUSTERED
    | CODEGEN
    | COLLECTION
    | COLUMNS
    | COMMENT
    | COMMIT
    | COMPACT
    | COMPACTIONS
    | COMPUTE
    | CONCATENATE
    | COST
    | CUBE
    | CURRENT
    | DATA
    | DATABASE
    | DATABASES
    | DATEADD
    | DATEDIFF
    | DAY
    | DAYOFYEAR
    | DBPROPERTIES
    | DEFAULT
    | DEFINED
    | DELETE
    | DELIMITED
    | DESC
    | DESCRIBE
    | DFS
    | DIRECTORIES
    | DIRECTORY
    | DISTRIBUTE
    | DIV
    | DROP
    | ESCAPED
    | EXCHANGE
    | EXISTS
    | EXPLAIN
    | EXPORT
    | EXTENDED
    | EXTERNAL
    | EXTRACT
    | FIELDS
    | FILEFORMAT
    | FIRST
    | FOLLOWING
    | FORMAT
    | FORMATTED
    | FUNCTION
    | FUNCTIONS
    | GLOBAL
    | GROUPING
    | HOUR
    | IF
    | IGNORE
    | IMPORT
    | INDEX
    | INDEXES
    | INPATH
    | INPUTFORMAT
    | INSERT
    | INTERVAL
    | ITEMS
    | KEYS
    | LAST
    | LAZY
    | LIKE
    | ILIKE
    | LIMIT
    | LINES
    | LIST
    | LOAD
    | LOCAL
    | LOCATION
    | LOCK
    | LOCKS
    | LOGICAL
    | MACRO
    | MAP
    | MATCHED
    | MERGE
    | MICROSECOND
    | MILLISECOND
    | MINUTE
    | MONTH
    | MSCK
    | NAMESPACE
    | NAMESPACES
    | NO
    | NULLS
    | OF
    | OPTION
    | OPTIONS
    | OUT
    | OUTPUTFORMAT
    | OVER
    | OVERLAY
    | OVERWRITE
    | PARTITION
    | PARTITIONED
    | PARTITIONS
    | PERCENTLIT
    | PIVOT
    | PLACING
    | POSITION
    | PRECEDING
    | PRINCIPALS
    | PROPERTIES
    | PURGE
    | QUARTER
    | QUERY
    | RANGE
    | RECORDREADER
    | RECORDWRITER
    | RECOVER
    | REDUCE
    | REFRESH
    | RENAME
    | REPAIR
    | REPEATABLE
    | REPLACE
    | RESET
    | RESPECT
    | RESTRICT
    | REVOKE
    | RLIKE
    | ROLE
    | ROLES
    | ROLLBACK
    | ROLLUP
    | ROW
    | ROWS
    | SCHEMA
    | SCHEMAS
    | SECOND
    | SEMI
    | SEPARATED
    | SERDE
    | SERDEPROPERTIES
    | SET
    | SETMINUS
    | SETS
    | SHOW
    | SKEWED
    | SORT
    | SORTED
    | START
    | STATISTICS
    | STORED
    | STRATIFY
    | STRUCT
    | SUBSTR
    | SUBSTRING
    | SYNC
    | SYSTEM_TIME
    | SYSTEM_VERSION
    | TABLES
    | TABLESAMPLE
    | TBLPROPERTIES
    | TEMPORARY
    | TERMINATED
    | TIMESTAMP
    | TIMESTAMPADD
    | TIMESTAMPDIFF
    | TOUCH
    | TRANSACTION
    | TRANSACTIONS
    | TRANSFORM
    | TRIM
    | TRUE
    | TRUNCATE
    | TRY_CAST
    | TYPE
    | UNARCHIVE
    | UNBOUNDED
    | UNCACHE
    | UNLOCK
    | UNSET
    | UPDATE
    | USE
    | VALUES
    | VERSION
    | VIEW
    | VIEWS
    | WEEK
    | WINDOW
    | YEAR
    | ZONE
//--ANSI-NON-RESERVED-END
    ;

// When `SQL_standard_keyword_behavior=false`, there are 2 kinds of keywords in Spark SQL.
// - Non-reserved keywords:
//     Same definition as the one when `SQL_standard_keyword_behavior=true`.
// - Strict-non-reserved keywords:
//     A strict version of non-reserved keywords, which can not be used as table alias.
// You can find the full keywords list by searching "Start of the keywords list" in this file.
// The strict-non-reserved keywords are listed in `strictNonReserved`.
// The non-reserved keywords are listed in `nonReserved`.
// These 2 together contain all the keywords.
strictNonReserved
    : ANTI
    | CROSS
    | EXCEPT
    | FULL
    | INNER
    | INTERSECT
    | JOIN
    | LATERAL
    | LEFT
    | NATURAL
    | ON
    | RIGHT
    | SEMI
    | SETMINUS
    | UNION
    | USING
    ;

nonReserved
//--DEFAULT-NON-RESERVED-START
    : ADD
    | AFTER
    | ALL
    | ALTER
    | ANALYZE
    | AND
    | ANY
    | ANY_VALUE
    | ARCHIVE
    | ARRAY
    | AS
    | ASC
    | AT
    | AUTHORIZATION
    | BETWEEN
    | BOTH
    | BUCKET
    | BUCKETS
    | BY
    | CACHE
    | CASCADE
    | CASE
    | CAST
    | CATALOG
    | CATALOGS
    | CHANGE
    | CHECK
    | CLEAR
    | CLUSTER
    | CLUSTERED
    | CODEGEN
    | COLLATE
    | COLLECTION
    | COLUMN
    | COLUMNS
    | COMMENT
    | COMMIT
    | COMPACT
    | COMPACTIONS
    | COMPUTE
    | CONCATENATE
    | CONSTRAINT
    | COST
    | CREATE
    | CUBE
    | CURRENT
    | CURRENT_DATE
    | CURRENT_TIME
    | CURRENT_TIMESTAMP
    | CURRENT_USER
    | DATA
    | DATABASE
    | DATABASES
    | DATEADD
    | DATEDIFF
    | DAY
    | DAYOFYEAR
    | DBPROPERTIES
    | DEFAULT
    | DEFINED
    | DELETE
    | DELIMITED
    | DESC
    | DESCRIBE
    | DFS
    | DIRECTORIES
    | DIRECTORY
    | DISTINCT
    | DISTRIBUTE
    | DIV
    | DROP
    | ELSE
    | END
    | ESCAPE
    | ESCAPED
    | EXCHANGE
    | EXISTS
    | EXPLAIN
    | EXPORT
    | EXTENDED
    | EXTERNAL
    | EXTRACT
    | FALSE
    | FETCH
    | FILTER
    | FIELDS
    | FILEFORMAT
    | FIRST
    | FOLLOWING
    | FOR
    | FOREIGN
    | FORMAT
    | FORMATTED
    | FROM
    | FUNCTION
    | FUNCTIONS
    | GLOBAL
    | GRANT
    | GROUP
    | GROUPING
    | HAVING
    | HOUR
    | IF
    | IGNORE
    | IMPORT
    | IN
    | INDEX
    | INDEXES
    | INPATH
    | INPUTFORMAT
    | INSERT
    | INTERVAL
    | INTO
    | IS
    | ITEMS
    | KEYS
    | LAST
    | LAZY
    | LEADING
    | LIKE
    | ILIKE
    | LIMIT
    | LINES
    | LIST
    | LOAD
    | LOCAL
    | LOCATION
    | LOCK
    | LOCKS
    | LOGICAL
    | MACRO
    | MAP
    | MATCHED
    | MERGE
    | MICROSECOND
    | MILLISECOND
    | MINUTE
    | MONTH
    | MSCK
    | NAMESPACE
    | NAMESPACES
    | NO
    | NOT
    | NULL
    | NULLS
    | OF
    | OFFSET
    | ONLY
    | OPTION
    | OPTIONS
    | OR
    | ORDER
    | OUT
    | OUTER
    | OUTPUTFORMAT
    | OVER
    | OVERLAPS
    | OVERLAY
    | OVERWRITE
    | PARTITION
    | PARTITIONED
    | PARTITIONS
    | PERCENTILE_CONT
    | PERCENTILE_DISC
    | PERCENTLIT
    | PIVOT
    | PLACING
    | POSITION
    | PRECEDING
    | PRIMARY
    | PRINCIPALS
    | PROPERTIES
    | PURGE
    | QUARTER
    | QUERY
    | RANGE
    | RECORDREADER
    | RECORDWRITER
    | RECOVER
    | REDUCE
    | REFERENCES
    | REFRESH
    | RENAME
    | REPAIR
    | REPEATABLE
    | REPLACE
    | RESET
    | RESPECT
    | RESTRICT
    | REVOKE
    | RLIKE
    | ROLE
    | ROLES
    | ROLLBACK
    | ROLLUP
    | ROW
    | ROWS
    | SCHEMA
    | SCHEMAS
    | SECOND
    | SELECT
    | SEPARATED
    | SERDE
    | SERDEPROPERTIES
    | SESSION_USER
    | SET
    | SETS
    | SHOW
    | SKEWED
    | SOME
    | SORT
    | SORTED
    | START
    | STATISTICS
    | STORED
    | STRATIFY
    | STRUCT
    | SUBSTR
    | SUBSTRING
    | SYNC
    | SYSTEM_TIME
    | SYSTEM_VERSION
    | TABLE
    | TABLES
    | TABLESAMPLE
    | TBLPROPERTIES
    | TEMPORARY
    | TERMINATED
    | THEN
    | TIME
    | TIMESTAMP
    | TIMESTAMPADD
    | TIMESTAMPDIFF
    | TO
    | TOUCH
    | TRAILING
    | TRANSACTION
    | TRANSACTIONS
    | TRANSFORM
    | TRIM
    | TRUE
    | TRUNCATE
    | TRY_CAST
    | TYPE
    | UNARCHIVE
    | UNBOUNDED
    | UNCACHE
    | UNIQUE
    | UNKNOWN
    | UNLOCK
    | UNSET
    | UPDATE
    | USE
    | USER
    | VALUES
    | VERSION
    | VIEW
    | VIEWS
    | WEEK
    | WHEN
    | WHERE
    | WINDOW
    | WITH
    | WITHIN
    | YEAR
    | ZONE
//--DEFAULT-NON-RESERVED-END
    ;
