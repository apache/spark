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

  /**
   * When true, double quoted literals are identifiers rather than STRINGs.
   */
  public boolean double_quoted_identifiers = false;
}

compoundOrSingleStatement
    : singleStatement
    | singleCompoundStatement
    ;

singleCompoundStatement
    : beginEndCompoundBlock SEMICOLON? EOF
    ;

beginEndCompoundBlock
    : beginLabel? BEGIN compoundBody END endLabel?
    ;

compoundBody
    : (compoundStatements+=compoundStatement SEMICOLON)*
    ;

compoundStatement
    : statement
    | setStatementWithOptionalVarKeyword
    | beginEndCompoundBlock
    | ifElseStatement
    | whileStatement
    | repeatStatement
    | leaveStatement
    | iterateStatement
    ;

setStatementWithOptionalVarKeyword
    : SET variable? assignmentList                              #setVariableWithOptionalKeyword
    | SET variable? LEFT_PAREN multipartIdentifierList RIGHT_PAREN EQ
        LEFT_PAREN query RIGHT_PAREN                            #setVariableWithOptionalKeyword
    ;

whileStatement
    : beginLabel? WHILE booleanExpression DO compoundBody END WHILE endLabel?
    ;

ifElseStatement
    : IF booleanExpression THEN conditionalBodies+=compoundBody
        (ELSE IF booleanExpression THEN conditionalBodies+=compoundBody)*
        (ELSE elseBody=compoundBody)? END IF
    ;

repeatStatement
    : beginLabel? REPEAT compoundBody UNTIL booleanExpression END REPEAT endLabel?
    ;

leaveStatement
    : LEAVE multipartIdentifier
    ;

iterateStatement
    : ITERATE multipartIdentifier
    ;

singleStatement
    : (statement|setResetStatement) SEMICOLON* EOF
    ;

beginLabel
    : multipartIdentifier COLON
    ;

endLabel
    : multipartIdentifier
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
    | executeImmediate                                                 #visitExecuteImmediate
    | ctes? dmlStatementNoWith                                         #dmlStatement
    | USE identifierReference                                          #use
    | USE namespace identifierReference                                #useNamespace
    | SET CATALOG (errorCapturingIdentifier | stringLit)                  #setCatalog
    | CREATE namespace (IF errorCapturingNot EXISTS)? identifierReference
        (commentSpec |
         locationSpec |
         (WITH (DBPROPERTIES | PROPERTIES) propertyList))*             #createNamespace
    | ALTER namespace identifierReference
        SET (DBPROPERTIES | PROPERTIES) propertyList                   #setNamespaceProperties
    | ALTER namespace identifierReference
        UNSET (DBPROPERTIES | PROPERTIES) propertyList                 #unsetNamespaceProperties
    | ALTER namespace identifierReference
        SET locationSpec                                               #setNamespaceLocation
    | DROP namespace (IF EXISTS)? identifierReference
        (RESTRICT | CASCADE)?                                          #dropNamespace
    | SHOW namespaces ((FROM | IN) multipartIdentifier)?
        (LIKE? pattern=stringLit)?                                        #showNamespaces
    | createTableHeader (LEFT_PAREN colDefinitionList RIGHT_PAREN)? tableProvider?
        createTableClauses
        (AS? query)?                                                   #createTable
    | CREATE TABLE (IF errorCapturingNot EXISTS)? target=tableIdentifier
        LIKE source=tableIdentifier
        (tableProvider |
        rowFormat |
        createFileFormat |
        locationSpec |
        (TBLPROPERTIES tableProps=propertyList))*                      #createTableLike
    | replaceTableHeader (LEFT_PAREN colDefinitionList RIGHT_PAREN)? tableProvider?
        createTableClauses
        (AS? query)?                                                   #replaceTable
    | ANALYZE TABLE identifierReference partitionSpec? COMPUTE STATISTICS
        (identifier | FOR COLUMNS identifierSeq | FOR ALL COLUMNS)?    #analyze
    | ANALYZE TABLES ((FROM | IN) identifierReference)? COMPUTE STATISTICS
        (identifier)?                                                  #analyzeTables
    | ALTER TABLE identifierReference
        ADD (COLUMN | COLUMNS)
        columns=qualifiedColTypeWithPositionList                       #addTableColumns
    | ALTER TABLE identifierReference
        ADD (COLUMN | COLUMNS)
        LEFT_PAREN columns=qualifiedColTypeWithPositionList RIGHT_PAREN #addTableColumns
    | ALTER TABLE table=identifierReference
        RENAME COLUMN
        from=multipartIdentifier TO to=errorCapturingIdentifier        #renameTableColumn
    | ALTER TABLE identifierReference
        DROP (COLUMN | COLUMNS) (IF EXISTS)?
        LEFT_PAREN columns=multipartIdentifierList RIGHT_PAREN         #dropTableColumns
    | ALTER TABLE identifierReference
        DROP (COLUMN | COLUMNS) (IF EXISTS)?
        columns=multipartIdentifierList                                #dropTableColumns
    | ALTER (TABLE | VIEW) from=identifierReference
        RENAME TO to=multipartIdentifier                               #renameTable
    | ALTER (TABLE | VIEW) identifierReference
        SET TBLPROPERTIES propertyList                                 #setTableProperties
    | ALTER (TABLE | VIEW) identifierReference
        UNSET TBLPROPERTIES (IF EXISTS)? propertyList                  #unsetTableProperties
    | ALTER TABLE table=identifierReference
        (ALTER | CHANGE) COLUMN? column=multipartIdentifier
        alterColumnAction?                                             #alterTableAlterColumn
    | ALTER TABLE table=identifierReference partitionSpec?
        CHANGE COLUMN?
        colName=multipartIdentifier colType colPosition?               #hiveChangeColumn
    | ALTER TABLE table=identifierReference partitionSpec?
        REPLACE COLUMNS
        LEFT_PAREN columns=qualifiedColTypeWithPositionList
        RIGHT_PAREN                                                    #hiveReplaceColumns
    | ALTER TABLE identifierReference (partitionSpec)?
        SET SERDE stringLit (WITH SERDEPROPERTIES propertyList)?       #setTableSerDe
    | ALTER TABLE identifierReference (partitionSpec)?
        SET SERDEPROPERTIES propertyList                               #setTableSerDe
    | ALTER (TABLE | VIEW) identifierReference ADD (IF errorCapturingNot EXISTS)?
        partitionSpecLocation+                                         #addTablePartition
    | ALTER TABLE identifierReference
        from=partitionSpec RENAME TO to=partitionSpec                  #renameTablePartition
    | ALTER (TABLE | VIEW) identifierReference
        DROP (IF EXISTS)? partitionSpec (COMMA partitionSpec)* PURGE?  #dropTablePartitions
    | ALTER TABLE identifierReference
        (partitionSpec)? SET locationSpec                              #setTableLocation
    | ALTER TABLE identifierReference RECOVER PARTITIONS                 #recoverPartitions
    | ALTER TABLE identifierReference
        (clusterBySpec | CLUSTER BY NONE)                              #alterClusterBy
    | DROP TABLE (IF EXISTS)? identifierReference PURGE?               #dropTable
    | DROP VIEW (IF EXISTS)? identifierReference                       #dropView
    | CREATE (OR REPLACE)? (GLOBAL? TEMPORARY)?
        VIEW (IF errorCapturingNot EXISTS)? identifierReference
        identifierCommentList?
        (commentSpec |
         schemaBinding |
         (PARTITIONED ON identifierList) |
         (TBLPROPERTIES propertyList))*
        AS query                                                       #createView
    | CREATE (OR REPLACE)? GLOBAL? TEMPORARY VIEW
        tableIdentifier (LEFT_PAREN colTypeList RIGHT_PAREN)? tableProvider
        (OPTIONS propertyList)?                                        #createTempViewUsing
    | ALTER VIEW identifierReference AS? query                         #alterViewQuery
    | ALTER VIEW identifierReference schemaBinding                     #alterViewSchemaBinding
    | CREATE (OR REPLACE)? TEMPORARY? FUNCTION (IF errorCapturingNot EXISTS)?
        identifierReference AS className=stringLit
        (USING resource (COMMA resource)*)?                            #createFunction
    | CREATE (OR REPLACE)? TEMPORARY? FUNCTION (IF errorCapturingNot EXISTS)?
        identifierReference LEFT_PAREN parameters=colDefinitionList? RIGHT_PAREN
        (RETURNS (dataType | TABLE LEFT_PAREN returnParams=colTypeList RIGHT_PAREN))?
        routineCharacteristics
        RETURN (query | expression)                                    #createUserDefinedFunction
    | DROP TEMPORARY? FUNCTION (IF EXISTS)? identifierReference        #dropFunction
    | DECLARE (OR REPLACE)? variable?
        identifierReference dataType? variableDefaultExpression?       #createVariable
    | DROP TEMPORARY variable (IF EXISTS)? identifierReference         #dropVariable
    | EXPLAIN (LOGICAL | FORMATTED | EXTENDED | CODEGEN | COST)?
        (statement|setResetStatement)                                  #explain
    | SHOW TABLES ((FROM | IN) identifierReference)?
        (LIKE? pattern=stringLit)?                                        #showTables
    | SHOW TABLE EXTENDED ((FROM | IN) ns=identifierReference)?
        LIKE pattern=stringLit partitionSpec?                             #showTableExtended
    | SHOW TBLPROPERTIES table=identifierReference
        (LEFT_PAREN key=propertyKey RIGHT_PAREN)?                      #showTblProperties
    | SHOW COLUMNS (FROM | IN) table=identifierReference
        ((FROM | IN) ns=multipartIdentifier)?                          #showColumns
    | SHOW VIEWS ((FROM | IN) identifierReference)?
        (LIKE? pattern=stringLit)?                                        #showViews
    | SHOW PARTITIONS identifierReference partitionSpec?               #showPartitions
    | SHOW identifier? FUNCTIONS ((FROM | IN) ns=identifierReference)?
        (LIKE? (legacy=multipartIdentifier | pattern=stringLit))?      #showFunctions
    | SHOW CREATE TABLE identifierReference (AS SERDE)?                #showCreateTable
    | SHOW CURRENT namespace                                           #showCurrentNamespace
    | SHOW CATALOGS (LIKE? pattern=stringLit)?                            #showCatalogs
    | (DESC | DESCRIBE) FUNCTION EXTENDED? describeFuncName            #describeFunction
    | (DESC | DESCRIBE) namespace EXTENDED?
        identifierReference                                            #describeNamespace
    | (DESC | DESCRIBE) TABLE? option=(EXTENDED | FORMATTED)?
        identifierReference partitionSpec? describeColName?            #describeRelation
    | (DESC | DESCRIBE) QUERY? query                                   #describeQuery
    | COMMENT ON namespace identifierReference IS
        comment                                                        #commentNamespace
    | COMMENT ON TABLE identifierReference IS comment                  #commentTable
    | REFRESH TABLE identifierReference                                #refreshTable
    | REFRESH FUNCTION identifierReference                             #refreshFunction
    | REFRESH (stringLit | .*?)                                        #refreshResource
    | CACHE LAZY? TABLE identifierReference
        (OPTIONS options=propertyList)? (AS? query)?                   #cacheTable
    | UNCACHE TABLE (IF EXISTS)? identifierReference                   #uncacheTable
    | CLEAR CACHE                                                      #clearCache
    | LOAD DATA LOCAL? INPATH path=stringLit OVERWRITE? INTO TABLE
        identifierReference partitionSpec?                             #loadData
    | TRUNCATE TABLE identifierReference partitionSpec?                #truncateTable
    | (MSCK)? REPAIR TABLE identifierReference
        (option=(ADD|DROP|SYNC) PARTITIONS)?                           #repairTable
    | op=(ADD | LIST) identifier .*?                                   #manageResource
    | CREATE INDEX (IF errorCapturingNot EXISTS)? identifier ON TABLE?
        identifierReference (USING indexType=identifier)?
        LEFT_PAREN columns=multipartIdentifierPropertyList RIGHT_PAREN
        (OPTIONS options=propertyList)?                                #createIndex
    | DROP INDEX (IF EXISTS)? identifier ON TABLE? identifierReference #dropIndex
    | unsupportedHiveNativeCommands .*?                                #failNativeCommand
    ;

setResetStatement
    : SET COLLATION collationName=identifier                           #setCollation
    | SET ROLE .*?                                                     #failSetRole
    | SET TIME ZONE interval                                           #setTimeZone
    | SET TIME ZONE timezone                                           #setTimeZone
    | SET TIME ZONE .*?                                                #setTimeZone
    | SET variable assignmentList                                      #setVariable
    | SET variable LEFT_PAREN multipartIdentifierList RIGHT_PAREN EQ
        LEFT_PAREN query RIGHT_PAREN                                   #setVariable
    | SET configKey EQ configValue                                     #setQuotedConfiguration
    | SET configKey (EQ .*?)?                                          #setConfiguration
    | SET .*? EQ configValue                                           #setQuotedConfiguration
    | SET .*?                                                          #setConfiguration
    | RESET configKey                                                  #resetQuotedConfiguration
    | RESET .*?                                                        #resetConfiguration
    ;

executeImmediate
    : EXECUTE IMMEDIATE queryParam=executeImmediateQueryParam (INTO targetVariable=multipartIdentifierList)? executeImmediateUsing?
    ;

executeImmediateUsing
    : USING LEFT_PAREN params=namedExpressionSeq RIGHT_PAREN
    | USING params=namedExpressionSeq
    ;

executeImmediateQueryParam
    : stringLit
    | multipartIdentifier
    ;

executeImmediateArgument
    : (constant|multipartIdentifier) (AS name=errorCapturingIdentifier)?
    ;

executeImmediateArgumentSeq
    : executeImmediateArgument (COMMA executeImmediateArgument)*
    ;

timezone
    : stringLit
    | LOCAL
    ;

configKey
    : quotedIdentifier
    ;

configValue
    : backQuotedIdentifier
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
    : CREATE TEMPORARY? EXTERNAL? TABLE (IF errorCapturingNot EXISTS)? identifierReference
    ;

replaceTableHeader
    : (CREATE OR)? REPLACE TABLE identifierReference
    ;

clusterBySpec
    : CLUSTER BY LEFT_PAREN multipartIdentifierList RIGHT_PAREN
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
    : LOCATION stringLit
    ;

schemaBinding
    : WITH SCHEMA (BINDING | COMPENSATION | EVOLUTION | TYPE EVOLUTION)
    ;

commentSpec
    : COMMENT stringLit
    ;

query
    : ctes? queryTerm queryOrganization
    ;

insertInto
    : INSERT OVERWRITE TABLE? identifierReference optionsClause? (partitionSpec (IF errorCapturingNot EXISTS)?)?  ((BY NAME) | identifierList)? #insertOverwriteTable
    | INSERT INTO TABLE? identifierReference optionsClause? partitionSpec? (IF errorCapturingNot EXISTS)? ((BY NAME) | identifierList)?   #insertIntoTable
    | INSERT INTO TABLE? identifierReference optionsClause? REPLACE whereClause                                             #insertIntoReplaceWhere
    | INSERT OVERWRITE LOCAL? DIRECTORY path=stringLit rowFormat? createFileFormat?                     #insertOverwriteHiveDir
    | INSERT OVERWRITE LOCAL? DIRECTORY (path=stringLit)? tableProvider (OPTIONS options=propertyList)? #insertOverwriteDir
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

variable
    : VARIABLE
    | VAR
    ;

describeFuncName
    : identifierReference
    | stringLit
    | comparisonOperator
    | arithmeticOperator
    | predicateOperator
    | shiftOperator
    | BANG
    ;

describeColName
    : nameParts+=errorCapturingIdentifier (DOT nameParts+=errorCapturingIdentifier)*
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
    :((OPTIONS options=expressionPropertyList) |
     (PARTITIONED BY partitioning=partitionFieldList) |
     skewSpec |
     clusterBySpec |
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
    : errorCapturingIdentifier (DOT errorCapturingIdentifier)*
    | stringLit
    ;

propertyValue
    : INTEGER_VALUE
    | DECIMAL_VALUE
    | booleanValue
    | stringLit
    ;

expressionPropertyList
    : LEFT_PAREN expressionProperty (COMMA expressionProperty)* RIGHT_PAREN
    ;

expressionProperty
    : key=propertyKey (EQ? value=expression)?
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
    : INPUTFORMAT inFmt=stringLit OUTPUTFORMAT outFmt=stringLit    #tableFileFormat
    | identifier                                             #genericFileFormat
    ;

storageHandler
    : stringLit (WITH SERDEPROPERTIES propertyList)?
    ;

resource
    : identifier stringLit
    ;

dmlStatementNoWith
    : insertInto query                                                             #singleInsertQuery
    | fromClause multiInsertQueryBody+                                             #multiInsertQuery
    | DELETE FROM identifierReference tableAlias whereClause?                      #deleteFromTable
    | UPDATE identifierReference tableAlias setClause whereClause?                 #updateTable
    | MERGE (WITH SCHEMA EVOLUTION)? INTO target=identifierReference targetAlias=tableAlias
        USING (source=identifierReference |
          LEFT_PAREN sourceQuery=query RIGHT_PAREN) sourceAlias=tableAlias
        ON mergeCondition=booleanExpression
        matchedClause*
        notMatchedClause*
        notMatchedBySourceClause*                                                  #mergeIntoTable
    ;

identifierReference
    : IDENTIFIER_KW LEFT_PAREN expression RIGHT_PAREN
    | multipartIdentifier
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
    | left=queryTerm OPERATOR_PIPE operatorPipeRightSide                                 #operatorPipeStatement
    ;

queryPrimary
    : querySpecification                                                    #queryPrimaryDefault
    | fromStatement                                                         #fromStmt
    | TABLE identifierReference                                             #table
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
      (RECORDWRITER recordWriter=stringLit)?
      USING script=stringLit
      (AS (identifierSeq | colTypeList | (LEFT_PAREN (identifierSeq | colTypeList) RIGHT_PAREN)))?
      outRowFormat=rowFormat?
      (RECORDREADER recordReader=stringLit)?
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
    : WHEN errorCapturingNot MATCHED (BY TARGET)? (AND notMatchedCond=booleanExpression)? THEN notMatchedAction
    ;

notMatchedBySourceClause
    : WHEN errorCapturingNot MATCHED BY SOURCE (AND notMatchedBySourceCond=booleanExpression)? THEN notMatchedBySourceAction
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

notMatchedBySourceAction
    : DELETE
    | UPDATE SET assignmentList
    ;

exceptClause
    : EXCEPT LEFT_PAREN exceptCols=multipartIdentifierList RIGHT_PAREN
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
    : FROM relation (COMMA relation)* lateralView* pivotClause? unpivotClause?
    ;

temporalClause
    : FOR? (SYSTEM_VERSION | VERSION) AS OF version
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
    : identifiers+=errorCapturingIdentifier
    | LEFT_PAREN identifiers+=errorCapturingIdentifier (COMMA identifiers+=errorCapturingIdentifier)* RIGHT_PAREN
    ;

pivotValue
    : expression (AS? errorCapturingIdentifier)?
    ;

unpivotClause
    : UNPIVOT nullOperator=unpivotNullClause? LEFT_PAREN
        operator=unpivotOperator
      RIGHT_PAREN (AS? errorCapturingIdentifier)?
    ;

unpivotNullClause
    : (INCLUDE | EXCLUDE) NULLS
    ;

unpivotOperator
    : (unpivotSingleValueColumnClause | unpivotMultiValueColumnClause)
    ;

unpivotSingleValueColumnClause
    : unpivotValueColumn FOR unpivotNameColumn IN LEFT_PAREN unpivotColumns+=unpivotColumnAndAlias (COMMA unpivotColumns+=unpivotColumnAndAlias)* RIGHT_PAREN
    ;

unpivotMultiValueColumnClause
    : LEFT_PAREN unpivotValueColumns+=unpivotValueColumn (COMMA unpivotValueColumns+=unpivotValueColumn)* RIGHT_PAREN
      FOR unpivotNameColumn
      IN LEFT_PAREN unpivotColumnSets+=unpivotColumnSet (COMMA unpivotColumnSets+=unpivotColumnSet)* RIGHT_PAREN
    ;

unpivotColumnSet
    : LEFT_PAREN unpivotColumns+=unpivotColumn (COMMA unpivotColumns+=unpivotColumn)* RIGHT_PAREN unpivotAlias?
    ;

unpivotValueColumn
    : identifier
    ;

unpivotNameColumn
    : identifier
    ;

unpivotColumnAndAlias
    : unpivotColumn unpivotAlias?
    ;

unpivotColumn
    : multipartIdentifier
    ;

unpivotAlias
    : AS? errorCapturingIdentifier
    ;

lateralView
    : LATERAL VIEW (OUTER)? qualifiedName LEFT_PAREN (expression (COMMA expression)*)? RIGHT_PAREN tblName=identifier (AS? colName+=identifier (COMMA colName+=identifier)*)?
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

relation
    : LATERAL? relationPrimary relationExtension*
    ;

relationExtension
    : joinRelation
    | pivotClause
    | unpivotClause
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
    : identifierReference temporalClause?
      optionsClause? sample? tableAlias                     #tableName
    | LEFT_PAREN query RIGHT_PAREN sample? tableAlias       #aliasedQuery
    | LEFT_PAREN relation RIGHT_PAREN sample? tableAlias    #aliasedRelation
    | inlineTable                                           #inlineTableDefault2
    | functionTable                                         #tableValuedFunction
    ;

optionsClause
    : WITH options=propertyList
    ;

inlineTable
    : VALUES expression (COMMA expression)* tableAlias
    ;

functionTableSubqueryArgument
    : TABLE identifierReference tableArgumentPartitioning?
    | TABLE LEFT_PAREN identifierReference RIGHT_PAREN tableArgumentPartitioning?
    | TABLE LEFT_PAREN query RIGHT_PAREN tableArgumentPartitioning?
    ;

tableArgumentPartitioning
    : ((WITH SINGLE PARTITION)
        | ((PARTITION | DISTRIBUTE) BY
            (((LEFT_PAREN partition+=expression (COMMA partition+=expression)* RIGHT_PAREN))
            | (expression (COMMA invalidMultiPartitionExpression=expression)+)
            | partition+=expression)))
      ((ORDER | SORT) BY
        (((LEFT_PAREN sortItem (COMMA sortItem)* RIGHT_PAREN)
        | (sortItem (COMMA invalidMultiSortItem=sortItem)+)
        | sortItem)))?
    ;

functionTableNamedArgumentExpression
    : key=identifier FAT_ARROW table=functionTableSubqueryArgument
    ;

functionTableReferenceArgument
    : functionTableSubqueryArgument
    | functionTableNamedArgumentExpression
    ;

functionTableArgument
    : functionTableReferenceArgument
    | functionArgument
    ;

functionTable
    : funcName=functionName LEFT_PAREN
      (functionTableArgument (COMMA functionTableArgument)*)?
      RIGHT_PAREN tableAlias
    ;

tableAlias
    : (AS? strictIdentifier identifierList?)?
    ;

rowFormat
    : ROW FORMAT SERDE name=stringLit (WITH SERDEPROPERTIES props=propertyList)?       #rowFormatSerde
    | ROW FORMAT DELIMITED
      (FIELDS TERMINATED BY fieldsTerminatedBy=stringLit (ESCAPED BY escapedBy=stringLit)?)?
      (COLLECTION ITEMS TERMINATED BY collectionItemsTerminatedBy=stringLit)?
      (MAP KEYS TERMINATED BY keysTerminatedBy=stringLit)?
      (LINES TERMINATED BY linesSeparatedBy=stringLit)?
      (NULL DEFINED AS nullDefinedAs=stringLit)?                                       #rowFormatDelimited
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

namedArgumentExpression
    : key=identifier FAT_ARROW value=expression
    ;

functionArgument
    : expression
    | namedArgumentExpression
    ;

expressionSeq
    : expression (COMMA expression)*
    ;

booleanExpression
    : (NOT | BANG) booleanExpression                               #logicalNot
    | EXISTS LEFT_PAREN query RIGHT_PAREN                          #exists
    | valueExpression predicate?                                   #predicated
    | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
    ;

predicate
    : errorCapturingNot? kind=BETWEEN lower=valueExpression AND upper=valueExpression
    | errorCapturingNot? kind=IN LEFT_PAREN expression (COMMA expression)* RIGHT_PAREN
    | errorCapturingNot? kind=IN LEFT_PAREN query RIGHT_PAREN
    | errorCapturingNot? kind=RLIKE pattern=valueExpression
    | errorCapturingNot? kind=(LIKE | ILIKE) quantifier=(ANY | SOME | ALL) (LEFT_PAREN RIGHT_PAREN | LEFT_PAREN expression (COMMA expression)* RIGHT_PAREN)
    | errorCapturingNot? kind=(LIKE | ILIKE) pattern=valueExpression (ESCAPE escapeChar=stringLit)?
    | IS errorCapturingNot? kind=NULL
    | IS errorCapturingNot? kind=(TRUE | FALSE | UNKNOWN)
    | IS errorCapturingNot? kind=DISTINCT FROM right=valueExpression
    ;

errorCapturingNot
    : NOT
    | BANG
    ;

valueExpression
    : primaryExpression                                                                      #valueExpressionDefault
    | operator=(MINUS | PLUS | TILDE) valueExpression                                        #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT | DIV) right=valueExpression #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS | CONCAT_PIPE) right=valueExpression       #arithmeticBinary
    | left=valueExpression shiftOperator right=valueExpression                               #shiftExpression
    | left=valueExpression operator=AMPERSAND right=valueExpression                          #arithmeticBinary
    | left=valueExpression operator=HAT right=valueExpression                                #arithmeticBinary
    | left=valueExpression operator=PIPE right=valueExpression                               #arithmeticBinary
    | left=valueExpression comparisonOperator right=valueExpression                          #comparison
    ;

shiftOperator
    : SHIFT_LEFT
    | SHIFT_RIGHT
    | SHIFT_RIGHT_UNSIGNED
    ;

datetimeUnit
    : YEAR | QUARTER | MONTH
    | WEEK | DAY | DAYOFYEAR
    | HOUR | MINUTE | SECOND | MILLISECOND | MICROSECOND
    ;

primaryExpression
    : name=(CURRENT_DATE | CURRENT_TIMESTAMP | CURRENT_USER | USER | SESSION_USER)             #currentLike
    | name=(TIMESTAMPADD | DATEADD | DATE_ADD) LEFT_PAREN (unit=datetimeUnit | invalidUnit=stringLit) COMMA unitsAmount=valueExpression COMMA timestamp=valueExpression RIGHT_PAREN             #timestampadd
    | name=(TIMESTAMPDIFF | DATEDIFF | DATE_DIFF | TIMEDIFF) LEFT_PAREN (unit=datetimeUnit | invalidUnit=stringLit) COMMA startTimestamp=valueExpression COMMA endTimestamp=valueExpression RIGHT_PAREN    #timestampdiff
    | CASE whenClause+ (ELSE elseExpression=expression)? END                                   #searchedCase
    | CASE value=expression whenClause+ (ELSE elseExpression=expression)? END                  #simpleCase
    | name=(CAST | TRY_CAST) LEFT_PAREN expression AS dataType RIGHT_PAREN                     #cast
    | primaryExpression collateClause                                                      #collate
    | primaryExpression DOUBLE_COLON dataType                                                  #castByColon
    | STRUCT LEFT_PAREN (argument+=namedExpression (COMMA argument+=namedExpression)*)? RIGHT_PAREN #struct
    | FIRST LEFT_PAREN expression (IGNORE NULLS)? RIGHT_PAREN                                  #first
    | ANY_VALUE LEFT_PAREN expression (IGNORE NULLS)? RIGHT_PAREN                              #any_value
    | LAST LEFT_PAREN expression (IGNORE NULLS)? RIGHT_PAREN                                   #last
    | POSITION LEFT_PAREN substr=valueExpression IN str=valueExpression RIGHT_PAREN            #position
    | constant                                                                                 #constantDefault
    | ASTERISK exceptClause?                                                                   #star
    | qualifiedName DOT ASTERISK exceptClause?                                                 #star
    | LEFT_PAREN namedExpression (COMMA namedExpression)+ RIGHT_PAREN                          #rowConstructor
    | LEFT_PAREN query RIGHT_PAREN                                                             #subqueryExpression
    | functionName LEFT_PAREN (setQuantifier? argument+=functionArgument
       (COMMA argument+=functionArgument)*)? RIGHT_PAREN
       (WITHIN GROUP LEFT_PAREN ORDER BY sortItem (COMMA sortItem)* RIGHT_PAREN)?
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
    ;

literalType
    : DATE
    | TIMESTAMP | TIMESTAMP_LTZ | TIMESTAMP_NTZ
    | INTERVAL
    | BINARY_HEX
    | unsupportedType=identifier
    ;

constant
    : NULL                                                                                     #nullLiteral
    | QUESTION                                                                                 #posParameterLiteral
    | COLON identifier                                                                         #namedParameterLiteral
    | interval                                                                                 #intervalLiteral
    | literalType stringLit                                                                    #typeConstructor
    | number                                                                                   #numericLiteral
    | booleanValue                                                                             #booleanLiteral
    | stringLit+                                                                               #stringLiteral
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
    : INTERVAL (errorCapturingMultiUnitsInterval | errorCapturingUnitToUnitInterval)
    ;

errorCapturingMultiUnitsInterval
    : body=multiUnitsInterval unitToUnitInterval?
    ;

multiUnitsInterval
    : (intervalValue unit+=unitInMultiUnits)+
    ;

errorCapturingUnitToUnitInterval
    : body=unitToUnitInterval (error1=multiUnitsInterval | error2=unitToUnitInterval)?
    ;

unitToUnitInterval
    : value=intervalValue from=unitInUnitToUnit TO to=unitInUnitToUnit
    ;

intervalValue
    : (PLUS | MINUS)?
      (INTEGER_VALUE | DECIMAL_VALUE | stringLit)
    ;

unitInMultiUnits
    : NANOSECOND | NANOSECONDS | MICROSECOND | MICROSECONDS | MILLISECOND | MILLISECONDS
    | SECOND | SECONDS | MINUTE | MINUTES | HOUR | HOURS | DAY | DAYS | WEEK | WEEKS
    | MONTH | MONTHS | YEAR | YEARS
    ;

unitInUnitToUnit
    : SECOND | MINUTE | HOUR | DAY | MONTH | YEAR
    ;

colPosition
    : position=FIRST | position=AFTER afterCol=errorCapturingIdentifier
    ;

collateClause
    : COLLATE collationName=identifier
    ;

type
    : BOOLEAN
    | TINYINT | BYTE
    | SMALLINT | SHORT
    | INT | INTEGER
    | BIGINT | LONG
    | FLOAT | REAL
    | DOUBLE
    | DATE
    | TIMESTAMP | TIMESTAMP_NTZ | TIMESTAMP_LTZ
    | STRING collateClause?
    | CHARACTER | CHAR
    | VARCHAR
    | BINARY
    | DECIMAL | DEC | NUMERIC
    | VOID
    | INTERVAL
    | VARIANT
    | ARRAY | STRUCT | MAP
    | unsupportedType=identifier
    ;

dataType
    : complex=ARRAY LT dataType GT                              #complexDataType
    | complex=MAP LT dataType COMMA dataType GT                 #complexDataType
    | complex=STRUCT (LT complexColTypeList? GT | NEQ)          #complexDataType
    | INTERVAL from=(YEAR | MONTH) (TO to=MONTH)?               #yearMonthIntervalDataType
    | INTERVAL from=(DAY | HOUR | MINUTE | SECOND)
      (TO to=(HOUR | MINUTE | SECOND))?                         #dayTimeIntervalDataType
    | type (LEFT_PAREN INTEGER_VALUE
      (COMMA INTEGER_VALUE)* RIGHT_PAREN)?                      #primitiveDataType
    ;

qualifiedColTypeWithPositionList
    : qualifiedColTypeWithPosition (COMMA qualifiedColTypeWithPosition)*
    ;

qualifiedColTypeWithPosition
    : name=multipartIdentifier dataType colDefinitionDescriptorWithPosition*
    ;

colDefinitionDescriptorWithPosition
    : errorCapturingNot NULL
    | defaultExpression
    | commentSpec
    | colPosition
    ;

defaultExpression
    : DEFAULT expression
    ;

variableDefaultExpression
    : (DEFAULT | EQ) expression
    ;

colTypeList
    : colType (COMMA colType)*
    ;

colType
    : colName=errorCapturingIdentifier dataType (errorCapturingNot NULL)? commentSpec?
    ;

colDefinitionList
    : colDefinition (COMMA colDefinition)*
    ;

colDefinition
    : colName=errorCapturingIdentifier dataType colDefinitionOption*
    ;

colDefinitionOption
    : errorCapturingNot NULL
    | defaultExpression
    | generationExpression
    | commentSpec
    ;

generationExpression
    : GENERATED ALWAYS AS LEFT_PAREN expression RIGHT_PAREN
    ;

complexColTypeList
    : complexColType (COMMA complexColType)*
    ;

complexColType
    : errorCapturingIdentifier COLON? dataType (errorCapturingNot NULL)? commentSpec?
    ;

routineCharacteristics
    : (routineLanguage
    | specificName
    | deterministic
    | sqlDataAccess
    | nullCall
    | commentSpec
    | rightsClause)*
    ;

routineLanguage
    : LANGUAGE (SQL | IDENTIFIER)
    ;

specificName
    : SPECIFIC specific=errorCapturingIdentifier
    ;

deterministic
    : DETERMINISTIC
    | errorCapturingNot DETERMINISTIC
    ;

sqlDataAccess
    : access=NO SQL
    | access=CONTAINS SQL
    | access=READS SQL DATA
    | access=MODIFIES SQL DATA
    ;

nullCall
    : RETURNS NULL ON NULL INPUT
    | CALLED ON NULL INPUT
    ;

rightsClause
    : SQL SECURITY INVOKER
    | SQL SECURITY DEFINER
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
    : IDENTIFIER_KW LEFT_PAREN expression RIGHT_PAREN
    | identFunc=IDENTIFIER_KW   // IDENTIFIER itself is also a valid function name.
    | qualifiedName
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
    | {double_quoted_identifiers}? DOUBLEQUOTED_STRING
    ;

backQuotedIdentifier
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
    | setOrDrop=(SET | DROP) errorCapturingNot NULL
    | SET defaultExpression
    | dropDefault=DROP DEFAULT
    ;

stringLit
    : STRING_LITERAL
    | {!double_quoted_identifiers}? DOUBLEQUOTED_STRING
    ;

comment
    : stringLit
    | NULL
    ;

version
    : INTEGER_VALUE
    | stringLit
    ;

operatorPipeRightSide
    : selectClause
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
    | ALWAYS
    | ANALYZE
    | ANTI
    | ANY_VALUE
    | ARCHIVE
    | ARRAY
    | ASC
    | AT
    | BEGIN
    | BETWEEN
    | BIGINT
    | BINARY
    | BINARY_HEX
    | BINDING
    | BOOLEAN
    | BUCKET
    | BUCKETS
    | BY
    | BYTE
    | CACHE
    | CALLED
    | CASCADE
    | CATALOG
    | CATALOGS
    | CHANGE
    | CHAR
    | CHARACTER
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
    | COMPENSATION
    | COMPUTE
    | CONCATENATE
    | CONTAINS
    | COST
    | CUBE
    | CURRENT
    | DATA
    | DATABASE
    | DATABASES
    | DATE
    | DATEADD
    | DATE_ADD
    | DATEDIFF
    | DATE_DIFF
    | DAY
    | DAYS
    | DAYOFYEAR
    | DBPROPERTIES
    | DEC
    | DECIMAL
    | DECLARE
    | DEFAULT
    | DEFINED
    | DEFINER
    | DELETE
    | DELIMITED
    | DESC
    | DESCRIBE
    | DETERMINISTIC
    | DFS
    | DIRECTORIES
    | DIRECTORY
    | DISTRIBUTE
    | DIV
    | DO
    | DOUBLE
    | DROP
    | ESCAPED
    | EVOLUTION
    | EXCHANGE
    | EXCLUDE
    | EXISTS
    | EXPLAIN
    | EXPORT
    | EXTENDED
    | EXTERNAL
    | EXTRACT
    | FIELDS
    | FILEFORMAT
    | FIRST
    | FLOAT
    | FOLLOWING
    | FORMAT
    | FORMATTED
    | FUNCTION
    | FUNCTIONS
    | GENERATED
    | GLOBAL
    | GROUPING
    | HOUR
    | HOURS
    | IDENTIFIER_KW
    | IF
    | IGNORE
    | IMMEDIATE
    | IMPORT
    | INCLUDE
    | INDEX
    | INDEXES
    | INPATH
    | INPUT
    | INPUTFORMAT
    | INSERT
    | INT
    | INTEGER
    | INTERVAL
    | INVOKER
    | ITEMS
    | ITERATE
    | KEYS
    | LANGUAGE
    | LAST
    | LAZY
    | LEAVE
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
    | LONG
    | MACRO
    | MAP
    | MATCHED
    | MERGE
    | MICROSECOND
    | MICROSECONDS
    | MILLISECOND
    | MILLISECONDS
    | MINUTE
    | MINUTES
    | MODIFIES
    | MONTH
    | MONTHS
    | MSCK
    | NAME
    | NAMESPACE
    | NAMESPACES
    | NANOSECOND
    | NANOSECONDS
    | NO
    | NONE
    | NULLS
    | NUMERIC
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
    | READS
    | REAL
    | RECORDREADER
    | RECORDWRITER
    | RECOVER
    | REDUCE
    | REFRESH
    | RENAME
    | REPAIR
    | REPEAT
    | REPEATABLE
    | REPLACE
    | RESET
    | RESPECT
    | RESTRICT
    | RETURN
    | RETURNS
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
    | SECONDS
    | SECURITY
    | SEMI
    | SEPARATED
    | SERDE
    | SERDEPROPERTIES
    | SET
    | SETMINUS
    | SETS
    | SHORT
    | SHOW
    | SINGLE
    | SKEWED
    | SMALLINT
    | SORT
    | SORTED
    | SOURCE
    | SPECIFIC
    | START
    | STATISTICS
    | STORED
    | STRATIFY
    | STRING
    | STRUCT
    | SUBSTR
    | SUBSTRING
    | SYNC
    | SYSTEM_TIME
    | SYSTEM_VERSION
    | TABLES
    | TABLESAMPLE
    | TARGET
    | TBLPROPERTIES
    | TEMPORARY
    | TERMINATED
    | TIMEDIFF
    | TIMESTAMP
    | TIMESTAMP_LTZ
    | TIMESTAMP_NTZ
    | TIMESTAMPADD
    | TIMESTAMPDIFF
    | TINYINT
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
    | UNPIVOT
    | UNSET
    | UNTIL
    | UPDATE
    | USE
    | VALUES
    | VARCHAR
    | VAR
    | VARIABLE
    | VARIANT
    | VERSION
    | VIEW
    | VIEWS
    | VOID
    | WEEK
    | WEEKS
    | WHILE
    | WINDOW
    | YEAR
    | YEARS
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
    | ALWAYS
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
    | BEGIN
    | BETWEEN
    | BIGINT
    | BINARY
    | BINARY_HEX
    | BINDING
    | BOOLEAN
    | BOTH
    | BUCKET
    | BUCKETS
    | BY
    | BYTE
    | CACHE
    | CALLED
    | CASCADE
    | CASE
    | CAST
    | CATALOG
    | CATALOGS
    | CHANGE
    | CHAR
    | CHARACTER
    | CHECK
    | CLEAR
    | CLUSTER
    | CLUSTERED
    | CODEGEN
    | COLLATE
    | COLLATION
    | COLLECTION
    | COLUMN
    | COLUMNS
    | COMMENT
    | COMMIT
    | COMPACT
    | COMPACTIONS
    | COMPENSATION
    | COMPUTE
    | CONCATENATE
    | CONSTRAINT
    | CONTAINS
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
    | DATE
    | DATEADD
    | DATE_ADD
    | DATEDIFF
    | DATE_DIFF
    | DAY
    | DAYS
    | DAYOFYEAR
    | DBPROPERTIES
    | DEC
    | DECIMAL
    | DECLARE
    | DEFAULT
    | DEFINED
    | DEFINER
    | DELETE
    | DELIMITED
    | DESC
    | DESCRIBE
    | DETERMINISTIC
    | DFS
    | DIRECTORIES
    | DIRECTORY
    | DISTINCT
    | DISTRIBUTE
    | DIV
    | DO
    | DOUBLE
    | DROP
    | ELSE
    | END
    | ESCAPE
    | ESCAPED
    | EVOLUTION
    | EXCHANGE
    | EXCLUDE
    | EXECUTE
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
    | FLOAT
    | FOLLOWING
    | FOR
    | FOREIGN
    | FORMAT
    | FORMATTED
    | FROM
    | FUNCTION
    | FUNCTIONS
    | GENERATED
    | GLOBAL
    | GRANT
    | GROUP
    | GROUPING
    | HAVING
    | HOUR
    | HOURS
    | IDENTIFIER_KW
    | IF
    | IGNORE
    | IMMEDIATE
    | IMPORT
    | IN
    | INCLUDE
    | INDEX
    | INDEXES
    | INPATH
    | INPUT
    | INPUTFORMAT
    | INSERT
    | INT
    | INTEGER
    | INTERVAL
    | INTO
    | INVOKER
    | IS
    | ITEMS
    | ITERATE
    | KEYS
    | LANGUAGE
    | LAST
    | LAZY
    | LEADING
    | LEAVE
    | LIKE
    | LONG
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
    | LONG
    | MACRO
    | MAP
    | MATCHED
    | MERGE
    | MICROSECOND
    | MICROSECONDS
    | MILLISECOND
    | MILLISECONDS
    | MINUTE
    | MINUTES
    | MODIFIES
    | MONTH
    | MONTHS
    | MSCK
    | NAME
    | NAMESPACE
    | NAMESPACES
    | NANOSECOND
    | NANOSECONDS
    | NO
    | NONE
    | NOT
    | NULL
    | NULLS
    | NUMERIC
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
    | READS
    | REAL
    | RECORDREADER
    | RECORDWRITER
    | RECOVER
    | REDUCE
    | REFERENCES
    | REFRESH
    | RENAME
    | REPAIR
    | REPEAT
    | REPEATABLE
    | REPLACE
    | RESET
    | RESPECT
    | RESTRICT
    | RETURN
    | RETURNS
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
    | SECONDS
    | SECURITY
    | SELECT
    | SEPARATED
    | SERDE
    | SERDEPROPERTIES
    | SESSION_USER
    | SET
    | SETS
    | SHORT
    | SHOW
    | SINGLE
    | SKEWED
    | SMALLINT
    | SOME
    | SORT
    | SORTED
    | SOURCE
    | SPECIFIC
    | SQL
    | START
    | STATISTICS
    | STORED
    | STRATIFY
    | STRING
    | STRUCT
    | SUBSTR
    | SUBSTRING
    | SYNC
    | SYSTEM_TIME
    | SYSTEM_VERSION
    | TABLE
    | TABLES
    | TABLESAMPLE
    | TARGET
    | TBLPROPERTIES
    | TEMPORARY
    | TERMINATED
    | THEN
    | TIME
    | TIMEDIFF
    | TIMESTAMP
    | TIMESTAMP_LTZ
    | TIMESTAMP_NTZ
    | TIMESTAMPADD
    | TIMESTAMPDIFF
    | TINYINT
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
    | UNPIVOT
    | UNSET
    | UNTIL
    | UPDATE
    | USE
    | USER
    | VALUES
    | VARCHAR
    | VAR
    | VARIABLE
    | VARIANT
    | VERSION
    | VIEW
    | VIEWS
    | VOID
    | WEEK
    | WEEKS
    | WHILE
    | WHEN
    | WHERE
    | WINDOW
    | WITH
    | WITHIN
    | YEAR
    | YEARS
    | ZONE
//--DEFAULT-NON-RESERVED-END
    ;
