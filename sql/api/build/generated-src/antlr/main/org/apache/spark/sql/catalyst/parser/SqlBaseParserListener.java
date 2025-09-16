// Generated from SqlBaseParser.g4 by ANTLR 4.9.3
package org.apache.spark.sql.catalyst.parser;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link SqlBaseParser}.
 */
public interface SqlBaseParserListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#compoundOrSingleStatement}.
	 * @param ctx the parse tree
	 */
	void enterCompoundOrSingleStatement(SqlBaseParser.CompoundOrSingleStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#compoundOrSingleStatement}.
	 * @param ctx the parse tree
	 */
	void exitCompoundOrSingleStatement(SqlBaseParser.CompoundOrSingleStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#singleCompoundStatement}.
	 * @param ctx the parse tree
	 */
	void enterSingleCompoundStatement(SqlBaseParser.SingleCompoundStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#singleCompoundStatement}.
	 * @param ctx the parse tree
	 */
	void exitSingleCompoundStatement(SqlBaseParser.SingleCompoundStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#beginEndCompoundBlock}.
	 * @param ctx the parse tree
	 */
	void enterBeginEndCompoundBlock(SqlBaseParser.BeginEndCompoundBlockContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#beginEndCompoundBlock}.
	 * @param ctx the parse tree
	 */
	void exitBeginEndCompoundBlock(SqlBaseParser.BeginEndCompoundBlockContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#compoundBody}.
	 * @param ctx the parse tree
	 */
	void enterCompoundBody(SqlBaseParser.CompoundBodyContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#compoundBody}.
	 * @param ctx the parse tree
	 */
	void exitCompoundBody(SqlBaseParser.CompoundBodyContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#compoundStatement}.
	 * @param ctx the parse tree
	 */
	void enterCompoundStatement(SqlBaseParser.CompoundStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#compoundStatement}.
	 * @param ctx the parse tree
	 */
	void exitCompoundStatement(SqlBaseParser.CompoundStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setVariableInsideSqlScript}
	 * labeled alternative in {@link SqlBaseParser#setStatementInsideSqlScript}.
	 * @param ctx the parse tree
	 */
	void enterSetVariableInsideSqlScript(SqlBaseParser.SetVariableInsideSqlScriptContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setVariableInsideSqlScript}
	 * labeled alternative in {@link SqlBaseParser#setStatementInsideSqlScript}.
	 * @param ctx the parse tree
	 */
	void exitSetVariableInsideSqlScript(SqlBaseParser.SetVariableInsideSqlScriptContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#sqlStateValue}.
	 * @param ctx the parse tree
	 */
	void enterSqlStateValue(SqlBaseParser.SqlStateValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#sqlStateValue}.
	 * @param ctx the parse tree
	 */
	void exitSqlStateValue(SqlBaseParser.SqlStateValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#declareConditionStatement}.
	 * @param ctx the parse tree
	 */
	void enterDeclareConditionStatement(SqlBaseParser.DeclareConditionStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#declareConditionStatement}.
	 * @param ctx the parse tree
	 */
	void exitDeclareConditionStatement(SqlBaseParser.DeclareConditionStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#conditionValue}.
	 * @param ctx the parse tree
	 */
	void enterConditionValue(SqlBaseParser.ConditionValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#conditionValue}.
	 * @param ctx the parse tree
	 */
	void exitConditionValue(SqlBaseParser.ConditionValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#conditionValues}.
	 * @param ctx the parse tree
	 */
	void enterConditionValues(SqlBaseParser.ConditionValuesContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#conditionValues}.
	 * @param ctx the parse tree
	 */
	void exitConditionValues(SqlBaseParser.ConditionValuesContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#declareHandlerStatement}.
	 * @param ctx the parse tree
	 */
	void enterDeclareHandlerStatement(SqlBaseParser.DeclareHandlerStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#declareHandlerStatement}.
	 * @param ctx the parse tree
	 */
	void exitDeclareHandlerStatement(SqlBaseParser.DeclareHandlerStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#whileStatement}.
	 * @param ctx the parse tree
	 */
	void enterWhileStatement(SqlBaseParser.WhileStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#whileStatement}.
	 * @param ctx the parse tree
	 */
	void exitWhileStatement(SqlBaseParser.WhileStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#ifElseStatement}.
	 * @param ctx the parse tree
	 */
	void enterIfElseStatement(SqlBaseParser.IfElseStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#ifElseStatement}.
	 * @param ctx the parse tree
	 */
	void exitIfElseStatement(SqlBaseParser.IfElseStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#repeatStatement}.
	 * @param ctx the parse tree
	 */
	void enterRepeatStatement(SqlBaseParser.RepeatStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#repeatStatement}.
	 * @param ctx the parse tree
	 */
	void exitRepeatStatement(SqlBaseParser.RepeatStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#leaveStatement}.
	 * @param ctx the parse tree
	 */
	void enterLeaveStatement(SqlBaseParser.LeaveStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#leaveStatement}.
	 * @param ctx the parse tree
	 */
	void exitLeaveStatement(SqlBaseParser.LeaveStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#iterateStatement}.
	 * @param ctx the parse tree
	 */
	void enterIterateStatement(SqlBaseParser.IterateStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#iterateStatement}.
	 * @param ctx the parse tree
	 */
	void exitIterateStatement(SqlBaseParser.IterateStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code searchedCaseStatement}
	 * labeled alternative in {@link SqlBaseParser#caseStatement}.
	 * @param ctx the parse tree
	 */
	void enterSearchedCaseStatement(SqlBaseParser.SearchedCaseStatementContext ctx);
	/**
	 * Exit a parse tree produced by the {@code searchedCaseStatement}
	 * labeled alternative in {@link SqlBaseParser#caseStatement}.
	 * @param ctx the parse tree
	 */
	void exitSearchedCaseStatement(SqlBaseParser.SearchedCaseStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code simpleCaseStatement}
	 * labeled alternative in {@link SqlBaseParser#caseStatement}.
	 * @param ctx the parse tree
	 */
	void enterSimpleCaseStatement(SqlBaseParser.SimpleCaseStatementContext ctx);
	/**
	 * Exit a parse tree produced by the {@code simpleCaseStatement}
	 * labeled alternative in {@link SqlBaseParser#caseStatement}.
	 * @param ctx the parse tree
	 */
	void exitSimpleCaseStatement(SqlBaseParser.SimpleCaseStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#loopStatement}.
	 * @param ctx the parse tree
	 */
	void enterLoopStatement(SqlBaseParser.LoopStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#loopStatement}.
	 * @param ctx the parse tree
	 */
	void exitLoopStatement(SqlBaseParser.LoopStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#forStatement}.
	 * @param ctx the parse tree
	 */
	void enterForStatement(SqlBaseParser.ForStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#forStatement}.
	 * @param ctx the parse tree
	 */
	void exitForStatement(SqlBaseParser.ForStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	void enterSingleStatement(SqlBaseParser.SingleStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#singleStatement}.
	 * @param ctx the parse tree
	 */
	void exitSingleStatement(SqlBaseParser.SingleStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#beginLabel}.
	 * @param ctx the parse tree
	 */
	void enterBeginLabel(SqlBaseParser.BeginLabelContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#beginLabel}.
	 * @param ctx the parse tree
	 */
	void exitBeginLabel(SqlBaseParser.BeginLabelContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#endLabel}.
	 * @param ctx the parse tree
	 */
	void enterEndLabel(SqlBaseParser.EndLabelContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#endLabel}.
	 * @param ctx the parse tree
	 */
	void exitEndLabel(SqlBaseParser.EndLabelContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#singleExpression}.
	 * @param ctx the parse tree
	 */
	void enterSingleExpression(SqlBaseParser.SingleExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#singleExpression}.
	 * @param ctx the parse tree
	 */
	void exitSingleExpression(SqlBaseParser.SingleExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#singleTableIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterSingleTableIdentifier(SqlBaseParser.SingleTableIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#singleTableIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitSingleTableIdentifier(SqlBaseParser.SingleTableIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#singleMultipartIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterSingleMultipartIdentifier(SqlBaseParser.SingleMultipartIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#singleMultipartIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitSingleMultipartIdentifier(SqlBaseParser.SingleMultipartIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#singleFunctionIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterSingleFunctionIdentifier(SqlBaseParser.SingleFunctionIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#singleFunctionIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitSingleFunctionIdentifier(SqlBaseParser.SingleFunctionIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#singleDataType}.
	 * @param ctx the parse tree
	 */
	void enterSingleDataType(SqlBaseParser.SingleDataTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#singleDataType}.
	 * @param ctx the parse tree
	 */
	void exitSingleDataType(SqlBaseParser.SingleDataTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#singleTableSchema}.
	 * @param ctx the parse tree
	 */
	void enterSingleTableSchema(SqlBaseParser.SingleTableSchemaContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#singleTableSchema}.
	 * @param ctx the parse tree
	 */
	void exitSingleTableSchema(SqlBaseParser.SingleTableSchemaContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#singleRoutineParamList}.
	 * @param ctx the parse tree
	 */
	void enterSingleRoutineParamList(SqlBaseParser.SingleRoutineParamListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#singleRoutineParamList}.
	 * @param ctx the parse tree
	 */
	void exitSingleRoutineParamList(SqlBaseParser.SingleRoutineParamListContext ctx);
	/**
	 * Enter a parse tree produced by the {@code statementDefault}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterStatementDefault(SqlBaseParser.StatementDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code statementDefault}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitStatementDefault(SqlBaseParser.StatementDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code visitExecuteImmediate}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterVisitExecuteImmediate(SqlBaseParser.VisitExecuteImmediateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code visitExecuteImmediate}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitVisitExecuteImmediate(SqlBaseParser.VisitExecuteImmediateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dmlStatement}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDmlStatement(SqlBaseParser.DmlStatementContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dmlStatement}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDmlStatement(SqlBaseParser.DmlStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code use}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterUse(SqlBaseParser.UseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code use}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitUse(SqlBaseParser.UseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code useNamespace}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterUseNamespace(SqlBaseParser.UseNamespaceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code useNamespace}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitUseNamespace(SqlBaseParser.UseNamespaceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setCatalog}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterSetCatalog(SqlBaseParser.SetCatalogContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setCatalog}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitSetCatalog(SqlBaseParser.SetCatalogContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createNamespace}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCreateNamespace(SqlBaseParser.CreateNamespaceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createNamespace}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCreateNamespace(SqlBaseParser.CreateNamespaceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setNamespaceProperties}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterSetNamespaceProperties(SqlBaseParser.SetNamespacePropertiesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setNamespaceProperties}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitSetNamespaceProperties(SqlBaseParser.SetNamespacePropertiesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code unsetNamespaceProperties}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterUnsetNamespaceProperties(SqlBaseParser.UnsetNamespacePropertiesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code unsetNamespaceProperties}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitUnsetNamespaceProperties(SqlBaseParser.UnsetNamespacePropertiesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setNamespaceCollation}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterSetNamespaceCollation(SqlBaseParser.SetNamespaceCollationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setNamespaceCollation}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitSetNamespaceCollation(SqlBaseParser.SetNamespaceCollationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setNamespaceLocation}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterSetNamespaceLocation(SqlBaseParser.SetNamespaceLocationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setNamespaceLocation}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitSetNamespaceLocation(SqlBaseParser.SetNamespaceLocationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropNamespace}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDropNamespace(SqlBaseParser.DropNamespaceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropNamespace}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDropNamespace(SqlBaseParser.DropNamespaceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showNamespaces}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowNamespaces(SqlBaseParser.ShowNamespacesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showNamespaces}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowNamespaces(SqlBaseParser.ShowNamespacesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCreateTable(SqlBaseParser.CreateTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCreateTable(SqlBaseParser.CreateTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createTableLike}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCreateTableLike(SqlBaseParser.CreateTableLikeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createTableLike}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCreateTableLike(SqlBaseParser.CreateTableLikeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code replaceTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterReplaceTable(SqlBaseParser.ReplaceTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code replaceTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitReplaceTable(SqlBaseParser.ReplaceTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code analyze}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterAnalyze(SqlBaseParser.AnalyzeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code analyze}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitAnalyze(SqlBaseParser.AnalyzeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code analyzeTables}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterAnalyzeTables(SqlBaseParser.AnalyzeTablesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code analyzeTables}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitAnalyzeTables(SqlBaseParser.AnalyzeTablesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code addTableColumns}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterAddTableColumns(SqlBaseParser.AddTableColumnsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code addTableColumns}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitAddTableColumns(SqlBaseParser.AddTableColumnsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code renameTableColumn}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterRenameTableColumn(SqlBaseParser.RenameTableColumnContext ctx);
	/**
	 * Exit a parse tree produced by the {@code renameTableColumn}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitRenameTableColumn(SqlBaseParser.RenameTableColumnContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropTableColumns}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDropTableColumns(SqlBaseParser.DropTableColumnsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropTableColumns}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDropTableColumns(SqlBaseParser.DropTableColumnsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code renameTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterRenameTable(SqlBaseParser.RenameTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code renameTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitRenameTable(SqlBaseParser.RenameTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setTableProperties}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterSetTableProperties(SqlBaseParser.SetTablePropertiesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setTableProperties}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitSetTableProperties(SqlBaseParser.SetTablePropertiesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code unsetTableProperties}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterUnsetTableProperties(SqlBaseParser.UnsetTablePropertiesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code unsetTableProperties}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitUnsetTableProperties(SqlBaseParser.UnsetTablePropertiesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterTableAlterColumn}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterAlterTableAlterColumn(SqlBaseParser.AlterTableAlterColumnContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterTableAlterColumn}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitAlterTableAlterColumn(SqlBaseParser.AlterTableAlterColumnContext ctx);
	/**
	 * Enter a parse tree produced by the {@code hiveChangeColumn}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterHiveChangeColumn(SqlBaseParser.HiveChangeColumnContext ctx);
	/**
	 * Exit a parse tree produced by the {@code hiveChangeColumn}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitHiveChangeColumn(SqlBaseParser.HiveChangeColumnContext ctx);
	/**
	 * Enter a parse tree produced by the {@code hiveReplaceColumns}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterHiveReplaceColumns(SqlBaseParser.HiveReplaceColumnsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code hiveReplaceColumns}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitHiveReplaceColumns(SqlBaseParser.HiveReplaceColumnsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setTableSerDe}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterSetTableSerDe(SqlBaseParser.SetTableSerDeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setTableSerDe}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitSetTableSerDe(SqlBaseParser.SetTableSerDeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code addTablePartition}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterAddTablePartition(SqlBaseParser.AddTablePartitionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code addTablePartition}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitAddTablePartition(SqlBaseParser.AddTablePartitionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code renameTablePartition}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterRenameTablePartition(SqlBaseParser.RenameTablePartitionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code renameTablePartition}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitRenameTablePartition(SqlBaseParser.RenameTablePartitionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropTablePartitions}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDropTablePartitions(SqlBaseParser.DropTablePartitionsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropTablePartitions}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDropTablePartitions(SqlBaseParser.DropTablePartitionsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setTableLocation}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterSetTableLocation(SqlBaseParser.SetTableLocationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setTableLocation}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitSetTableLocation(SqlBaseParser.SetTableLocationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code recoverPartitions}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterRecoverPartitions(SqlBaseParser.RecoverPartitionsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code recoverPartitions}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitRecoverPartitions(SqlBaseParser.RecoverPartitionsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterClusterBy}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterAlterClusterBy(SqlBaseParser.AlterClusterByContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterClusterBy}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitAlterClusterBy(SqlBaseParser.AlterClusterByContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterTableCollation}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterAlterTableCollation(SqlBaseParser.AlterTableCollationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterTableCollation}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitAlterTableCollation(SqlBaseParser.AlterTableCollationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code addTableConstraint}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterAddTableConstraint(SqlBaseParser.AddTableConstraintContext ctx);
	/**
	 * Exit a parse tree produced by the {@code addTableConstraint}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitAddTableConstraint(SqlBaseParser.AddTableConstraintContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropTableConstraint}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDropTableConstraint(SqlBaseParser.DropTableConstraintContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropTableConstraint}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDropTableConstraint(SqlBaseParser.DropTableConstraintContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDropTable(SqlBaseParser.DropTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDropTable(SqlBaseParser.DropTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropView}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDropView(SqlBaseParser.DropViewContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropView}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDropView(SqlBaseParser.DropViewContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createView}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCreateView(SqlBaseParser.CreateViewContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createView}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCreateView(SqlBaseParser.CreateViewContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createTempViewUsing}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCreateTempViewUsing(SqlBaseParser.CreateTempViewUsingContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createTempViewUsing}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCreateTempViewUsing(SqlBaseParser.CreateTempViewUsingContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterViewQuery}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterAlterViewQuery(SqlBaseParser.AlterViewQueryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterViewQuery}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitAlterViewQuery(SqlBaseParser.AlterViewQueryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code alterViewSchemaBinding}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterAlterViewSchemaBinding(SqlBaseParser.AlterViewSchemaBindingContext ctx);
	/**
	 * Exit a parse tree produced by the {@code alterViewSchemaBinding}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitAlterViewSchemaBinding(SqlBaseParser.AlterViewSchemaBindingContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCreateFunction(SqlBaseParser.CreateFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCreateFunction(SqlBaseParser.CreateFunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createUserDefinedFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCreateUserDefinedFunction(SqlBaseParser.CreateUserDefinedFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createUserDefinedFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCreateUserDefinedFunction(SqlBaseParser.CreateUserDefinedFunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDropFunction(SqlBaseParser.DropFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDropFunction(SqlBaseParser.DropFunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createVariable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCreateVariable(SqlBaseParser.CreateVariableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createVariable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCreateVariable(SqlBaseParser.CreateVariableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropVariable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDropVariable(SqlBaseParser.DropVariableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropVariable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDropVariable(SqlBaseParser.DropVariableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code explain}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterExplain(SqlBaseParser.ExplainContext ctx);
	/**
	 * Exit a parse tree produced by the {@code explain}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitExplain(SqlBaseParser.ExplainContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showTables}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowTables(SqlBaseParser.ShowTablesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showTables}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowTables(SqlBaseParser.ShowTablesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showTableExtended}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowTableExtended(SqlBaseParser.ShowTableExtendedContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showTableExtended}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowTableExtended(SqlBaseParser.ShowTableExtendedContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showTblProperties}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowTblProperties(SqlBaseParser.ShowTblPropertiesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showTblProperties}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowTblProperties(SqlBaseParser.ShowTblPropertiesContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showColumns}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowColumns(SqlBaseParser.ShowColumnsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showColumns}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowColumns(SqlBaseParser.ShowColumnsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showViews}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowViews(SqlBaseParser.ShowViewsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showViews}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowViews(SqlBaseParser.ShowViewsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showPartitions}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowPartitions(SqlBaseParser.ShowPartitionsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showPartitions}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowPartitions(SqlBaseParser.ShowPartitionsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showFunctions}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowFunctions(SqlBaseParser.ShowFunctionsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showFunctions}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowFunctions(SqlBaseParser.ShowFunctionsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showProcedures}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowProcedures(SqlBaseParser.ShowProceduresContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showProcedures}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowProcedures(SqlBaseParser.ShowProceduresContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCreateTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowCreateTable(SqlBaseParser.ShowCreateTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCreateTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowCreateTable(SqlBaseParser.ShowCreateTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCurrentNamespace}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowCurrentNamespace(SqlBaseParser.ShowCurrentNamespaceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCurrentNamespace}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowCurrentNamespace(SqlBaseParser.ShowCurrentNamespaceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code showCatalogs}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterShowCatalogs(SqlBaseParser.ShowCatalogsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code showCatalogs}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitShowCatalogs(SqlBaseParser.ShowCatalogsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code describeFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDescribeFunction(SqlBaseParser.DescribeFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code describeFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDescribeFunction(SqlBaseParser.DescribeFunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code describeProcedure}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDescribeProcedure(SqlBaseParser.DescribeProcedureContext ctx);
	/**
	 * Exit a parse tree produced by the {@code describeProcedure}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDescribeProcedure(SqlBaseParser.DescribeProcedureContext ctx);
	/**
	 * Enter a parse tree produced by the {@code describeNamespace}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDescribeNamespace(SqlBaseParser.DescribeNamespaceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code describeNamespace}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDescribeNamespace(SqlBaseParser.DescribeNamespaceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code describeRelation}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDescribeRelation(SqlBaseParser.DescribeRelationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code describeRelation}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDescribeRelation(SqlBaseParser.DescribeRelationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code describeQuery}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDescribeQuery(SqlBaseParser.DescribeQueryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code describeQuery}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDescribeQuery(SqlBaseParser.DescribeQueryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code commentNamespace}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCommentNamespace(SqlBaseParser.CommentNamespaceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code commentNamespace}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCommentNamespace(SqlBaseParser.CommentNamespaceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code commentTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCommentTable(SqlBaseParser.CommentTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code commentTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCommentTable(SqlBaseParser.CommentTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code refreshTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterRefreshTable(SqlBaseParser.RefreshTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code refreshTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitRefreshTable(SqlBaseParser.RefreshTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code refreshFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterRefreshFunction(SqlBaseParser.RefreshFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code refreshFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitRefreshFunction(SqlBaseParser.RefreshFunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code refreshResource}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterRefreshResource(SqlBaseParser.RefreshResourceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code refreshResource}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitRefreshResource(SqlBaseParser.RefreshResourceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cacheTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCacheTable(SqlBaseParser.CacheTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cacheTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCacheTable(SqlBaseParser.CacheTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code uncacheTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterUncacheTable(SqlBaseParser.UncacheTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code uncacheTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitUncacheTable(SqlBaseParser.UncacheTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code clearCache}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterClearCache(SqlBaseParser.ClearCacheContext ctx);
	/**
	 * Exit a parse tree produced by the {@code clearCache}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitClearCache(SqlBaseParser.ClearCacheContext ctx);
	/**
	 * Enter a parse tree produced by the {@code loadData}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterLoadData(SqlBaseParser.LoadDataContext ctx);
	/**
	 * Exit a parse tree produced by the {@code loadData}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitLoadData(SqlBaseParser.LoadDataContext ctx);
	/**
	 * Enter a parse tree produced by the {@code truncateTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterTruncateTable(SqlBaseParser.TruncateTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code truncateTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitTruncateTable(SqlBaseParser.TruncateTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code repairTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterRepairTable(SqlBaseParser.RepairTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code repairTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitRepairTable(SqlBaseParser.RepairTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code manageResource}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterManageResource(SqlBaseParser.ManageResourceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code manageResource}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitManageResource(SqlBaseParser.ManageResourceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createIndex}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCreateIndex(SqlBaseParser.CreateIndexContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createIndex}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCreateIndex(SqlBaseParser.CreateIndexContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dropIndex}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterDropIndex(SqlBaseParser.DropIndexContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dropIndex}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitDropIndex(SqlBaseParser.DropIndexContext ctx);
	/**
	 * Enter a parse tree produced by the {@code call}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCall(SqlBaseParser.CallContext ctx);
	/**
	 * Exit a parse tree produced by the {@code call}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCall(SqlBaseParser.CallContext ctx);
	/**
	 * Enter a parse tree produced by the {@code failNativeCommand}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterFailNativeCommand(SqlBaseParser.FailNativeCommandContext ctx);
	/**
	 * Exit a parse tree produced by the {@code failNativeCommand}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitFailNativeCommand(SqlBaseParser.FailNativeCommandContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createPipelineDataset}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCreatePipelineDataset(SqlBaseParser.CreatePipelineDatasetContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createPipelineDataset}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCreatePipelineDataset(SqlBaseParser.CreatePipelineDatasetContext ctx);
	/**
	 * Enter a parse tree produced by the {@code createPipelineInsertIntoFlow}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void enterCreatePipelineInsertIntoFlow(SqlBaseParser.CreatePipelineInsertIntoFlowContext ctx);
	/**
	 * Exit a parse tree produced by the {@code createPipelineInsertIntoFlow}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 */
	void exitCreatePipelineInsertIntoFlow(SqlBaseParser.CreatePipelineInsertIntoFlowContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#materializedView}.
	 * @param ctx the parse tree
	 */
	void enterMaterializedView(SqlBaseParser.MaterializedViewContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#materializedView}.
	 * @param ctx the parse tree
	 */
	void exitMaterializedView(SqlBaseParser.MaterializedViewContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#streamingTable}.
	 * @param ctx the parse tree
	 */
	void enterStreamingTable(SqlBaseParser.StreamingTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#streamingTable}.
	 * @param ctx the parse tree
	 */
	void exitStreamingTable(SqlBaseParser.StreamingTableContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#createPipelineDatasetHeader}.
	 * @param ctx the parse tree
	 */
	void enterCreatePipelineDatasetHeader(SqlBaseParser.CreatePipelineDatasetHeaderContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#createPipelineDatasetHeader}.
	 * @param ctx the parse tree
	 */
	void exitCreatePipelineDatasetHeader(SqlBaseParser.CreatePipelineDatasetHeaderContext ctx);
	/**
	 * Enter a parse tree produced by the {@code streamTableName}
	 * labeled alternative in {@link SqlBaseParser#streamRelationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterStreamTableName(SqlBaseParser.StreamTableNameContext ctx);
	/**
	 * Exit a parse tree produced by the {@code streamTableName}
	 * labeled alternative in {@link SqlBaseParser#streamRelationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitStreamTableName(SqlBaseParser.StreamTableNameContext ctx);
	/**
	 * Enter a parse tree produced by the {@code failSetRole}
	 * labeled alternative in {@link SqlBaseParser#setResetStatement}.
	 * @param ctx the parse tree
	 */
	void enterFailSetRole(SqlBaseParser.FailSetRoleContext ctx);
	/**
	 * Exit a parse tree produced by the {@code failSetRole}
	 * labeled alternative in {@link SqlBaseParser#setResetStatement}.
	 * @param ctx the parse tree
	 */
	void exitFailSetRole(SqlBaseParser.FailSetRoleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setTimeZone}
	 * labeled alternative in {@link SqlBaseParser#setResetStatement}.
	 * @param ctx the parse tree
	 */
	void enterSetTimeZone(SqlBaseParser.SetTimeZoneContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setTimeZone}
	 * labeled alternative in {@link SqlBaseParser#setResetStatement}.
	 * @param ctx the parse tree
	 */
	void exitSetTimeZone(SqlBaseParser.SetTimeZoneContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setVariable}
	 * labeled alternative in {@link SqlBaseParser#setResetStatement}.
	 * @param ctx the parse tree
	 */
	void enterSetVariable(SqlBaseParser.SetVariableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setVariable}
	 * labeled alternative in {@link SqlBaseParser#setResetStatement}.
	 * @param ctx the parse tree
	 */
	void exitSetVariable(SqlBaseParser.SetVariableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setQuotedConfiguration}
	 * labeled alternative in {@link SqlBaseParser#setResetStatement}.
	 * @param ctx the parse tree
	 */
	void enterSetQuotedConfiguration(SqlBaseParser.SetQuotedConfigurationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setQuotedConfiguration}
	 * labeled alternative in {@link SqlBaseParser#setResetStatement}.
	 * @param ctx the parse tree
	 */
	void exitSetQuotedConfiguration(SqlBaseParser.SetQuotedConfigurationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setConfiguration}
	 * labeled alternative in {@link SqlBaseParser#setResetStatement}.
	 * @param ctx the parse tree
	 */
	void enterSetConfiguration(SqlBaseParser.SetConfigurationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setConfiguration}
	 * labeled alternative in {@link SqlBaseParser#setResetStatement}.
	 * @param ctx the parse tree
	 */
	void exitSetConfiguration(SqlBaseParser.SetConfigurationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code resetQuotedConfiguration}
	 * labeled alternative in {@link SqlBaseParser#setResetStatement}.
	 * @param ctx the parse tree
	 */
	void enterResetQuotedConfiguration(SqlBaseParser.ResetQuotedConfigurationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code resetQuotedConfiguration}
	 * labeled alternative in {@link SqlBaseParser#setResetStatement}.
	 * @param ctx the parse tree
	 */
	void exitResetQuotedConfiguration(SqlBaseParser.ResetQuotedConfigurationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code resetConfiguration}
	 * labeled alternative in {@link SqlBaseParser#setResetStatement}.
	 * @param ctx the parse tree
	 */
	void enterResetConfiguration(SqlBaseParser.ResetConfigurationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code resetConfiguration}
	 * labeled alternative in {@link SqlBaseParser#setResetStatement}.
	 * @param ctx the parse tree
	 */
	void exitResetConfiguration(SqlBaseParser.ResetConfigurationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#executeImmediate}.
	 * @param ctx the parse tree
	 */
	void enterExecuteImmediate(SqlBaseParser.ExecuteImmediateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#executeImmediate}.
	 * @param ctx the parse tree
	 */
	void exitExecuteImmediate(SqlBaseParser.ExecuteImmediateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#executeImmediateUsing}.
	 * @param ctx the parse tree
	 */
	void enterExecuteImmediateUsing(SqlBaseParser.ExecuteImmediateUsingContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#executeImmediateUsing}.
	 * @param ctx the parse tree
	 */
	void exitExecuteImmediateUsing(SqlBaseParser.ExecuteImmediateUsingContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#timezone}.
	 * @param ctx the parse tree
	 */
	void enterTimezone(SqlBaseParser.TimezoneContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#timezone}.
	 * @param ctx the parse tree
	 */
	void exitTimezone(SqlBaseParser.TimezoneContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#configKey}.
	 * @param ctx the parse tree
	 */
	void enterConfigKey(SqlBaseParser.ConfigKeyContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#configKey}.
	 * @param ctx the parse tree
	 */
	void exitConfigKey(SqlBaseParser.ConfigKeyContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#configValue}.
	 * @param ctx the parse tree
	 */
	void enterConfigValue(SqlBaseParser.ConfigValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#configValue}.
	 * @param ctx the parse tree
	 */
	void exitConfigValue(SqlBaseParser.ConfigValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#unsupportedHiveNativeCommands}.
	 * @param ctx the parse tree
	 */
	void enterUnsupportedHiveNativeCommands(SqlBaseParser.UnsupportedHiveNativeCommandsContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#unsupportedHiveNativeCommands}.
	 * @param ctx the parse tree
	 */
	void exitUnsupportedHiveNativeCommands(SqlBaseParser.UnsupportedHiveNativeCommandsContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#createTableHeader}.
	 * @param ctx the parse tree
	 */
	void enterCreateTableHeader(SqlBaseParser.CreateTableHeaderContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#createTableHeader}.
	 * @param ctx the parse tree
	 */
	void exitCreateTableHeader(SqlBaseParser.CreateTableHeaderContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#replaceTableHeader}.
	 * @param ctx the parse tree
	 */
	void enterReplaceTableHeader(SqlBaseParser.ReplaceTableHeaderContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#replaceTableHeader}.
	 * @param ctx the parse tree
	 */
	void exitReplaceTableHeader(SqlBaseParser.ReplaceTableHeaderContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#clusterBySpec}.
	 * @param ctx the parse tree
	 */
	void enterClusterBySpec(SqlBaseParser.ClusterBySpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#clusterBySpec}.
	 * @param ctx the parse tree
	 */
	void exitClusterBySpec(SqlBaseParser.ClusterBySpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#bucketSpec}.
	 * @param ctx the parse tree
	 */
	void enterBucketSpec(SqlBaseParser.BucketSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#bucketSpec}.
	 * @param ctx the parse tree
	 */
	void exitBucketSpec(SqlBaseParser.BucketSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#skewSpec}.
	 * @param ctx the parse tree
	 */
	void enterSkewSpec(SqlBaseParser.SkewSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#skewSpec}.
	 * @param ctx the parse tree
	 */
	void exitSkewSpec(SqlBaseParser.SkewSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#locationSpec}.
	 * @param ctx the parse tree
	 */
	void enterLocationSpec(SqlBaseParser.LocationSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#locationSpec}.
	 * @param ctx the parse tree
	 */
	void exitLocationSpec(SqlBaseParser.LocationSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#schemaBinding}.
	 * @param ctx the parse tree
	 */
	void enterSchemaBinding(SqlBaseParser.SchemaBindingContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#schemaBinding}.
	 * @param ctx the parse tree
	 */
	void exitSchemaBinding(SqlBaseParser.SchemaBindingContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#commentSpec}.
	 * @param ctx the parse tree
	 */
	void enterCommentSpec(SqlBaseParser.CommentSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#commentSpec}.
	 * @param ctx the parse tree
	 */
	void exitCommentSpec(SqlBaseParser.CommentSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#singleQuery}.
	 * @param ctx the parse tree
	 */
	void enterSingleQuery(SqlBaseParser.SingleQueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#singleQuery}.
	 * @param ctx the parse tree
	 */
	void exitSingleQuery(SqlBaseParser.SingleQueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#query}.
	 * @param ctx the parse tree
	 */
	void enterQuery(SqlBaseParser.QueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#query}.
	 * @param ctx the parse tree
	 */
	void exitQuery(SqlBaseParser.QueryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code insertOverwriteTable}
	 * labeled alternative in {@link SqlBaseParser#insertInto}.
	 * @param ctx the parse tree
	 */
	void enterInsertOverwriteTable(SqlBaseParser.InsertOverwriteTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code insertOverwriteTable}
	 * labeled alternative in {@link SqlBaseParser#insertInto}.
	 * @param ctx the parse tree
	 */
	void exitInsertOverwriteTable(SqlBaseParser.InsertOverwriteTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code insertIntoTable}
	 * labeled alternative in {@link SqlBaseParser#insertInto}.
	 * @param ctx the parse tree
	 */
	void enterInsertIntoTable(SqlBaseParser.InsertIntoTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code insertIntoTable}
	 * labeled alternative in {@link SqlBaseParser#insertInto}.
	 * @param ctx the parse tree
	 */
	void exitInsertIntoTable(SqlBaseParser.InsertIntoTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code insertIntoReplaceWhere}
	 * labeled alternative in {@link SqlBaseParser#insertInto}.
	 * @param ctx the parse tree
	 */
	void enterInsertIntoReplaceWhere(SqlBaseParser.InsertIntoReplaceWhereContext ctx);
	/**
	 * Exit a parse tree produced by the {@code insertIntoReplaceWhere}
	 * labeled alternative in {@link SqlBaseParser#insertInto}.
	 * @param ctx the parse tree
	 */
	void exitInsertIntoReplaceWhere(SqlBaseParser.InsertIntoReplaceWhereContext ctx);
	/**
	 * Enter a parse tree produced by the {@code insertOverwriteHiveDir}
	 * labeled alternative in {@link SqlBaseParser#insertInto}.
	 * @param ctx the parse tree
	 */
	void enterInsertOverwriteHiveDir(SqlBaseParser.InsertOverwriteHiveDirContext ctx);
	/**
	 * Exit a parse tree produced by the {@code insertOverwriteHiveDir}
	 * labeled alternative in {@link SqlBaseParser#insertInto}.
	 * @param ctx the parse tree
	 */
	void exitInsertOverwriteHiveDir(SqlBaseParser.InsertOverwriteHiveDirContext ctx);
	/**
	 * Enter a parse tree produced by the {@code insertOverwriteDir}
	 * labeled alternative in {@link SqlBaseParser#insertInto}.
	 * @param ctx the parse tree
	 */
	void enterInsertOverwriteDir(SqlBaseParser.InsertOverwriteDirContext ctx);
	/**
	 * Exit a parse tree produced by the {@code insertOverwriteDir}
	 * labeled alternative in {@link SqlBaseParser#insertInto}.
	 * @param ctx the parse tree
	 */
	void exitInsertOverwriteDir(SqlBaseParser.InsertOverwriteDirContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#partitionSpecLocation}.
	 * @param ctx the parse tree
	 */
	void enterPartitionSpecLocation(SqlBaseParser.PartitionSpecLocationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#partitionSpecLocation}.
	 * @param ctx the parse tree
	 */
	void exitPartitionSpecLocation(SqlBaseParser.PartitionSpecLocationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#partitionSpec}.
	 * @param ctx the parse tree
	 */
	void enterPartitionSpec(SqlBaseParser.PartitionSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#partitionSpec}.
	 * @param ctx the parse tree
	 */
	void exitPartitionSpec(SqlBaseParser.PartitionSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#partitionVal}.
	 * @param ctx the parse tree
	 */
	void enterPartitionVal(SqlBaseParser.PartitionValContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#partitionVal}.
	 * @param ctx the parse tree
	 */
	void exitPartitionVal(SqlBaseParser.PartitionValContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#createPipelineFlowHeader}.
	 * @param ctx the parse tree
	 */
	void enterCreatePipelineFlowHeader(SqlBaseParser.CreatePipelineFlowHeaderContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#createPipelineFlowHeader}.
	 * @param ctx the parse tree
	 */
	void exitCreatePipelineFlowHeader(SqlBaseParser.CreatePipelineFlowHeaderContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#namespace}.
	 * @param ctx the parse tree
	 */
	void enterNamespace(SqlBaseParser.NamespaceContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#namespace}.
	 * @param ctx the parse tree
	 */
	void exitNamespace(SqlBaseParser.NamespaceContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#namespaces}.
	 * @param ctx the parse tree
	 */
	void enterNamespaces(SqlBaseParser.NamespacesContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#namespaces}.
	 * @param ctx the parse tree
	 */
	void exitNamespaces(SqlBaseParser.NamespacesContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#variable}.
	 * @param ctx the parse tree
	 */
	void enterVariable(SqlBaseParser.VariableContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#variable}.
	 * @param ctx the parse tree
	 */
	void exitVariable(SqlBaseParser.VariableContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#describeFuncName}.
	 * @param ctx the parse tree
	 */
	void enterDescribeFuncName(SqlBaseParser.DescribeFuncNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#describeFuncName}.
	 * @param ctx the parse tree
	 */
	void exitDescribeFuncName(SqlBaseParser.DescribeFuncNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#describeColName}.
	 * @param ctx the parse tree
	 */
	void enterDescribeColName(SqlBaseParser.DescribeColNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#describeColName}.
	 * @param ctx the parse tree
	 */
	void exitDescribeColName(SqlBaseParser.DescribeColNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#ctes}.
	 * @param ctx the parse tree
	 */
	void enterCtes(SqlBaseParser.CtesContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#ctes}.
	 * @param ctx the parse tree
	 */
	void exitCtes(SqlBaseParser.CtesContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#namedQuery}.
	 * @param ctx the parse tree
	 */
	void enterNamedQuery(SqlBaseParser.NamedQueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#namedQuery}.
	 * @param ctx the parse tree
	 */
	void exitNamedQuery(SqlBaseParser.NamedQueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#tableProvider}.
	 * @param ctx the parse tree
	 */
	void enterTableProvider(SqlBaseParser.TableProviderContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#tableProvider}.
	 * @param ctx the parse tree
	 */
	void exitTableProvider(SqlBaseParser.TableProviderContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#createTableClauses}.
	 * @param ctx the parse tree
	 */
	void enterCreateTableClauses(SqlBaseParser.CreateTableClausesContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#createTableClauses}.
	 * @param ctx the parse tree
	 */
	void exitCreateTableClauses(SqlBaseParser.CreateTableClausesContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#propertyList}.
	 * @param ctx the parse tree
	 */
	void enterPropertyList(SqlBaseParser.PropertyListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#propertyList}.
	 * @param ctx the parse tree
	 */
	void exitPropertyList(SqlBaseParser.PropertyListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#property}.
	 * @param ctx the parse tree
	 */
	void enterProperty(SqlBaseParser.PropertyContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#property}.
	 * @param ctx the parse tree
	 */
	void exitProperty(SqlBaseParser.PropertyContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#propertyKey}.
	 * @param ctx the parse tree
	 */
	void enterPropertyKey(SqlBaseParser.PropertyKeyContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#propertyKey}.
	 * @param ctx the parse tree
	 */
	void exitPropertyKey(SqlBaseParser.PropertyKeyContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	void enterPropertyValue(SqlBaseParser.PropertyValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	void exitPropertyValue(SqlBaseParser.PropertyValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#expressionPropertyList}.
	 * @param ctx the parse tree
	 */
	void enterExpressionPropertyList(SqlBaseParser.ExpressionPropertyListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#expressionPropertyList}.
	 * @param ctx the parse tree
	 */
	void exitExpressionPropertyList(SqlBaseParser.ExpressionPropertyListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#expressionProperty}.
	 * @param ctx the parse tree
	 */
	void enterExpressionProperty(SqlBaseParser.ExpressionPropertyContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#expressionProperty}.
	 * @param ctx the parse tree
	 */
	void exitExpressionProperty(SqlBaseParser.ExpressionPropertyContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#constantList}.
	 * @param ctx the parse tree
	 */
	void enterConstantList(SqlBaseParser.ConstantListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#constantList}.
	 * @param ctx the parse tree
	 */
	void exitConstantList(SqlBaseParser.ConstantListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#nestedConstantList}.
	 * @param ctx the parse tree
	 */
	void enterNestedConstantList(SqlBaseParser.NestedConstantListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#nestedConstantList}.
	 * @param ctx the parse tree
	 */
	void exitNestedConstantList(SqlBaseParser.NestedConstantListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#createFileFormat}.
	 * @param ctx the parse tree
	 */
	void enterCreateFileFormat(SqlBaseParser.CreateFileFormatContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#createFileFormat}.
	 * @param ctx the parse tree
	 */
	void exitCreateFileFormat(SqlBaseParser.CreateFileFormatContext ctx);
	/**
	 * Enter a parse tree produced by the {@code tableFileFormat}
	 * labeled alternative in {@link SqlBaseParser#fileFormat}.
	 * @param ctx the parse tree
	 */
	void enterTableFileFormat(SqlBaseParser.TableFileFormatContext ctx);
	/**
	 * Exit a parse tree produced by the {@code tableFileFormat}
	 * labeled alternative in {@link SqlBaseParser#fileFormat}.
	 * @param ctx the parse tree
	 */
	void exitTableFileFormat(SqlBaseParser.TableFileFormatContext ctx);
	/**
	 * Enter a parse tree produced by the {@code genericFileFormat}
	 * labeled alternative in {@link SqlBaseParser#fileFormat}.
	 * @param ctx the parse tree
	 */
	void enterGenericFileFormat(SqlBaseParser.GenericFileFormatContext ctx);
	/**
	 * Exit a parse tree produced by the {@code genericFileFormat}
	 * labeled alternative in {@link SqlBaseParser#fileFormat}.
	 * @param ctx the parse tree
	 */
	void exitGenericFileFormat(SqlBaseParser.GenericFileFormatContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#storageHandler}.
	 * @param ctx the parse tree
	 */
	void enterStorageHandler(SqlBaseParser.StorageHandlerContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#storageHandler}.
	 * @param ctx the parse tree
	 */
	void exitStorageHandler(SqlBaseParser.StorageHandlerContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#resource}.
	 * @param ctx the parse tree
	 */
	void enterResource(SqlBaseParser.ResourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#resource}.
	 * @param ctx the parse tree
	 */
	void exitResource(SqlBaseParser.ResourceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code singleInsertQuery}
	 * labeled alternative in {@link SqlBaseParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 */
	void enterSingleInsertQuery(SqlBaseParser.SingleInsertQueryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code singleInsertQuery}
	 * labeled alternative in {@link SqlBaseParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 */
	void exitSingleInsertQuery(SqlBaseParser.SingleInsertQueryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code multiInsertQuery}
	 * labeled alternative in {@link SqlBaseParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 */
	void enterMultiInsertQuery(SqlBaseParser.MultiInsertQueryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code multiInsertQuery}
	 * labeled alternative in {@link SqlBaseParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 */
	void exitMultiInsertQuery(SqlBaseParser.MultiInsertQueryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code deleteFromTable}
	 * labeled alternative in {@link SqlBaseParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 */
	void enterDeleteFromTable(SqlBaseParser.DeleteFromTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code deleteFromTable}
	 * labeled alternative in {@link SqlBaseParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 */
	void exitDeleteFromTable(SqlBaseParser.DeleteFromTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code updateTable}
	 * labeled alternative in {@link SqlBaseParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 */
	void enterUpdateTable(SqlBaseParser.UpdateTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code updateTable}
	 * labeled alternative in {@link SqlBaseParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 */
	void exitUpdateTable(SqlBaseParser.UpdateTableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code mergeIntoTable}
	 * labeled alternative in {@link SqlBaseParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 */
	void enterMergeIntoTable(SqlBaseParser.MergeIntoTableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code mergeIntoTable}
	 * labeled alternative in {@link SqlBaseParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 */
	void exitMergeIntoTable(SqlBaseParser.MergeIntoTableContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#identifierReference}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierReference(SqlBaseParser.IdentifierReferenceContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#identifierReference}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierReference(SqlBaseParser.IdentifierReferenceContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#catalogIdentifierReference}.
	 * @param ctx the parse tree
	 */
	void enterCatalogIdentifierReference(SqlBaseParser.CatalogIdentifierReferenceContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#catalogIdentifierReference}.
	 * @param ctx the parse tree
	 */
	void exitCatalogIdentifierReference(SqlBaseParser.CatalogIdentifierReferenceContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#queryOrganization}.
	 * @param ctx the parse tree
	 */
	void enterQueryOrganization(SqlBaseParser.QueryOrganizationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#queryOrganization}.
	 * @param ctx the parse tree
	 */
	void exitQueryOrganization(SqlBaseParser.QueryOrganizationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#multiInsertQueryBody}.
	 * @param ctx the parse tree
	 */
	void enterMultiInsertQueryBody(SqlBaseParser.MultiInsertQueryBodyContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#multiInsertQueryBody}.
	 * @param ctx the parse tree
	 */
	void exitMultiInsertQueryBody(SqlBaseParser.MultiInsertQueryBodyContext ctx);
	/**
	 * Enter a parse tree produced by the {@code operatorPipeStatement}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void enterOperatorPipeStatement(SqlBaseParser.OperatorPipeStatementContext ctx);
	/**
	 * Exit a parse tree produced by the {@code operatorPipeStatement}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void exitOperatorPipeStatement(SqlBaseParser.OperatorPipeStatementContext ctx);
	/**
	 * Enter a parse tree produced by the {@code queryTermDefault}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void enterQueryTermDefault(SqlBaseParser.QueryTermDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code queryTermDefault}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void exitQueryTermDefault(SqlBaseParser.QueryTermDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code setOperation}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void enterSetOperation(SqlBaseParser.SetOperationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code setOperation}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 */
	void exitSetOperation(SqlBaseParser.SetOperationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterQueryPrimaryDefault(SqlBaseParser.QueryPrimaryDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitQueryPrimaryDefault(SqlBaseParser.QueryPrimaryDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code fromStmt}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterFromStmt(SqlBaseParser.FromStmtContext ctx);
	/**
	 * Exit a parse tree produced by the {@code fromStmt}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitFromStmt(SqlBaseParser.FromStmtContext ctx);
	/**
	 * Enter a parse tree produced by the {@code table}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterTable(SqlBaseParser.TableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code table}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitTable(SqlBaseParser.TableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code inlineTableDefault1}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterInlineTableDefault1(SqlBaseParser.InlineTableDefault1Context ctx);
	/**
	 * Exit a parse tree produced by the {@code inlineTableDefault1}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitInlineTableDefault1(SqlBaseParser.InlineTableDefault1Context ctx);
	/**
	 * Enter a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void enterSubquery(SqlBaseParser.SubqueryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 */
	void exitSubquery(SqlBaseParser.SubqueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#sortItem}.
	 * @param ctx the parse tree
	 */
	void enterSortItem(SqlBaseParser.SortItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#sortItem}.
	 * @param ctx the parse tree
	 */
	void exitSortItem(SqlBaseParser.SortItemContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#fromStatement}.
	 * @param ctx the parse tree
	 */
	void enterFromStatement(SqlBaseParser.FromStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#fromStatement}.
	 * @param ctx the parse tree
	 */
	void exitFromStatement(SqlBaseParser.FromStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#fromStatementBody}.
	 * @param ctx the parse tree
	 */
	void enterFromStatementBody(SqlBaseParser.FromStatementBodyContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#fromStatementBody}.
	 * @param ctx the parse tree
	 */
	void exitFromStatementBody(SqlBaseParser.FromStatementBodyContext ctx);
	/**
	 * Enter a parse tree produced by the {@code transformQuerySpecification}
	 * labeled alternative in {@link SqlBaseParser#querySpecification}.
	 * @param ctx the parse tree
	 */
	void enterTransformQuerySpecification(SqlBaseParser.TransformQuerySpecificationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code transformQuerySpecification}
	 * labeled alternative in {@link SqlBaseParser#querySpecification}.
	 * @param ctx the parse tree
	 */
	void exitTransformQuerySpecification(SqlBaseParser.TransformQuerySpecificationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code regularQuerySpecification}
	 * labeled alternative in {@link SqlBaseParser#querySpecification}.
	 * @param ctx the parse tree
	 */
	void enterRegularQuerySpecification(SqlBaseParser.RegularQuerySpecificationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code regularQuerySpecification}
	 * labeled alternative in {@link SqlBaseParser#querySpecification}.
	 * @param ctx the parse tree
	 */
	void exitRegularQuerySpecification(SqlBaseParser.RegularQuerySpecificationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#transformClause}.
	 * @param ctx the parse tree
	 */
	void enterTransformClause(SqlBaseParser.TransformClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#transformClause}.
	 * @param ctx the parse tree
	 */
	void exitTransformClause(SqlBaseParser.TransformClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#selectClause}.
	 * @param ctx the parse tree
	 */
	void enterSelectClause(SqlBaseParser.SelectClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#selectClause}.
	 * @param ctx the parse tree
	 */
	void exitSelectClause(SqlBaseParser.SelectClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#setClause}.
	 * @param ctx the parse tree
	 */
	void enterSetClause(SqlBaseParser.SetClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#setClause}.
	 * @param ctx the parse tree
	 */
	void exitSetClause(SqlBaseParser.SetClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#matchedClause}.
	 * @param ctx the parse tree
	 */
	void enterMatchedClause(SqlBaseParser.MatchedClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#matchedClause}.
	 * @param ctx the parse tree
	 */
	void exitMatchedClause(SqlBaseParser.MatchedClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#notMatchedClause}.
	 * @param ctx the parse tree
	 */
	void enterNotMatchedClause(SqlBaseParser.NotMatchedClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#notMatchedClause}.
	 * @param ctx the parse tree
	 */
	void exitNotMatchedClause(SqlBaseParser.NotMatchedClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#notMatchedBySourceClause}.
	 * @param ctx the parse tree
	 */
	void enterNotMatchedBySourceClause(SqlBaseParser.NotMatchedBySourceClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#notMatchedBySourceClause}.
	 * @param ctx the parse tree
	 */
	void exitNotMatchedBySourceClause(SqlBaseParser.NotMatchedBySourceClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#matchedAction}.
	 * @param ctx the parse tree
	 */
	void enterMatchedAction(SqlBaseParser.MatchedActionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#matchedAction}.
	 * @param ctx the parse tree
	 */
	void exitMatchedAction(SqlBaseParser.MatchedActionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#notMatchedAction}.
	 * @param ctx the parse tree
	 */
	void enterNotMatchedAction(SqlBaseParser.NotMatchedActionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#notMatchedAction}.
	 * @param ctx the parse tree
	 */
	void exitNotMatchedAction(SqlBaseParser.NotMatchedActionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#notMatchedBySourceAction}.
	 * @param ctx the parse tree
	 */
	void enterNotMatchedBySourceAction(SqlBaseParser.NotMatchedBySourceActionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#notMatchedBySourceAction}.
	 * @param ctx the parse tree
	 */
	void exitNotMatchedBySourceAction(SqlBaseParser.NotMatchedBySourceActionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#exceptClause}.
	 * @param ctx the parse tree
	 */
	void enterExceptClause(SqlBaseParser.ExceptClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#exceptClause}.
	 * @param ctx the parse tree
	 */
	void exitExceptClause(SqlBaseParser.ExceptClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#assignmentList}.
	 * @param ctx the parse tree
	 */
	void enterAssignmentList(SqlBaseParser.AssignmentListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#assignmentList}.
	 * @param ctx the parse tree
	 */
	void exitAssignmentList(SqlBaseParser.AssignmentListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#assignment}.
	 * @param ctx the parse tree
	 */
	void enterAssignment(SqlBaseParser.AssignmentContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#assignment}.
	 * @param ctx the parse tree
	 */
	void exitAssignment(SqlBaseParser.AssignmentContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#whereClause}.
	 * @param ctx the parse tree
	 */
	void enterWhereClause(SqlBaseParser.WhereClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#whereClause}.
	 * @param ctx the parse tree
	 */
	void exitWhereClause(SqlBaseParser.WhereClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#havingClause}.
	 * @param ctx the parse tree
	 */
	void enterHavingClause(SqlBaseParser.HavingClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#havingClause}.
	 * @param ctx the parse tree
	 */
	void exitHavingClause(SqlBaseParser.HavingClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#hint}.
	 * @param ctx the parse tree
	 */
	void enterHint(SqlBaseParser.HintContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#hint}.
	 * @param ctx the parse tree
	 */
	void exitHint(SqlBaseParser.HintContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#hintStatement}.
	 * @param ctx the parse tree
	 */
	void enterHintStatement(SqlBaseParser.HintStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#hintStatement}.
	 * @param ctx the parse tree
	 */
	void exitHintStatement(SqlBaseParser.HintStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#fromClause}.
	 * @param ctx the parse tree
	 */
	void enterFromClause(SqlBaseParser.FromClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#fromClause}.
	 * @param ctx the parse tree
	 */
	void exitFromClause(SqlBaseParser.FromClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#temporalClause}.
	 * @param ctx the parse tree
	 */
	void enterTemporalClause(SqlBaseParser.TemporalClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#temporalClause}.
	 * @param ctx the parse tree
	 */
	void exitTemporalClause(SqlBaseParser.TemporalClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#aggregationClause}.
	 * @param ctx the parse tree
	 */
	void enterAggregationClause(SqlBaseParser.AggregationClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#aggregationClause}.
	 * @param ctx the parse tree
	 */
	void exitAggregationClause(SqlBaseParser.AggregationClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#groupByClause}.
	 * @param ctx the parse tree
	 */
	void enterGroupByClause(SqlBaseParser.GroupByClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#groupByClause}.
	 * @param ctx the parse tree
	 */
	void exitGroupByClause(SqlBaseParser.GroupByClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#groupingAnalytics}.
	 * @param ctx the parse tree
	 */
	void enterGroupingAnalytics(SqlBaseParser.GroupingAnalyticsContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#groupingAnalytics}.
	 * @param ctx the parse tree
	 */
	void exitGroupingAnalytics(SqlBaseParser.GroupingAnalyticsContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void enterGroupingElement(SqlBaseParser.GroupingElementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#groupingElement}.
	 * @param ctx the parse tree
	 */
	void exitGroupingElement(SqlBaseParser.GroupingElementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#groupingSet}.
	 * @param ctx the parse tree
	 */
	void enterGroupingSet(SqlBaseParser.GroupingSetContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#groupingSet}.
	 * @param ctx the parse tree
	 */
	void exitGroupingSet(SqlBaseParser.GroupingSetContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#pivotClause}.
	 * @param ctx the parse tree
	 */
	void enterPivotClause(SqlBaseParser.PivotClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#pivotClause}.
	 * @param ctx the parse tree
	 */
	void exitPivotClause(SqlBaseParser.PivotClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#pivotColumn}.
	 * @param ctx the parse tree
	 */
	void enterPivotColumn(SqlBaseParser.PivotColumnContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#pivotColumn}.
	 * @param ctx the parse tree
	 */
	void exitPivotColumn(SqlBaseParser.PivotColumnContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#pivotValue}.
	 * @param ctx the parse tree
	 */
	void enterPivotValue(SqlBaseParser.PivotValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#pivotValue}.
	 * @param ctx the parse tree
	 */
	void exitPivotValue(SqlBaseParser.PivotValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#unpivotClause}.
	 * @param ctx the parse tree
	 */
	void enterUnpivotClause(SqlBaseParser.UnpivotClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#unpivotClause}.
	 * @param ctx the parse tree
	 */
	void exitUnpivotClause(SqlBaseParser.UnpivotClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#unpivotNullClause}.
	 * @param ctx the parse tree
	 */
	void enterUnpivotNullClause(SqlBaseParser.UnpivotNullClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#unpivotNullClause}.
	 * @param ctx the parse tree
	 */
	void exitUnpivotNullClause(SqlBaseParser.UnpivotNullClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#unpivotOperator}.
	 * @param ctx the parse tree
	 */
	void enterUnpivotOperator(SqlBaseParser.UnpivotOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#unpivotOperator}.
	 * @param ctx the parse tree
	 */
	void exitUnpivotOperator(SqlBaseParser.UnpivotOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#unpivotSingleValueColumnClause}.
	 * @param ctx the parse tree
	 */
	void enterUnpivotSingleValueColumnClause(SqlBaseParser.UnpivotSingleValueColumnClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#unpivotSingleValueColumnClause}.
	 * @param ctx the parse tree
	 */
	void exitUnpivotSingleValueColumnClause(SqlBaseParser.UnpivotSingleValueColumnClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#unpivotMultiValueColumnClause}.
	 * @param ctx the parse tree
	 */
	void enterUnpivotMultiValueColumnClause(SqlBaseParser.UnpivotMultiValueColumnClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#unpivotMultiValueColumnClause}.
	 * @param ctx the parse tree
	 */
	void exitUnpivotMultiValueColumnClause(SqlBaseParser.UnpivotMultiValueColumnClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#unpivotColumnSet}.
	 * @param ctx the parse tree
	 */
	void enterUnpivotColumnSet(SqlBaseParser.UnpivotColumnSetContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#unpivotColumnSet}.
	 * @param ctx the parse tree
	 */
	void exitUnpivotColumnSet(SqlBaseParser.UnpivotColumnSetContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#unpivotValueColumn}.
	 * @param ctx the parse tree
	 */
	void enterUnpivotValueColumn(SqlBaseParser.UnpivotValueColumnContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#unpivotValueColumn}.
	 * @param ctx the parse tree
	 */
	void exitUnpivotValueColumn(SqlBaseParser.UnpivotValueColumnContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#unpivotNameColumn}.
	 * @param ctx the parse tree
	 */
	void enterUnpivotNameColumn(SqlBaseParser.UnpivotNameColumnContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#unpivotNameColumn}.
	 * @param ctx the parse tree
	 */
	void exitUnpivotNameColumn(SqlBaseParser.UnpivotNameColumnContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#unpivotColumnAndAlias}.
	 * @param ctx the parse tree
	 */
	void enterUnpivotColumnAndAlias(SqlBaseParser.UnpivotColumnAndAliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#unpivotColumnAndAlias}.
	 * @param ctx the parse tree
	 */
	void exitUnpivotColumnAndAlias(SqlBaseParser.UnpivotColumnAndAliasContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#unpivotColumn}.
	 * @param ctx the parse tree
	 */
	void enterUnpivotColumn(SqlBaseParser.UnpivotColumnContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#unpivotColumn}.
	 * @param ctx the parse tree
	 */
	void exitUnpivotColumn(SqlBaseParser.UnpivotColumnContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#unpivotAlias}.
	 * @param ctx the parse tree
	 */
	void enterUnpivotAlias(SqlBaseParser.UnpivotAliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#unpivotAlias}.
	 * @param ctx the parse tree
	 */
	void exitUnpivotAlias(SqlBaseParser.UnpivotAliasContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#lateralView}.
	 * @param ctx the parse tree
	 */
	void enterLateralView(SqlBaseParser.LateralViewContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#lateralView}.
	 * @param ctx the parse tree
	 */
	void exitLateralView(SqlBaseParser.LateralViewContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#setQuantifier}.
	 * @param ctx the parse tree
	 */
	void enterSetQuantifier(SqlBaseParser.SetQuantifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#setQuantifier}.
	 * @param ctx the parse tree
	 */
	void exitSetQuantifier(SqlBaseParser.SetQuantifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#relation}.
	 * @param ctx the parse tree
	 */
	void enterRelation(SqlBaseParser.RelationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#relation}.
	 * @param ctx the parse tree
	 */
	void exitRelation(SqlBaseParser.RelationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#relationExtension}.
	 * @param ctx the parse tree
	 */
	void enterRelationExtension(SqlBaseParser.RelationExtensionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#relationExtension}.
	 * @param ctx the parse tree
	 */
	void exitRelationExtension(SqlBaseParser.RelationExtensionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#joinRelation}.
	 * @param ctx the parse tree
	 */
	void enterJoinRelation(SqlBaseParser.JoinRelationContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#joinRelation}.
	 * @param ctx the parse tree
	 */
	void exitJoinRelation(SqlBaseParser.JoinRelationContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#joinType}.
	 * @param ctx the parse tree
	 */
	void enterJoinType(SqlBaseParser.JoinTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#joinType}.
	 * @param ctx the parse tree
	 */
	void exitJoinType(SqlBaseParser.JoinTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#joinCriteria}.
	 * @param ctx the parse tree
	 */
	void enterJoinCriteria(SqlBaseParser.JoinCriteriaContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#joinCriteria}.
	 * @param ctx the parse tree
	 */
	void exitJoinCriteria(SqlBaseParser.JoinCriteriaContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#sample}.
	 * @param ctx the parse tree
	 */
	void enterSample(SqlBaseParser.SampleContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#sample}.
	 * @param ctx the parse tree
	 */
	void exitSample(SqlBaseParser.SampleContext ctx);
	/**
	 * Enter a parse tree produced by the {@code sampleByPercentile}
	 * labeled alternative in {@link SqlBaseParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	void enterSampleByPercentile(SqlBaseParser.SampleByPercentileContext ctx);
	/**
	 * Exit a parse tree produced by the {@code sampleByPercentile}
	 * labeled alternative in {@link SqlBaseParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	void exitSampleByPercentile(SqlBaseParser.SampleByPercentileContext ctx);
	/**
	 * Enter a parse tree produced by the {@code sampleByRows}
	 * labeled alternative in {@link SqlBaseParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	void enterSampleByRows(SqlBaseParser.SampleByRowsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code sampleByRows}
	 * labeled alternative in {@link SqlBaseParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	void exitSampleByRows(SqlBaseParser.SampleByRowsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code sampleByBucket}
	 * labeled alternative in {@link SqlBaseParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	void enterSampleByBucket(SqlBaseParser.SampleByBucketContext ctx);
	/**
	 * Exit a parse tree produced by the {@code sampleByBucket}
	 * labeled alternative in {@link SqlBaseParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	void exitSampleByBucket(SqlBaseParser.SampleByBucketContext ctx);
	/**
	 * Enter a parse tree produced by the {@code sampleByBytes}
	 * labeled alternative in {@link SqlBaseParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	void enterSampleByBytes(SqlBaseParser.SampleByBytesContext ctx);
	/**
	 * Exit a parse tree produced by the {@code sampleByBytes}
	 * labeled alternative in {@link SqlBaseParser#sampleMethod}.
	 * @param ctx the parse tree
	 */
	void exitSampleByBytes(SqlBaseParser.SampleByBytesContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#identifierList}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierList(SqlBaseParser.IdentifierListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#identifierList}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierList(SqlBaseParser.IdentifierListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#identifierSeq}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierSeq(SqlBaseParser.IdentifierSeqContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#identifierSeq}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierSeq(SqlBaseParser.IdentifierSeqContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#orderedIdentifierList}.
	 * @param ctx the parse tree
	 */
	void enterOrderedIdentifierList(SqlBaseParser.OrderedIdentifierListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#orderedIdentifierList}.
	 * @param ctx the parse tree
	 */
	void exitOrderedIdentifierList(SqlBaseParser.OrderedIdentifierListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#orderedIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterOrderedIdentifier(SqlBaseParser.OrderedIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#orderedIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitOrderedIdentifier(SqlBaseParser.OrderedIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#identifierCommentList}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierCommentList(SqlBaseParser.IdentifierCommentListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#identifierCommentList}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierCommentList(SqlBaseParser.IdentifierCommentListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#identifierComment}.
	 * @param ctx the parse tree
	 */
	void enterIdentifierComment(SqlBaseParser.IdentifierCommentContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#identifierComment}.
	 * @param ctx the parse tree
	 */
	void exitIdentifierComment(SqlBaseParser.IdentifierCommentContext ctx);
	/**
	 * Enter a parse tree produced by the {@code streamRelation}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterStreamRelation(SqlBaseParser.StreamRelationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code streamRelation}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitStreamRelation(SqlBaseParser.StreamRelationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterTableName(SqlBaseParser.TableNameContext ctx);
	/**
	 * Exit a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitTableName(SqlBaseParser.TableNameContext ctx);
	/**
	 * Enter a parse tree produced by the {@code aliasedQuery}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterAliasedQuery(SqlBaseParser.AliasedQueryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code aliasedQuery}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitAliasedQuery(SqlBaseParser.AliasedQueryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code aliasedRelation}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterAliasedRelation(SqlBaseParser.AliasedRelationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code aliasedRelation}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitAliasedRelation(SqlBaseParser.AliasedRelationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code inlineTableDefault2}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterInlineTableDefault2(SqlBaseParser.InlineTableDefault2Context ctx);
	/**
	 * Exit a parse tree produced by the {@code inlineTableDefault2}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitInlineTableDefault2(SqlBaseParser.InlineTableDefault2Context ctx);
	/**
	 * Enter a parse tree produced by the {@code tableValuedFunction}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void enterTableValuedFunction(SqlBaseParser.TableValuedFunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code tableValuedFunction}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 */
	void exitTableValuedFunction(SqlBaseParser.TableValuedFunctionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#optionsClause}.
	 * @param ctx the parse tree
	 */
	void enterOptionsClause(SqlBaseParser.OptionsClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#optionsClause}.
	 * @param ctx the parse tree
	 */
	void exitOptionsClause(SqlBaseParser.OptionsClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#inlineTable}.
	 * @param ctx the parse tree
	 */
	void enterInlineTable(SqlBaseParser.InlineTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#inlineTable}.
	 * @param ctx the parse tree
	 */
	void exitInlineTable(SqlBaseParser.InlineTableContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#functionTableSubqueryArgument}.
	 * @param ctx the parse tree
	 */
	void enterFunctionTableSubqueryArgument(SqlBaseParser.FunctionTableSubqueryArgumentContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#functionTableSubqueryArgument}.
	 * @param ctx the parse tree
	 */
	void exitFunctionTableSubqueryArgument(SqlBaseParser.FunctionTableSubqueryArgumentContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#tableArgumentPartitioning}.
	 * @param ctx the parse tree
	 */
	void enterTableArgumentPartitioning(SqlBaseParser.TableArgumentPartitioningContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#tableArgumentPartitioning}.
	 * @param ctx the parse tree
	 */
	void exitTableArgumentPartitioning(SqlBaseParser.TableArgumentPartitioningContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#functionTableNamedArgumentExpression}.
	 * @param ctx the parse tree
	 */
	void enterFunctionTableNamedArgumentExpression(SqlBaseParser.FunctionTableNamedArgumentExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#functionTableNamedArgumentExpression}.
	 * @param ctx the parse tree
	 */
	void exitFunctionTableNamedArgumentExpression(SqlBaseParser.FunctionTableNamedArgumentExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#functionTableReferenceArgument}.
	 * @param ctx the parse tree
	 */
	void enterFunctionTableReferenceArgument(SqlBaseParser.FunctionTableReferenceArgumentContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#functionTableReferenceArgument}.
	 * @param ctx the parse tree
	 */
	void exitFunctionTableReferenceArgument(SqlBaseParser.FunctionTableReferenceArgumentContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#functionTableArgument}.
	 * @param ctx the parse tree
	 */
	void enterFunctionTableArgument(SqlBaseParser.FunctionTableArgumentContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#functionTableArgument}.
	 * @param ctx the parse tree
	 */
	void exitFunctionTableArgument(SqlBaseParser.FunctionTableArgumentContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#functionTable}.
	 * @param ctx the parse tree
	 */
	void enterFunctionTable(SqlBaseParser.FunctionTableContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#functionTable}.
	 * @param ctx the parse tree
	 */
	void exitFunctionTable(SqlBaseParser.FunctionTableContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#tableAlias}.
	 * @param ctx the parse tree
	 */
	void enterTableAlias(SqlBaseParser.TableAliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#tableAlias}.
	 * @param ctx the parse tree
	 */
	void exitTableAlias(SqlBaseParser.TableAliasContext ctx);
	/**
	 * Enter a parse tree produced by the {@code rowFormatSerde}
	 * labeled alternative in {@link SqlBaseParser#rowFormat}.
	 * @param ctx the parse tree
	 */
	void enterRowFormatSerde(SqlBaseParser.RowFormatSerdeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code rowFormatSerde}
	 * labeled alternative in {@link SqlBaseParser#rowFormat}.
	 * @param ctx the parse tree
	 */
	void exitRowFormatSerde(SqlBaseParser.RowFormatSerdeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code rowFormatDelimited}
	 * labeled alternative in {@link SqlBaseParser#rowFormat}.
	 * @param ctx the parse tree
	 */
	void enterRowFormatDelimited(SqlBaseParser.RowFormatDelimitedContext ctx);
	/**
	 * Exit a parse tree produced by the {@code rowFormatDelimited}
	 * labeled alternative in {@link SqlBaseParser#rowFormat}.
	 * @param ctx the parse tree
	 */
	void exitRowFormatDelimited(SqlBaseParser.RowFormatDelimitedContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#multipartIdentifierList}.
	 * @param ctx the parse tree
	 */
	void enterMultipartIdentifierList(SqlBaseParser.MultipartIdentifierListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#multipartIdentifierList}.
	 * @param ctx the parse tree
	 */
	void exitMultipartIdentifierList(SqlBaseParser.MultipartIdentifierListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#multipartIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterMultipartIdentifier(SqlBaseParser.MultipartIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#multipartIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitMultipartIdentifier(SqlBaseParser.MultipartIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#multipartIdentifierPropertyList}.
	 * @param ctx the parse tree
	 */
	void enterMultipartIdentifierPropertyList(SqlBaseParser.MultipartIdentifierPropertyListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#multipartIdentifierPropertyList}.
	 * @param ctx the parse tree
	 */
	void exitMultipartIdentifierPropertyList(SqlBaseParser.MultipartIdentifierPropertyListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#multipartIdentifierProperty}.
	 * @param ctx the parse tree
	 */
	void enterMultipartIdentifierProperty(SqlBaseParser.MultipartIdentifierPropertyContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#multipartIdentifierProperty}.
	 * @param ctx the parse tree
	 */
	void exitMultipartIdentifierProperty(SqlBaseParser.MultipartIdentifierPropertyContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#tableIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterTableIdentifier(SqlBaseParser.TableIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#tableIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitTableIdentifier(SqlBaseParser.TableIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#functionIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterFunctionIdentifier(SqlBaseParser.FunctionIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#functionIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitFunctionIdentifier(SqlBaseParser.FunctionIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#namedExpression}.
	 * @param ctx the parse tree
	 */
	void enterNamedExpression(SqlBaseParser.NamedExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#namedExpression}.
	 * @param ctx the parse tree
	 */
	void exitNamedExpression(SqlBaseParser.NamedExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#namedExpressionSeq}.
	 * @param ctx the parse tree
	 */
	void enterNamedExpressionSeq(SqlBaseParser.NamedExpressionSeqContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#namedExpressionSeq}.
	 * @param ctx the parse tree
	 */
	void exitNamedExpressionSeq(SqlBaseParser.NamedExpressionSeqContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#partitionFieldList}.
	 * @param ctx the parse tree
	 */
	void enterPartitionFieldList(SqlBaseParser.PartitionFieldListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#partitionFieldList}.
	 * @param ctx the parse tree
	 */
	void exitPartitionFieldList(SqlBaseParser.PartitionFieldListContext ctx);
	/**
	 * Enter a parse tree produced by the {@code partitionTransform}
	 * labeled alternative in {@link SqlBaseParser#partitionField}.
	 * @param ctx the parse tree
	 */
	void enterPartitionTransform(SqlBaseParser.PartitionTransformContext ctx);
	/**
	 * Exit a parse tree produced by the {@code partitionTransform}
	 * labeled alternative in {@link SqlBaseParser#partitionField}.
	 * @param ctx the parse tree
	 */
	void exitPartitionTransform(SqlBaseParser.PartitionTransformContext ctx);
	/**
	 * Enter a parse tree produced by the {@code partitionColumn}
	 * labeled alternative in {@link SqlBaseParser#partitionField}.
	 * @param ctx the parse tree
	 */
	void enterPartitionColumn(SqlBaseParser.PartitionColumnContext ctx);
	/**
	 * Exit a parse tree produced by the {@code partitionColumn}
	 * labeled alternative in {@link SqlBaseParser#partitionField}.
	 * @param ctx the parse tree
	 */
	void exitPartitionColumn(SqlBaseParser.PartitionColumnContext ctx);
	/**
	 * Enter a parse tree produced by the {@code identityTransform}
	 * labeled alternative in {@link SqlBaseParser#transform}.
	 * @param ctx the parse tree
	 */
	void enterIdentityTransform(SqlBaseParser.IdentityTransformContext ctx);
	/**
	 * Exit a parse tree produced by the {@code identityTransform}
	 * labeled alternative in {@link SqlBaseParser#transform}.
	 * @param ctx the parse tree
	 */
	void exitIdentityTransform(SqlBaseParser.IdentityTransformContext ctx);
	/**
	 * Enter a parse tree produced by the {@code applyTransform}
	 * labeled alternative in {@link SqlBaseParser#transform}.
	 * @param ctx the parse tree
	 */
	void enterApplyTransform(SqlBaseParser.ApplyTransformContext ctx);
	/**
	 * Exit a parse tree produced by the {@code applyTransform}
	 * labeled alternative in {@link SqlBaseParser#transform}.
	 * @param ctx the parse tree
	 */
	void exitApplyTransform(SqlBaseParser.ApplyTransformContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#transformArgument}.
	 * @param ctx the parse tree
	 */
	void enterTransformArgument(SqlBaseParser.TransformArgumentContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#transformArgument}.
	 * @param ctx the parse tree
	 */
	void exitTransformArgument(SqlBaseParser.TransformArgumentContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterExpression(SqlBaseParser.ExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitExpression(SqlBaseParser.ExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#namedArgumentExpression}.
	 * @param ctx the parse tree
	 */
	void enterNamedArgumentExpression(SqlBaseParser.NamedArgumentExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#namedArgumentExpression}.
	 * @param ctx the parse tree
	 */
	void exitNamedArgumentExpression(SqlBaseParser.NamedArgumentExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#functionArgument}.
	 * @param ctx the parse tree
	 */
	void enterFunctionArgument(SqlBaseParser.FunctionArgumentContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#functionArgument}.
	 * @param ctx the parse tree
	 */
	void exitFunctionArgument(SqlBaseParser.FunctionArgumentContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#expressionSeq}.
	 * @param ctx the parse tree
	 */
	void enterExpressionSeq(SqlBaseParser.ExpressionSeqContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#expressionSeq}.
	 * @param ctx the parse tree
	 */
	void exitExpressionSeq(SqlBaseParser.ExpressionSeqContext ctx);
	/**
	 * Enter a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterLogicalNot(SqlBaseParser.LogicalNotContext ctx);
	/**
	 * Exit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitLogicalNot(SqlBaseParser.LogicalNotContext ctx);
	/**
	 * Enter a parse tree produced by the {@code predicated}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterPredicated(SqlBaseParser.PredicatedContext ctx);
	/**
	 * Exit a parse tree produced by the {@code predicated}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitPredicated(SqlBaseParser.PredicatedContext ctx);
	/**
	 * Enter a parse tree produced by the {@code exists}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterExists(SqlBaseParser.ExistsContext ctx);
	/**
	 * Exit a parse tree produced by the {@code exists}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitExists(SqlBaseParser.ExistsContext ctx);
	/**
	 * Enter a parse tree produced by the {@code logicalBinary}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void enterLogicalBinary(SqlBaseParser.LogicalBinaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code logicalBinary}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 */
	void exitLogicalBinary(SqlBaseParser.LogicalBinaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterPredicate(SqlBaseParser.PredicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitPredicate(SqlBaseParser.PredicateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#errorCapturingNot}.
	 * @param ctx the parse tree
	 */
	void enterErrorCapturingNot(SqlBaseParser.ErrorCapturingNotContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#errorCapturingNot}.
	 * @param ctx the parse tree
	 */
	void exitErrorCapturingNot(SqlBaseParser.ErrorCapturingNotContext ctx);
	/**
	 * Enter a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterValueExpressionDefault(SqlBaseParser.ValueExpressionDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitValueExpressionDefault(SqlBaseParser.ValueExpressionDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterComparison(SqlBaseParser.ComparisonContext ctx);
	/**
	 * Exit a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitComparison(SqlBaseParser.ComparisonContext ctx);
	/**
	 * Enter a parse tree produced by the {@code shiftExpression}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterShiftExpression(SqlBaseParser.ShiftExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code shiftExpression}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitShiftExpression(SqlBaseParser.ShiftExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext ctx);
	/**
	 * Enter a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void enterArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext ctx);
	/**
	 * Exit a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	void exitArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#shiftOperator}.
	 * @param ctx the parse tree
	 */
	void enterShiftOperator(SqlBaseParser.ShiftOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#shiftOperator}.
	 * @param ctx the parse tree
	 */
	void exitShiftOperator(SqlBaseParser.ShiftOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#datetimeUnit}.
	 * @param ctx the parse tree
	 */
	void enterDatetimeUnit(SqlBaseParser.DatetimeUnitContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#datetimeUnit}.
	 * @param ctx the parse tree
	 */
	void exitDatetimeUnit(SqlBaseParser.DatetimeUnitContext ctx);
	/**
	 * Enter a parse tree produced by the {@code struct}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterStruct(SqlBaseParser.StructContext ctx);
	/**
	 * Exit a parse tree produced by the {@code struct}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitStruct(SqlBaseParser.StructContext ctx);
	/**
	 * Enter a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterDereference(SqlBaseParser.DereferenceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitDereference(SqlBaseParser.DereferenceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code castByColon}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterCastByColon(SqlBaseParser.CastByColonContext ctx);
	/**
	 * Exit a parse tree produced by the {@code castByColon}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitCastByColon(SqlBaseParser.CastByColonContext ctx);
	/**
	 * Enter a parse tree produced by the {@code timestampadd}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterTimestampadd(SqlBaseParser.TimestampaddContext ctx);
	/**
	 * Exit a parse tree produced by the {@code timestampadd}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitTimestampadd(SqlBaseParser.TimestampaddContext ctx);
	/**
	 * Enter a parse tree produced by the {@code substring}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSubstring(SqlBaseParser.SubstringContext ctx);
	/**
	 * Exit a parse tree produced by the {@code substring}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSubstring(SqlBaseParser.SubstringContext ctx);
	/**
	 * Enter a parse tree produced by the {@code cast}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterCast(SqlBaseParser.CastContext ctx);
	/**
	 * Exit a parse tree produced by the {@code cast}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitCast(SqlBaseParser.CastContext ctx);
	/**
	 * Enter a parse tree produced by the {@code lambda}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterLambda(SqlBaseParser.LambdaContext ctx);
	/**
	 * Exit a parse tree produced by the {@code lambda}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitLambda(SqlBaseParser.LambdaContext ctx);
	/**
	 * Enter a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code any_value}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterAny_value(SqlBaseParser.Any_valueContext ctx);
	/**
	 * Exit a parse tree produced by the {@code any_value}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitAny_value(SqlBaseParser.Any_valueContext ctx);
	/**
	 * Enter a parse tree produced by the {@code trim}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterTrim(SqlBaseParser.TrimContext ctx);
	/**
	 * Exit a parse tree produced by the {@code trim}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitTrim(SqlBaseParser.TrimContext ctx);
	/**
	 * Enter a parse tree produced by the {@code semiStructuredExtract}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSemiStructuredExtract(SqlBaseParser.SemiStructuredExtractContext ctx);
	/**
	 * Exit a parse tree produced by the {@code semiStructuredExtract}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSemiStructuredExtract(SqlBaseParser.SemiStructuredExtractContext ctx);
	/**
	 * Enter a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSimpleCase(SqlBaseParser.SimpleCaseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSimpleCase(SqlBaseParser.SimpleCaseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code currentLike}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterCurrentLike(SqlBaseParser.CurrentLikeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code currentLike}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitCurrentLike(SqlBaseParser.CurrentLikeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterColumnReference(SqlBaseParser.ColumnReferenceContext ctx);
	/**
	 * Exit a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitColumnReference(SqlBaseParser.ColumnReferenceContext ctx);
	/**
	 * Enter a parse tree produced by the {@code rowConstructor}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterRowConstructor(SqlBaseParser.RowConstructorContext ctx);
	/**
	 * Exit a parse tree produced by the {@code rowConstructor}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitRowConstructor(SqlBaseParser.RowConstructorContext ctx);
	/**
	 * Enter a parse tree produced by the {@code last}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterLast(SqlBaseParser.LastContext ctx);
	/**
	 * Exit a parse tree produced by the {@code last}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitLast(SqlBaseParser.LastContext ctx);
	/**
	 * Enter a parse tree produced by the {@code star}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterStar(SqlBaseParser.StarContext ctx);
	/**
	 * Exit a parse tree produced by the {@code star}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitStar(SqlBaseParser.StarContext ctx);
	/**
	 * Enter a parse tree produced by the {@code overlay}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterOverlay(SqlBaseParser.OverlayContext ctx);
	/**
	 * Exit a parse tree produced by the {@code overlay}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitOverlay(SqlBaseParser.OverlayContext ctx);
	/**
	 * Enter a parse tree produced by the {@code subscript}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSubscript(SqlBaseParser.SubscriptContext ctx);
	/**
	 * Exit a parse tree produced by the {@code subscript}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSubscript(SqlBaseParser.SubscriptContext ctx);
	/**
	 * Enter a parse tree produced by the {@code timestampdiff}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterTimestampdiff(SqlBaseParser.TimestampdiffContext ctx);
	/**
	 * Exit a parse tree produced by the {@code timestampdiff}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitTimestampdiff(SqlBaseParser.TimestampdiffContext ctx);
	/**
	 * Enter a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSubqueryExpression(SqlBaseParser.SubqueryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code collate}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterCollate(SqlBaseParser.CollateContext ctx);
	/**
	 * Exit a parse tree produced by the {@code collate}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitCollate(SqlBaseParser.CollateContext ctx);
	/**
	 * Enter a parse tree produced by the {@code constantDefault}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterConstantDefault(SqlBaseParser.ConstantDefaultContext ctx);
	/**
	 * Exit a parse tree produced by the {@code constantDefault}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitConstantDefault(SqlBaseParser.ConstantDefaultContext ctx);
	/**
	 * Enter a parse tree produced by the {@code extract}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterExtract(SqlBaseParser.ExtractContext ctx);
	/**
	 * Exit a parse tree produced by the {@code extract}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitExtract(SqlBaseParser.ExtractContext ctx);
	/**
	 * Enter a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterFunctionCall(SqlBaseParser.FunctionCallContext ctx);
	/**
	 * Exit a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitFunctionCall(SqlBaseParser.FunctionCallContext ctx);
	/**
	 * Enter a parse tree produced by the {@code searchedCase}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterSearchedCase(SqlBaseParser.SearchedCaseContext ctx);
	/**
	 * Exit a parse tree produced by the {@code searchedCase}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitSearchedCase(SqlBaseParser.SearchedCaseContext ctx);
	/**
	 * Enter a parse tree produced by the {@code position}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterPosition(SqlBaseParser.PositionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code position}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitPosition(SqlBaseParser.PositionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code first}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterFirst(SqlBaseParser.FirstContext ctx);
	/**
	 * Exit a parse tree produced by the {@code first}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitFirst(SqlBaseParser.FirstContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#semiStructuredExtractionPath}.
	 * @param ctx the parse tree
	 */
	void enterSemiStructuredExtractionPath(SqlBaseParser.SemiStructuredExtractionPathContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#semiStructuredExtractionPath}.
	 * @param ctx the parse tree
	 */
	void exitSemiStructuredExtractionPath(SqlBaseParser.SemiStructuredExtractionPathContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#jsonPathIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterJsonPathIdentifier(SqlBaseParser.JsonPathIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#jsonPathIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitJsonPathIdentifier(SqlBaseParser.JsonPathIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#jsonPathBracketedIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterJsonPathBracketedIdentifier(SqlBaseParser.JsonPathBracketedIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#jsonPathBracketedIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitJsonPathBracketedIdentifier(SqlBaseParser.JsonPathBracketedIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#jsonPathFirstPart}.
	 * @param ctx the parse tree
	 */
	void enterJsonPathFirstPart(SqlBaseParser.JsonPathFirstPartContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#jsonPathFirstPart}.
	 * @param ctx the parse tree
	 */
	void exitJsonPathFirstPart(SqlBaseParser.JsonPathFirstPartContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#jsonPathParts}.
	 * @param ctx the parse tree
	 */
	void enterJsonPathParts(SqlBaseParser.JsonPathPartsContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#jsonPathParts}.
	 * @param ctx the parse tree
	 */
	void exitJsonPathParts(SqlBaseParser.JsonPathPartsContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#literalType}.
	 * @param ctx the parse tree
	 */
	void enterLiteralType(SqlBaseParser.LiteralTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#literalType}.
	 * @param ctx the parse tree
	 */
	void exitLiteralType(SqlBaseParser.LiteralTypeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterNullLiteral(SqlBaseParser.NullLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitNullLiteral(SqlBaseParser.NullLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code posParameterLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterPosParameterLiteral(SqlBaseParser.PosParameterLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code posParameterLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitPosParameterLiteral(SqlBaseParser.PosParameterLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code namedParameterLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterNamedParameterLiteral(SqlBaseParser.NamedParameterLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code namedParameterLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitNamedParameterLiteral(SqlBaseParser.NamedParameterLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code intervalLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterIntervalLiteral(SqlBaseParser.IntervalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code intervalLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitIntervalLiteral(SqlBaseParser.IntervalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterTypeConstructor(SqlBaseParser.TypeConstructorContext ctx);
	/**
	 * Exit a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitTypeConstructor(SqlBaseParser.TypeConstructorContext ctx);
	/**
	 * Enter a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterNumericLiteral(SqlBaseParser.NumericLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitNumericLiteral(SqlBaseParser.NumericLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterBooleanLiteral(SqlBaseParser.BooleanLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitBooleanLiteral(SqlBaseParser.BooleanLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 */
	void enterStringLiteral(SqlBaseParser.StringLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 */
	void exitStringLiteral(SqlBaseParser.StringLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
	void enterComparisonOperator(SqlBaseParser.ComparisonOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
	void exitComparisonOperator(SqlBaseParser.ComparisonOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#arithmeticOperator}.
	 * @param ctx the parse tree
	 */
	void enterArithmeticOperator(SqlBaseParser.ArithmeticOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#arithmeticOperator}.
	 * @param ctx the parse tree
	 */
	void exitArithmeticOperator(SqlBaseParser.ArithmeticOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#predicateOperator}.
	 * @param ctx the parse tree
	 */
	void enterPredicateOperator(SqlBaseParser.PredicateOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#predicateOperator}.
	 * @param ctx the parse tree
	 */
	void exitPredicateOperator(SqlBaseParser.PredicateOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	void enterBooleanValue(SqlBaseParser.BooleanValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#booleanValue}.
	 * @param ctx the parse tree
	 */
	void exitBooleanValue(SqlBaseParser.BooleanValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#interval}.
	 * @param ctx the parse tree
	 */
	void enterInterval(SqlBaseParser.IntervalContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#interval}.
	 * @param ctx the parse tree
	 */
	void exitInterval(SqlBaseParser.IntervalContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#errorCapturingMultiUnitsInterval}.
	 * @param ctx the parse tree
	 */
	void enterErrorCapturingMultiUnitsInterval(SqlBaseParser.ErrorCapturingMultiUnitsIntervalContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#errorCapturingMultiUnitsInterval}.
	 * @param ctx the parse tree
	 */
	void exitErrorCapturingMultiUnitsInterval(SqlBaseParser.ErrorCapturingMultiUnitsIntervalContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#multiUnitsInterval}.
	 * @param ctx the parse tree
	 */
	void enterMultiUnitsInterval(SqlBaseParser.MultiUnitsIntervalContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#multiUnitsInterval}.
	 * @param ctx the parse tree
	 */
	void exitMultiUnitsInterval(SqlBaseParser.MultiUnitsIntervalContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#errorCapturingUnitToUnitInterval}.
	 * @param ctx the parse tree
	 */
	void enterErrorCapturingUnitToUnitInterval(SqlBaseParser.ErrorCapturingUnitToUnitIntervalContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#errorCapturingUnitToUnitInterval}.
	 * @param ctx the parse tree
	 */
	void exitErrorCapturingUnitToUnitInterval(SqlBaseParser.ErrorCapturingUnitToUnitIntervalContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#unitToUnitInterval}.
	 * @param ctx the parse tree
	 */
	void enterUnitToUnitInterval(SqlBaseParser.UnitToUnitIntervalContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#unitToUnitInterval}.
	 * @param ctx the parse tree
	 */
	void exitUnitToUnitInterval(SqlBaseParser.UnitToUnitIntervalContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#intervalValue}.
	 * @param ctx the parse tree
	 */
	void enterIntervalValue(SqlBaseParser.IntervalValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#intervalValue}.
	 * @param ctx the parse tree
	 */
	void exitIntervalValue(SqlBaseParser.IntervalValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#unitInMultiUnits}.
	 * @param ctx the parse tree
	 */
	void enterUnitInMultiUnits(SqlBaseParser.UnitInMultiUnitsContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#unitInMultiUnits}.
	 * @param ctx the parse tree
	 */
	void exitUnitInMultiUnits(SqlBaseParser.UnitInMultiUnitsContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#unitInUnitToUnit}.
	 * @param ctx the parse tree
	 */
	void enterUnitInUnitToUnit(SqlBaseParser.UnitInUnitToUnitContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#unitInUnitToUnit}.
	 * @param ctx the parse tree
	 */
	void exitUnitInUnitToUnit(SqlBaseParser.UnitInUnitToUnitContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#colPosition}.
	 * @param ctx the parse tree
	 */
	void enterColPosition(SqlBaseParser.ColPositionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#colPosition}.
	 * @param ctx the parse tree
	 */
	void exitColPosition(SqlBaseParser.ColPositionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#collationSpec}.
	 * @param ctx the parse tree
	 */
	void enterCollationSpec(SqlBaseParser.CollationSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#collationSpec}.
	 * @param ctx the parse tree
	 */
	void exitCollationSpec(SqlBaseParser.CollationSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#collateClause}.
	 * @param ctx the parse tree
	 */
	void enterCollateClause(SqlBaseParser.CollateClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#collateClause}.
	 * @param ctx the parse tree
	 */
	void exitCollateClause(SqlBaseParser.CollateClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#nonTrivialPrimitiveType}.
	 * @param ctx the parse tree
	 */
	void enterNonTrivialPrimitiveType(SqlBaseParser.NonTrivialPrimitiveTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#nonTrivialPrimitiveType}.
	 * @param ctx the parse tree
	 */
	void exitNonTrivialPrimitiveType(SqlBaseParser.NonTrivialPrimitiveTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#trivialPrimitiveType}.
	 * @param ctx the parse tree
	 */
	void enterTrivialPrimitiveType(SqlBaseParser.TrivialPrimitiveTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#trivialPrimitiveType}.
	 * @param ctx the parse tree
	 */
	void exitTrivialPrimitiveType(SqlBaseParser.TrivialPrimitiveTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#primitiveType}.
	 * @param ctx the parse tree
	 */
	void enterPrimitiveType(SqlBaseParser.PrimitiveTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#primitiveType}.
	 * @param ctx the parse tree
	 */
	void exitPrimitiveType(SqlBaseParser.PrimitiveTypeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code complexDataType}
	 * labeled alternative in {@link SqlBaseParser#dataType}.
	 * @param ctx the parse tree
	 */
	void enterComplexDataType(SqlBaseParser.ComplexDataTypeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code complexDataType}
	 * labeled alternative in {@link SqlBaseParser#dataType}.
	 * @param ctx the parse tree
	 */
	void exitComplexDataType(SqlBaseParser.ComplexDataTypeContext ctx);
	/**
	 * Enter a parse tree produced by the {@code primitiveDataType}
	 * labeled alternative in {@link SqlBaseParser#dataType}.
	 * @param ctx the parse tree
	 */
	void enterPrimitiveDataType(SqlBaseParser.PrimitiveDataTypeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code primitiveDataType}
	 * labeled alternative in {@link SqlBaseParser#dataType}.
	 * @param ctx the parse tree
	 */
	void exitPrimitiveDataType(SqlBaseParser.PrimitiveDataTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#qualifiedColTypeWithPositionList}.
	 * @param ctx the parse tree
	 */
	void enterQualifiedColTypeWithPositionList(SqlBaseParser.QualifiedColTypeWithPositionListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#qualifiedColTypeWithPositionList}.
	 * @param ctx the parse tree
	 */
	void exitQualifiedColTypeWithPositionList(SqlBaseParser.QualifiedColTypeWithPositionListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#qualifiedColTypeWithPosition}.
	 * @param ctx the parse tree
	 */
	void enterQualifiedColTypeWithPosition(SqlBaseParser.QualifiedColTypeWithPositionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#qualifiedColTypeWithPosition}.
	 * @param ctx the parse tree
	 */
	void exitQualifiedColTypeWithPosition(SqlBaseParser.QualifiedColTypeWithPositionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#colDefinitionDescriptorWithPosition}.
	 * @param ctx the parse tree
	 */
	void enterColDefinitionDescriptorWithPosition(SqlBaseParser.ColDefinitionDescriptorWithPositionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#colDefinitionDescriptorWithPosition}.
	 * @param ctx the parse tree
	 */
	void exitColDefinitionDescriptorWithPosition(SqlBaseParser.ColDefinitionDescriptorWithPositionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#defaultExpression}.
	 * @param ctx the parse tree
	 */
	void enterDefaultExpression(SqlBaseParser.DefaultExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#defaultExpression}.
	 * @param ctx the parse tree
	 */
	void exitDefaultExpression(SqlBaseParser.DefaultExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#variableDefaultExpression}.
	 * @param ctx the parse tree
	 */
	void enterVariableDefaultExpression(SqlBaseParser.VariableDefaultExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#variableDefaultExpression}.
	 * @param ctx the parse tree
	 */
	void exitVariableDefaultExpression(SqlBaseParser.VariableDefaultExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#colTypeList}.
	 * @param ctx the parse tree
	 */
	void enterColTypeList(SqlBaseParser.ColTypeListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#colTypeList}.
	 * @param ctx the parse tree
	 */
	void exitColTypeList(SqlBaseParser.ColTypeListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#colType}.
	 * @param ctx the parse tree
	 */
	void enterColType(SqlBaseParser.ColTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#colType}.
	 * @param ctx the parse tree
	 */
	void exitColType(SqlBaseParser.ColTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#tableElementList}.
	 * @param ctx the parse tree
	 */
	void enterTableElementList(SqlBaseParser.TableElementListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#tableElementList}.
	 * @param ctx the parse tree
	 */
	void exitTableElementList(SqlBaseParser.TableElementListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#tableElement}.
	 * @param ctx the parse tree
	 */
	void enterTableElement(SqlBaseParser.TableElementContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#tableElement}.
	 * @param ctx the parse tree
	 */
	void exitTableElement(SqlBaseParser.TableElementContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#colDefinitionList}.
	 * @param ctx the parse tree
	 */
	void enterColDefinitionList(SqlBaseParser.ColDefinitionListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#colDefinitionList}.
	 * @param ctx the parse tree
	 */
	void exitColDefinitionList(SqlBaseParser.ColDefinitionListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#colDefinition}.
	 * @param ctx the parse tree
	 */
	void enterColDefinition(SqlBaseParser.ColDefinitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#colDefinition}.
	 * @param ctx the parse tree
	 */
	void exitColDefinition(SqlBaseParser.ColDefinitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#colDefinitionOption}.
	 * @param ctx the parse tree
	 */
	void enterColDefinitionOption(SqlBaseParser.ColDefinitionOptionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#colDefinitionOption}.
	 * @param ctx the parse tree
	 */
	void exitColDefinitionOption(SqlBaseParser.ColDefinitionOptionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code generatedColumn}
	 * labeled alternative in {@link SqlBaseParser#generationExpression}.
	 * @param ctx the parse tree
	 */
	void enterGeneratedColumn(SqlBaseParser.GeneratedColumnContext ctx);
	/**
	 * Exit a parse tree produced by the {@code generatedColumn}
	 * labeled alternative in {@link SqlBaseParser#generationExpression}.
	 * @param ctx the parse tree
	 */
	void exitGeneratedColumn(SqlBaseParser.GeneratedColumnContext ctx);
	/**
	 * Enter a parse tree produced by the {@code identityColumn}
	 * labeled alternative in {@link SqlBaseParser#generationExpression}.
	 * @param ctx the parse tree
	 */
	void enterIdentityColumn(SqlBaseParser.IdentityColumnContext ctx);
	/**
	 * Exit a parse tree produced by the {@code identityColumn}
	 * labeled alternative in {@link SqlBaseParser#generationExpression}.
	 * @param ctx the parse tree
	 */
	void exitIdentityColumn(SqlBaseParser.IdentityColumnContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#identityColSpec}.
	 * @param ctx the parse tree
	 */
	void enterIdentityColSpec(SqlBaseParser.IdentityColSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#identityColSpec}.
	 * @param ctx the parse tree
	 */
	void exitIdentityColSpec(SqlBaseParser.IdentityColSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#sequenceGeneratorOption}.
	 * @param ctx the parse tree
	 */
	void enterSequenceGeneratorOption(SqlBaseParser.SequenceGeneratorOptionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#sequenceGeneratorOption}.
	 * @param ctx the parse tree
	 */
	void exitSequenceGeneratorOption(SqlBaseParser.SequenceGeneratorOptionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#sequenceGeneratorStartOrStep}.
	 * @param ctx the parse tree
	 */
	void enterSequenceGeneratorStartOrStep(SqlBaseParser.SequenceGeneratorStartOrStepContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#sequenceGeneratorStartOrStep}.
	 * @param ctx the parse tree
	 */
	void exitSequenceGeneratorStartOrStep(SqlBaseParser.SequenceGeneratorStartOrStepContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#complexColTypeList}.
	 * @param ctx the parse tree
	 */
	void enterComplexColTypeList(SqlBaseParser.ComplexColTypeListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#complexColTypeList}.
	 * @param ctx the parse tree
	 */
	void exitComplexColTypeList(SqlBaseParser.ComplexColTypeListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#complexColType}.
	 * @param ctx the parse tree
	 */
	void enterComplexColType(SqlBaseParser.ComplexColTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#complexColType}.
	 * @param ctx the parse tree
	 */
	void exitComplexColType(SqlBaseParser.ComplexColTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#routineCharacteristics}.
	 * @param ctx the parse tree
	 */
	void enterRoutineCharacteristics(SqlBaseParser.RoutineCharacteristicsContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#routineCharacteristics}.
	 * @param ctx the parse tree
	 */
	void exitRoutineCharacteristics(SqlBaseParser.RoutineCharacteristicsContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#routineLanguage}.
	 * @param ctx the parse tree
	 */
	void enterRoutineLanguage(SqlBaseParser.RoutineLanguageContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#routineLanguage}.
	 * @param ctx the parse tree
	 */
	void exitRoutineLanguage(SqlBaseParser.RoutineLanguageContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#specificName}.
	 * @param ctx the parse tree
	 */
	void enterSpecificName(SqlBaseParser.SpecificNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#specificName}.
	 * @param ctx the parse tree
	 */
	void exitSpecificName(SqlBaseParser.SpecificNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#deterministic}.
	 * @param ctx the parse tree
	 */
	void enterDeterministic(SqlBaseParser.DeterministicContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#deterministic}.
	 * @param ctx the parse tree
	 */
	void exitDeterministic(SqlBaseParser.DeterministicContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#sqlDataAccess}.
	 * @param ctx the parse tree
	 */
	void enterSqlDataAccess(SqlBaseParser.SqlDataAccessContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#sqlDataAccess}.
	 * @param ctx the parse tree
	 */
	void exitSqlDataAccess(SqlBaseParser.SqlDataAccessContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#nullCall}.
	 * @param ctx the parse tree
	 */
	void enterNullCall(SqlBaseParser.NullCallContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#nullCall}.
	 * @param ctx the parse tree
	 */
	void exitNullCall(SqlBaseParser.NullCallContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#rightsClause}.
	 * @param ctx the parse tree
	 */
	void enterRightsClause(SqlBaseParser.RightsClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#rightsClause}.
	 * @param ctx the parse tree
	 */
	void exitRightsClause(SqlBaseParser.RightsClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#whenClause}.
	 * @param ctx the parse tree
	 */
	void enterWhenClause(SqlBaseParser.WhenClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#whenClause}.
	 * @param ctx the parse tree
	 */
	void exitWhenClause(SqlBaseParser.WhenClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#windowClause}.
	 * @param ctx the parse tree
	 */
	void enterWindowClause(SqlBaseParser.WindowClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#windowClause}.
	 * @param ctx the parse tree
	 */
	void exitWindowClause(SqlBaseParser.WindowClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#namedWindow}.
	 * @param ctx the parse tree
	 */
	void enterNamedWindow(SqlBaseParser.NamedWindowContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#namedWindow}.
	 * @param ctx the parse tree
	 */
	void exitNamedWindow(SqlBaseParser.NamedWindowContext ctx);
	/**
	 * Enter a parse tree produced by the {@code windowRef}
	 * labeled alternative in {@link SqlBaseParser#windowSpec}.
	 * @param ctx the parse tree
	 */
	void enterWindowRef(SqlBaseParser.WindowRefContext ctx);
	/**
	 * Exit a parse tree produced by the {@code windowRef}
	 * labeled alternative in {@link SqlBaseParser#windowSpec}.
	 * @param ctx the parse tree
	 */
	void exitWindowRef(SqlBaseParser.WindowRefContext ctx);
	/**
	 * Enter a parse tree produced by the {@code windowDef}
	 * labeled alternative in {@link SqlBaseParser#windowSpec}.
	 * @param ctx the parse tree
	 */
	void enterWindowDef(SqlBaseParser.WindowDefContext ctx);
	/**
	 * Exit a parse tree produced by the {@code windowDef}
	 * labeled alternative in {@link SqlBaseParser#windowSpec}.
	 * @param ctx the parse tree
	 */
	void exitWindowDef(SqlBaseParser.WindowDefContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#windowFrame}.
	 * @param ctx the parse tree
	 */
	void enterWindowFrame(SqlBaseParser.WindowFrameContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#windowFrame}.
	 * @param ctx the parse tree
	 */
	void exitWindowFrame(SqlBaseParser.WindowFrameContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#frameBound}.
	 * @param ctx the parse tree
	 */
	void enterFrameBound(SqlBaseParser.FrameBoundContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#frameBound}.
	 * @param ctx the parse tree
	 */
	void exitFrameBound(SqlBaseParser.FrameBoundContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#qualifiedNameList}.
	 * @param ctx the parse tree
	 */
	void enterQualifiedNameList(SqlBaseParser.QualifiedNameListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#qualifiedNameList}.
	 * @param ctx the parse tree
	 */
	void exitQualifiedNameList(SqlBaseParser.QualifiedNameListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#functionName}.
	 * @param ctx the parse tree
	 */
	void enterFunctionName(SqlBaseParser.FunctionNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#functionName}.
	 * @param ctx the parse tree
	 */
	void exitFunctionName(SqlBaseParser.FunctionNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	void enterQualifiedName(SqlBaseParser.QualifiedNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	void exitQualifiedName(SqlBaseParser.QualifiedNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#errorCapturingIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterErrorCapturingIdentifier(SqlBaseParser.ErrorCapturingIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#errorCapturingIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitErrorCapturingIdentifier(SqlBaseParser.ErrorCapturingIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code errorIdent}
	 * labeled alternative in {@link SqlBaseParser#errorCapturingIdentifierExtra}.
	 * @param ctx the parse tree
	 */
	void enterErrorIdent(SqlBaseParser.ErrorIdentContext ctx);
	/**
	 * Exit a parse tree produced by the {@code errorIdent}
	 * labeled alternative in {@link SqlBaseParser#errorCapturingIdentifierExtra}.
	 * @param ctx the parse tree
	 */
	void exitErrorIdent(SqlBaseParser.ErrorIdentContext ctx);
	/**
	 * Enter a parse tree produced by the {@code realIdent}
	 * labeled alternative in {@link SqlBaseParser#errorCapturingIdentifierExtra}.
	 * @param ctx the parse tree
	 */
	void enterRealIdent(SqlBaseParser.RealIdentContext ctx);
	/**
	 * Exit a parse tree produced by the {@code realIdent}
	 * labeled alternative in {@link SqlBaseParser#errorCapturingIdentifierExtra}.
	 * @param ctx the parse tree
	 */
	void exitRealIdent(SqlBaseParser.RealIdentContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#identifier}.
	 * @param ctx the parse tree
	 */
	void enterIdentifier(SqlBaseParser.IdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#identifier}.
	 * @param ctx the parse tree
	 */
	void exitIdentifier(SqlBaseParser.IdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link SqlBaseParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link SqlBaseParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code quotedIdentifierAlternative}
	 * labeled alternative in {@link SqlBaseParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterQuotedIdentifierAlternative(SqlBaseParser.QuotedIdentifierAlternativeContext ctx);
	/**
	 * Exit a parse tree produced by the {@code quotedIdentifierAlternative}
	 * labeled alternative in {@link SqlBaseParser#strictIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitQuotedIdentifierAlternative(SqlBaseParser.QuotedIdentifierAlternativeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#backQuotedIdentifier}.
	 * @param ctx the parse tree
	 */
	void enterBackQuotedIdentifier(SqlBaseParser.BackQuotedIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#backQuotedIdentifier}.
	 * @param ctx the parse tree
	 */
	void exitBackQuotedIdentifier(SqlBaseParser.BackQuotedIdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code exponentLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void enterExponentLiteral(SqlBaseParser.ExponentLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code exponentLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void exitExponentLiteral(SqlBaseParser.ExponentLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void enterDecimalLiteral(SqlBaseParser.DecimalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void exitDecimalLiteral(SqlBaseParser.DecimalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code legacyDecimalLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void enterLegacyDecimalLiteral(SqlBaseParser.LegacyDecimalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code legacyDecimalLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void exitLegacyDecimalLiteral(SqlBaseParser.LegacyDecimalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void enterIntegerLiteral(SqlBaseParser.IntegerLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void exitIntegerLiteral(SqlBaseParser.IntegerLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code bigIntLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void enterBigIntLiteral(SqlBaseParser.BigIntLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code bigIntLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void exitBigIntLiteral(SqlBaseParser.BigIntLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code smallIntLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void enterSmallIntLiteral(SqlBaseParser.SmallIntLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code smallIntLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void exitSmallIntLiteral(SqlBaseParser.SmallIntLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code tinyIntLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void enterTinyIntLiteral(SqlBaseParser.TinyIntLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code tinyIntLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void exitTinyIntLiteral(SqlBaseParser.TinyIntLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void enterDoubleLiteral(SqlBaseParser.DoubleLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void exitDoubleLiteral(SqlBaseParser.DoubleLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code floatLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void enterFloatLiteral(SqlBaseParser.FloatLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code floatLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void exitFloatLiteral(SqlBaseParser.FloatLiteralContext ctx);
	/**
	 * Enter a parse tree produced by the {@code bigDecimalLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void enterBigDecimalLiteral(SqlBaseParser.BigDecimalLiteralContext ctx);
	/**
	 * Exit a parse tree produced by the {@code bigDecimalLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 */
	void exitBigDecimalLiteral(SqlBaseParser.BigDecimalLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#columnConstraintDefinition}.
	 * @param ctx the parse tree
	 */
	void enterColumnConstraintDefinition(SqlBaseParser.ColumnConstraintDefinitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#columnConstraintDefinition}.
	 * @param ctx the parse tree
	 */
	void exitColumnConstraintDefinition(SqlBaseParser.ColumnConstraintDefinitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#columnConstraint}.
	 * @param ctx the parse tree
	 */
	void enterColumnConstraint(SqlBaseParser.ColumnConstraintContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#columnConstraint}.
	 * @param ctx the parse tree
	 */
	void exitColumnConstraint(SqlBaseParser.ColumnConstraintContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#tableConstraintDefinition}.
	 * @param ctx the parse tree
	 */
	void enterTableConstraintDefinition(SqlBaseParser.TableConstraintDefinitionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#tableConstraintDefinition}.
	 * @param ctx the parse tree
	 */
	void exitTableConstraintDefinition(SqlBaseParser.TableConstraintDefinitionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#tableConstraint}.
	 * @param ctx the parse tree
	 */
	void enterTableConstraint(SqlBaseParser.TableConstraintContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#tableConstraint}.
	 * @param ctx the parse tree
	 */
	void exitTableConstraint(SqlBaseParser.TableConstraintContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#checkConstraint}.
	 * @param ctx the parse tree
	 */
	void enterCheckConstraint(SqlBaseParser.CheckConstraintContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#checkConstraint}.
	 * @param ctx the parse tree
	 */
	void exitCheckConstraint(SqlBaseParser.CheckConstraintContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#uniqueSpec}.
	 * @param ctx the parse tree
	 */
	void enterUniqueSpec(SqlBaseParser.UniqueSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#uniqueSpec}.
	 * @param ctx the parse tree
	 */
	void exitUniqueSpec(SqlBaseParser.UniqueSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#uniqueConstraint}.
	 * @param ctx the parse tree
	 */
	void enterUniqueConstraint(SqlBaseParser.UniqueConstraintContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#uniqueConstraint}.
	 * @param ctx the parse tree
	 */
	void exitUniqueConstraint(SqlBaseParser.UniqueConstraintContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#referenceSpec}.
	 * @param ctx the parse tree
	 */
	void enterReferenceSpec(SqlBaseParser.ReferenceSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#referenceSpec}.
	 * @param ctx the parse tree
	 */
	void exitReferenceSpec(SqlBaseParser.ReferenceSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#foreignKeyConstraint}.
	 * @param ctx the parse tree
	 */
	void enterForeignKeyConstraint(SqlBaseParser.ForeignKeyConstraintContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#foreignKeyConstraint}.
	 * @param ctx the parse tree
	 */
	void exitForeignKeyConstraint(SqlBaseParser.ForeignKeyConstraintContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#constraintCharacteristic}.
	 * @param ctx the parse tree
	 */
	void enterConstraintCharacteristic(SqlBaseParser.ConstraintCharacteristicContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#constraintCharacteristic}.
	 * @param ctx the parse tree
	 */
	void exitConstraintCharacteristic(SqlBaseParser.ConstraintCharacteristicContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#enforcedCharacteristic}.
	 * @param ctx the parse tree
	 */
	void enterEnforcedCharacteristic(SqlBaseParser.EnforcedCharacteristicContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#enforcedCharacteristic}.
	 * @param ctx the parse tree
	 */
	void exitEnforcedCharacteristic(SqlBaseParser.EnforcedCharacteristicContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#relyCharacteristic}.
	 * @param ctx the parse tree
	 */
	void enterRelyCharacteristic(SqlBaseParser.RelyCharacteristicContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#relyCharacteristic}.
	 * @param ctx the parse tree
	 */
	void exitRelyCharacteristic(SqlBaseParser.RelyCharacteristicContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#alterColumnSpecList}.
	 * @param ctx the parse tree
	 */
	void enterAlterColumnSpecList(SqlBaseParser.AlterColumnSpecListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#alterColumnSpecList}.
	 * @param ctx the parse tree
	 */
	void exitAlterColumnSpecList(SqlBaseParser.AlterColumnSpecListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#alterColumnSpec}.
	 * @param ctx the parse tree
	 */
	void enterAlterColumnSpec(SqlBaseParser.AlterColumnSpecContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#alterColumnSpec}.
	 * @param ctx the parse tree
	 */
	void exitAlterColumnSpec(SqlBaseParser.AlterColumnSpecContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#alterColumnAction}.
	 * @param ctx the parse tree
	 */
	void enterAlterColumnAction(SqlBaseParser.AlterColumnActionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#alterColumnAction}.
	 * @param ctx the parse tree
	 */
	void exitAlterColumnAction(SqlBaseParser.AlterColumnActionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#stringLit}.
	 * @param ctx the parse tree
	 */
	void enterStringLit(SqlBaseParser.StringLitContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#stringLit}.
	 * @param ctx the parse tree
	 */
	void exitStringLit(SqlBaseParser.StringLitContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#comment}.
	 * @param ctx the parse tree
	 */
	void enterComment(SqlBaseParser.CommentContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#comment}.
	 * @param ctx the parse tree
	 */
	void exitComment(SqlBaseParser.CommentContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#version}.
	 * @param ctx the parse tree
	 */
	void enterVersion(SqlBaseParser.VersionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#version}.
	 * @param ctx the parse tree
	 */
	void exitVersion(SqlBaseParser.VersionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#operatorPipeRightSide}.
	 * @param ctx the parse tree
	 */
	void enterOperatorPipeRightSide(SqlBaseParser.OperatorPipeRightSideContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#operatorPipeRightSide}.
	 * @param ctx the parse tree
	 */
	void exitOperatorPipeRightSide(SqlBaseParser.OperatorPipeRightSideContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#operatorPipeSetAssignmentSeq}.
	 * @param ctx the parse tree
	 */
	void enterOperatorPipeSetAssignmentSeq(SqlBaseParser.OperatorPipeSetAssignmentSeqContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#operatorPipeSetAssignmentSeq}.
	 * @param ctx the parse tree
	 */
	void exitOperatorPipeSetAssignmentSeq(SqlBaseParser.OperatorPipeSetAssignmentSeqContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#ansiNonReserved}.
	 * @param ctx the parse tree
	 */
	void enterAnsiNonReserved(SqlBaseParser.AnsiNonReservedContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#ansiNonReserved}.
	 * @param ctx the parse tree
	 */
	void exitAnsiNonReserved(SqlBaseParser.AnsiNonReservedContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#strictNonReserved}.
	 * @param ctx the parse tree
	 */
	void enterStrictNonReserved(SqlBaseParser.StrictNonReservedContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#strictNonReserved}.
	 * @param ctx the parse tree
	 */
	void exitStrictNonReserved(SqlBaseParser.StrictNonReservedContext ctx);
	/**
	 * Enter a parse tree produced by {@link SqlBaseParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void enterNonReserved(SqlBaseParser.NonReservedContext ctx);
	/**
	 * Exit a parse tree produced by {@link SqlBaseParser#nonReserved}.
	 * @param ctx the parse tree
	 */
	void exitNonReserved(SqlBaseParser.NonReservedContext ctx);
}