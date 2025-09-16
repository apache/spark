// Generated from SqlBaseParser.g4 by ANTLR 4.9.3
package org.apache.spark.sql.catalyst.parser;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link SqlBaseParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface SqlBaseParserVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#compoundOrSingleStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCompoundOrSingleStatement(SqlBaseParser.CompoundOrSingleStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#singleCompoundStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleCompoundStatement(SqlBaseParser.SingleCompoundStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#beginEndCompoundBlock}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBeginEndCompoundBlock(SqlBaseParser.BeginEndCompoundBlockContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#compoundBody}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCompoundBody(SqlBaseParser.CompoundBodyContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#compoundStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCompoundStatement(SqlBaseParser.CompoundStatementContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setVariableInsideSqlScript}
	 * labeled alternative in {@link SqlBaseParser#setStatementInsideSqlScript}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetVariableInsideSqlScript(SqlBaseParser.SetVariableInsideSqlScriptContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#sqlStateValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSqlStateValue(SqlBaseParser.SqlStateValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#declareConditionStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDeclareConditionStatement(SqlBaseParser.DeclareConditionStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#conditionValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConditionValue(SqlBaseParser.ConditionValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#conditionValues}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConditionValues(SqlBaseParser.ConditionValuesContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#declareHandlerStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDeclareHandlerStatement(SqlBaseParser.DeclareHandlerStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#whileStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWhileStatement(SqlBaseParser.WhileStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#ifElseStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIfElseStatement(SqlBaseParser.IfElseStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#repeatStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRepeatStatement(SqlBaseParser.RepeatStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#leaveStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLeaveStatement(SqlBaseParser.LeaveStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#iterateStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIterateStatement(SqlBaseParser.IterateStatementContext ctx);
	/**
	 * Visit a parse tree produced by the {@code searchedCaseStatement}
	 * labeled alternative in {@link SqlBaseParser#caseStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSearchedCaseStatement(SqlBaseParser.SearchedCaseStatementContext ctx);
	/**
	 * Visit a parse tree produced by the {@code simpleCaseStatement}
	 * labeled alternative in {@link SqlBaseParser#caseStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSimpleCaseStatement(SqlBaseParser.SimpleCaseStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#loopStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLoopStatement(SqlBaseParser.LoopStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#forStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitForStatement(SqlBaseParser.ForStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#singleStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleStatement(SqlBaseParser.SingleStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#beginLabel}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBeginLabel(SqlBaseParser.BeginLabelContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#endLabel}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitEndLabel(SqlBaseParser.EndLabelContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#singleExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleExpression(SqlBaseParser.SingleExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#singleTableIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleTableIdentifier(SqlBaseParser.SingleTableIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#singleMultipartIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleMultipartIdentifier(SqlBaseParser.SingleMultipartIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#singleFunctionIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleFunctionIdentifier(SqlBaseParser.SingleFunctionIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#singleDataType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleDataType(SqlBaseParser.SingleDataTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#singleTableSchema}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleTableSchema(SqlBaseParser.SingleTableSchemaContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#singleRoutineParamList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleRoutineParamList(SqlBaseParser.SingleRoutineParamListContext ctx);
	/**
	 * Visit a parse tree produced by the {@code statementDefault}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStatementDefault(SqlBaseParser.StatementDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code visitExecuteImmediate}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitVisitExecuteImmediate(SqlBaseParser.VisitExecuteImmediateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code dmlStatement}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDmlStatement(SqlBaseParser.DmlStatementContext ctx);
	/**
	 * Visit a parse tree produced by the {@code use}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUse(SqlBaseParser.UseContext ctx);
	/**
	 * Visit a parse tree produced by the {@code useNamespace}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUseNamespace(SqlBaseParser.UseNamespaceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setCatalog}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetCatalog(SqlBaseParser.SetCatalogContext ctx);
	/**
	 * Visit a parse tree produced by the {@code createNamespace}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateNamespace(SqlBaseParser.CreateNamespaceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setNamespaceProperties}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetNamespaceProperties(SqlBaseParser.SetNamespacePropertiesContext ctx);
	/**
	 * Visit a parse tree produced by the {@code unsetNamespaceProperties}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnsetNamespaceProperties(SqlBaseParser.UnsetNamespacePropertiesContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setNamespaceCollation}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetNamespaceCollation(SqlBaseParser.SetNamespaceCollationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setNamespaceLocation}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetNamespaceLocation(SqlBaseParser.SetNamespaceLocationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code dropNamespace}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropNamespace(SqlBaseParser.DropNamespaceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showNamespaces}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowNamespaces(SqlBaseParser.ShowNamespacesContext ctx);
	/**
	 * Visit a parse tree produced by the {@code createTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateTable(SqlBaseParser.CreateTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code createTableLike}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateTableLike(SqlBaseParser.CreateTableLikeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code replaceTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitReplaceTable(SqlBaseParser.ReplaceTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code analyze}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAnalyze(SqlBaseParser.AnalyzeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code analyzeTables}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAnalyzeTables(SqlBaseParser.AnalyzeTablesContext ctx);
	/**
	 * Visit a parse tree produced by the {@code addTableColumns}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAddTableColumns(SqlBaseParser.AddTableColumnsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code renameTableColumn}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRenameTableColumn(SqlBaseParser.RenameTableColumnContext ctx);
	/**
	 * Visit a parse tree produced by the {@code dropTableColumns}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropTableColumns(SqlBaseParser.DropTableColumnsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code renameTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRenameTable(SqlBaseParser.RenameTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setTableProperties}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetTableProperties(SqlBaseParser.SetTablePropertiesContext ctx);
	/**
	 * Visit a parse tree produced by the {@code unsetTableProperties}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnsetTableProperties(SqlBaseParser.UnsetTablePropertiesContext ctx);
	/**
	 * Visit a parse tree produced by the {@code alterTableAlterColumn}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAlterTableAlterColumn(SqlBaseParser.AlterTableAlterColumnContext ctx);
	/**
	 * Visit a parse tree produced by the {@code hiveChangeColumn}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHiveChangeColumn(SqlBaseParser.HiveChangeColumnContext ctx);
	/**
	 * Visit a parse tree produced by the {@code hiveReplaceColumns}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHiveReplaceColumns(SqlBaseParser.HiveReplaceColumnsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setTableSerDe}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetTableSerDe(SqlBaseParser.SetTableSerDeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code addTablePartition}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAddTablePartition(SqlBaseParser.AddTablePartitionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code renameTablePartition}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRenameTablePartition(SqlBaseParser.RenameTablePartitionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code dropTablePartitions}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropTablePartitions(SqlBaseParser.DropTablePartitionsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setTableLocation}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetTableLocation(SqlBaseParser.SetTableLocationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code recoverPartitions}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRecoverPartitions(SqlBaseParser.RecoverPartitionsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code alterClusterBy}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAlterClusterBy(SqlBaseParser.AlterClusterByContext ctx);
	/**
	 * Visit a parse tree produced by the {@code alterTableCollation}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAlterTableCollation(SqlBaseParser.AlterTableCollationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code addTableConstraint}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAddTableConstraint(SqlBaseParser.AddTableConstraintContext ctx);
	/**
	 * Visit a parse tree produced by the {@code dropTableConstraint}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropTableConstraint(SqlBaseParser.DropTableConstraintContext ctx);
	/**
	 * Visit a parse tree produced by the {@code dropTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropTable(SqlBaseParser.DropTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code dropView}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropView(SqlBaseParser.DropViewContext ctx);
	/**
	 * Visit a parse tree produced by the {@code createView}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateView(SqlBaseParser.CreateViewContext ctx);
	/**
	 * Visit a parse tree produced by the {@code createTempViewUsing}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateTempViewUsing(SqlBaseParser.CreateTempViewUsingContext ctx);
	/**
	 * Visit a parse tree produced by the {@code alterViewQuery}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAlterViewQuery(SqlBaseParser.AlterViewQueryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code alterViewSchemaBinding}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAlterViewSchemaBinding(SqlBaseParser.AlterViewSchemaBindingContext ctx);
	/**
	 * Visit a parse tree produced by the {@code createFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateFunction(SqlBaseParser.CreateFunctionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code createUserDefinedFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateUserDefinedFunction(SqlBaseParser.CreateUserDefinedFunctionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code dropFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropFunction(SqlBaseParser.DropFunctionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code createVariable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateVariable(SqlBaseParser.CreateVariableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code dropVariable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropVariable(SqlBaseParser.DropVariableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code explain}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExplain(SqlBaseParser.ExplainContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showTables}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowTables(SqlBaseParser.ShowTablesContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showTableExtended}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowTableExtended(SqlBaseParser.ShowTableExtendedContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showTblProperties}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowTblProperties(SqlBaseParser.ShowTblPropertiesContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showColumns}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowColumns(SqlBaseParser.ShowColumnsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showViews}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowViews(SqlBaseParser.ShowViewsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showPartitions}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowPartitions(SqlBaseParser.ShowPartitionsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showFunctions}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowFunctions(SqlBaseParser.ShowFunctionsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showProcedures}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowProcedures(SqlBaseParser.ShowProceduresContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showCreateTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowCreateTable(SqlBaseParser.ShowCreateTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showCurrentNamespace}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowCurrentNamespace(SqlBaseParser.ShowCurrentNamespaceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code showCatalogs}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShowCatalogs(SqlBaseParser.ShowCatalogsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code describeFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDescribeFunction(SqlBaseParser.DescribeFunctionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code describeProcedure}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDescribeProcedure(SqlBaseParser.DescribeProcedureContext ctx);
	/**
	 * Visit a parse tree produced by the {@code describeNamespace}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDescribeNamespace(SqlBaseParser.DescribeNamespaceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code describeRelation}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDescribeRelation(SqlBaseParser.DescribeRelationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code describeQuery}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDescribeQuery(SqlBaseParser.DescribeQueryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code commentNamespace}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCommentNamespace(SqlBaseParser.CommentNamespaceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code commentTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCommentTable(SqlBaseParser.CommentTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code refreshTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRefreshTable(SqlBaseParser.RefreshTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code refreshFunction}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRefreshFunction(SqlBaseParser.RefreshFunctionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code refreshResource}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRefreshResource(SqlBaseParser.RefreshResourceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code cacheTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCacheTable(SqlBaseParser.CacheTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code uncacheTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUncacheTable(SqlBaseParser.UncacheTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code clearCache}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitClearCache(SqlBaseParser.ClearCacheContext ctx);
	/**
	 * Visit a parse tree produced by the {@code loadData}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLoadData(SqlBaseParser.LoadDataContext ctx);
	/**
	 * Visit a parse tree produced by the {@code truncateTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTruncateTable(SqlBaseParser.TruncateTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code repairTable}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRepairTable(SqlBaseParser.RepairTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code manageResource}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitManageResource(SqlBaseParser.ManageResourceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code createIndex}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateIndex(SqlBaseParser.CreateIndexContext ctx);
	/**
	 * Visit a parse tree produced by the {@code dropIndex}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDropIndex(SqlBaseParser.DropIndexContext ctx);
	/**
	 * Visit a parse tree produced by the {@code call}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCall(SqlBaseParser.CallContext ctx);
	/**
	 * Visit a parse tree produced by the {@code failNativeCommand}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFailNativeCommand(SqlBaseParser.FailNativeCommandContext ctx);
	/**
	 * Visit a parse tree produced by the {@code createPipelineDataset}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreatePipelineDataset(SqlBaseParser.CreatePipelineDatasetContext ctx);
	/**
	 * Visit a parse tree produced by the {@code createPipelineInsertIntoFlow}
	 * labeled alternative in {@link SqlBaseParser#statement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreatePipelineInsertIntoFlow(SqlBaseParser.CreatePipelineInsertIntoFlowContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#materializedView}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMaterializedView(SqlBaseParser.MaterializedViewContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#streamingTable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStreamingTable(SqlBaseParser.StreamingTableContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#createPipelineDatasetHeader}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreatePipelineDatasetHeader(SqlBaseParser.CreatePipelineDatasetHeaderContext ctx);
	/**
	 * Visit a parse tree produced by the {@code streamTableName}
	 * labeled alternative in {@link SqlBaseParser#streamRelationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStreamTableName(SqlBaseParser.StreamTableNameContext ctx);
	/**
	 * Visit a parse tree produced by the {@code failSetRole}
	 * labeled alternative in {@link SqlBaseParser#setResetStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFailSetRole(SqlBaseParser.FailSetRoleContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setTimeZone}
	 * labeled alternative in {@link SqlBaseParser#setResetStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetTimeZone(SqlBaseParser.SetTimeZoneContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setVariable}
	 * labeled alternative in {@link SqlBaseParser#setResetStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetVariable(SqlBaseParser.SetVariableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setQuotedConfiguration}
	 * labeled alternative in {@link SqlBaseParser#setResetStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetQuotedConfiguration(SqlBaseParser.SetQuotedConfigurationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setConfiguration}
	 * labeled alternative in {@link SqlBaseParser#setResetStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetConfiguration(SqlBaseParser.SetConfigurationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code resetQuotedConfiguration}
	 * labeled alternative in {@link SqlBaseParser#setResetStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitResetQuotedConfiguration(SqlBaseParser.ResetQuotedConfigurationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code resetConfiguration}
	 * labeled alternative in {@link SqlBaseParser#setResetStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitResetConfiguration(SqlBaseParser.ResetConfigurationContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#executeImmediate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExecuteImmediate(SqlBaseParser.ExecuteImmediateContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#executeImmediateUsing}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExecuteImmediateUsing(SqlBaseParser.ExecuteImmediateUsingContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#timezone}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTimezone(SqlBaseParser.TimezoneContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#configKey}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConfigKey(SqlBaseParser.ConfigKeyContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#configValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConfigValue(SqlBaseParser.ConfigValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#unsupportedHiveNativeCommands}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnsupportedHiveNativeCommands(SqlBaseParser.UnsupportedHiveNativeCommandsContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#createTableHeader}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateTableHeader(SqlBaseParser.CreateTableHeaderContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#replaceTableHeader}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitReplaceTableHeader(SqlBaseParser.ReplaceTableHeaderContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#clusterBySpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitClusterBySpec(SqlBaseParser.ClusterBySpecContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#bucketSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBucketSpec(SqlBaseParser.BucketSpecContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#skewSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSkewSpec(SqlBaseParser.SkewSpecContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#locationSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLocationSpec(SqlBaseParser.LocationSpecContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#schemaBinding}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSchemaBinding(SqlBaseParser.SchemaBindingContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#commentSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCommentSpec(SqlBaseParser.CommentSpecContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#singleQuery}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleQuery(SqlBaseParser.SingleQueryContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#query}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuery(SqlBaseParser.QueryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code insertOverwriteTable}
	 * labeled alternative in {@link SqlBaseParser#insertInto}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInsertOverwriteTable(SqlBaseParser.InsertOverwriteTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code insertIntoTable}
	 * labeled alternative in {@link SqlBaseParser#insertInto}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInsertIntoTable(SqlBaseParser.InsertIntoTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code insertIntoReplaceWhere}
	 * labeled alternative in {@link SqlBaseParser#insertInto}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInsertIntoReplaceWhere(SqlBaseParser.InsertIntoReplaceWhereContext ctx);
	/**
	 * Visit a parse tree produced by the {@code insertOverwriteHiveDir}
	 * labeled alternative in {@link SqlBaseParser#insertInto}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInsertOverwriteHiveDir(SqlBaseParser.InsertOverwriteHiveDirContext ctx);
	/**
	 * Visit a parse tree produced by the {@code insertOverwriteDir}
	 * labeled alternative in {@link SqlBaseParser#insertInto}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInsertOverwriteDir(SqlBaseParser.InsertOverwriteDirContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#partitionSpecLocation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPartitionSpecLocation(SqlBaseParser.PartitionSpecLocationContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#partitionSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPartitionSpec(SqlBaseParser.PartitionSpecContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#partitionVal}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPartitionVal(SqlBaseParser.PartitionValContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#createPipelineFlowHeader}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreatePipelineFlowHeader(SqlBaseParser.CreatePipelineFlowHeaderContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#namespace}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNamespace(SqlBaseParser.NamespaceContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#namespaces}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNamespaces(SqlBaseParser.NamespacesContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#variable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitVariable(SqlBaseParser.VariableContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#describeFuncName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDescribeFuncName(SqlBaseParser.DescribeFuncNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#describeColName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDescribeColName(SqlBaseParser.DescribeColNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#ctes}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCtes(SqlBaseParser.CtesContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#namedQuery}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNamedQuery(SqlBaseParser.NamedQueryContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#tableProvider}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableProvider(SqlBaseParser.TableProviderContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#createTableClauses}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateTableClauses(SqlBaseParser.CreateTableClausesContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#propertyList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPropertyList(SqlBaseParser.PropertyListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#property}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitProperty(SqlBaseParser.PropertyContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#propertyKey}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPropertyKey(SqlBaseParser.PropertyKeyContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#propertyValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPropertyValue(SqlBaseParser.PropertyValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#expressionPropertyList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpressionPropertyList(SqlBaseParser.ExpressionPropertyListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#expressionProperty}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpressionProperty(SqlBaseParser.ExpressionPropertyContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#constantList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConstantList(SqlBaseParser.ConstantListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#nestedConstantList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNestedConstantList(SqlBaseParser.NestedConstantListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#createFileFormat}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCreateFileFormat(SqlBaseParser.CreateFileFormatContext ctx);
	/**
	 * Visit a parse tree produced by the {@code tableFileFormat}
	 * labeled alternative in {@link SqlBaseParser#fileFormat}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableFileFormat(SqlBaseParser.TableFileFormatContext ctx);
	/**
	 * Visit a parse tree produced by the {@code genericFileFormat}
	 * labeled alternative in {@link SqlBaseParser#fileFormat}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGenericFileFormat(SqlBaseParser.GenericFileFormatContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#storageHandler}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStorageHandler(SqlBaseParser.StorageHandlerContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#resource}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitResource(SqlBaseParser.ResourceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code singleInsertQuery}
	 * labeled alternative in {@link SqlBaseParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSingleInsertQuery(SqlBaseParser.SingleInsertQueryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code multiInsertQuery}
	 * labeled alternative in {@link SqlBaseParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMultiInsertQuery(SqlBaseParser.MultiInsertQueryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code deleteFromTable}
	 * labeled alternative in {@link SqlBaseParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDeleteFromTable(SqlBaseParser.DeleteFromTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code updateTable}
	 * labeled alternative in {@link SqlBaseParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUpdateTable(SqlBaseParser.UpdateTableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code mergeIntoTable}
	 * labeled alternative in {@link SqlBaseParser#dmlStatementNoWith}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMergeIntoTable(SqlBaseParser.MergeIntoTableContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#identifierReference}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifierReference(SqlBaseParser.IdentifierReferenceContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#catalogIdentifierReference}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCatalogIdentifierReference(SqlBaseParser.CatalogIdentifierReferenceContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#queryOrganization}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQueryOrganization(SqlBaseParser.QueryOrganizationContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#multiInsertQueryBody}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMultiInsertQueryBody(SqlBaseParser.MultiInsertQueryBodyContext ctx);
	/**
	 * Visit a parse tree produced by the {@code operatorPipeStatement}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOperatorPipeStatement(SqlBaseParser.OperatorPipeStatementContext ctx);
	/**
	 * Visit a parse tree produced by the {@code queryTermDefault}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQueryTermDefault(SqlBaseParser.QueryTermDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code setOperation}
	 * labeled alternative in {@link SqlBaseParser#queryTerm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetOperation(SqlBaseParser.SetOperationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code queryPrimaryDefault}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQueryPrimaryDefault(SqlBaseParser.QueryPrimaryDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code fromStmt}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFromStmt(SqlBaseParser.FromStmtContext ctx);
	/**
	 * Visit a parse tree produced by the {@code table}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTable(SqlBaseParser.TableContext ctx);
	/**
	 * Visit a parse tree produced by the {@code inlineTableDefault1}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInlineTableDefault1(SqlBaseParser.InlineTableDefault1Context ctx);
	/**
	 * Visit a parse tree produced by the {@code subquery}
	 * labeled alternative in {@link SqlBaseParser#queryPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubquery(SqlBaseParser.SubqueryContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#sortItem}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSortItem(SqlBaseParser.SortItemContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#fromStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFromStatement(SqlBaseParser.FromStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#fromStatementBody}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFromStatementBody(SqlBaseParser.FromStatementBodyContext ctx);
	/**
	 * Visit a parse tree produced by the {@code transformQuerySpecification}
	 * labeled alternative in {@link SqlBaseParser#querySpecification}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTransformQuerySpecification(SqlBaseParser.TransformQuerySpecificationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code regularQuerySpecification}
	 * labeled alternative in {@link SqlBaseParser#querySpecification}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRegularQuerySpecification(SqlBaseParser.RegularQuerySpecificationContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#transformClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTransformClause(SqlBaseParser.TransformClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#selectClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelectClause(SqlBaseParser.SelectClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#setClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetClause(SqlBaseParser.SetClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#matchedClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMatchedClause(SqlBaseParser.MatchedClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#notMatchedClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNotMatchedClause(SqlBaseParser.NotMatchedClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#notMatchedBySourceClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNotMatchedBySourceClause(SqlBaseParser.NotMatchedBySourceClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#matchedAction}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMatchedAction(SqlBaseParser.MatchedActionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#notMatchedAction}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNotMatchedAction(SqlBaseParser.NotMatchedActionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#notMatchedBySourceAction}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNotMatchedBySourceAction(SqlBaseParser.NotMatchedBySourceActionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#exceptClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExceptClause(SqlBaseParser.ExceptClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#assignmentList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAssignmentList(SqlBaseParser.AssignmentListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#assignment}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAssignment(SqlBaseParser.AssignmentContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#whereClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWhereClause(SqlBaseParser.WhereClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#havingClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHavingClause(SqlBaseParser.HavingClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#hint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHint(SqlBaseParser.HintContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#hintStatement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHintStatement(SqlBaseParser.HintStatementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#fromClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFromClause(SqlBaseParser.FromClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#temporalClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTemporalClause(SqlBaseParser.TemporalClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#aggregationClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAggregationClause(SqlBaseParser.AggregationClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#groupByClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupByClause(SqlBaseParser.GroupByClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#groupingAnalytics}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupingAnalytics(SqlBaseParser.GroupingAnalyticsContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#groupingElement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupingElement(SqlBaseParser.GroupingElementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#groupingSet}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroupingSet(SqlBaseParser.GroupingSetContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#pivotClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPivotClause(SqlBaseParser.PivotClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#pivotColumn}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPivotColumn(SqlBaseParser.PivotColumnContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#pivotValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPivotValue(SqlBaseParser.PivotValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#unpivotClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnpivotClause(SqlBaseParser.UnpivotClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#unpivotNullClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnpivotNullClause(SqlBaseParser.UnpivotNullClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#unpivotOperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnpivotOperator(SqlBaseParser.UnpivotOperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#unpivotSingleValueColumnClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnpivotSingleValueColumnClause(SqlBaseParser.UnpivotSingleValueColumnClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#unpivotMultiValueColumnClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnpivotMultiValueColumnClause(SqlBaseParser.UnpivotMultiValueColumnClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#unpivotColumnSet}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnpivotColumnSet(SqlBaseParser.UnpivotColumnSetContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#unpivotValueColumn}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnpivotValueColumn(SqlBaseParser.UnpivotValueColumnContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#unpivotNameColumn}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnpivotNameColumn(SqlBaseParser.UnpivotNameColumnContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#unpivotColumnAndAlias}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnpivotColumnAndAlias(SqlBaseParser.UnpivotColumnAndAliasContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#unpivotColumn}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnpivotColumn(SqlBaseParser.UnpivotColumnContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#unpivotAlias}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnpivotAlias(SqlBaseParser.UnpivotAliasContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#lateralView}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLateralView(SqlBaseParser.LateralViewContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#setQuantifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSetQuantifier(SqlBaseParser.SetQuantifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#relation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRelation(SqlBaseParser.RelationContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#relationExtension}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRelationExtension(SqlBaseParser.RelationExtensionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#joinRelation}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinRelation(SqlBaseParser.JoinRelationContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#joinType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinType(SqlBaseParser.JoinTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#joinCriteria}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJoinCriteria(SqlBaseParser.JoinCriteriaContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#sample}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSample(SqlBaseParser.SampleContext ctx);
	/**
	 * Visit a parse tree produced by the {@code sampleByPercentile}
	 * labeled alternative in {@link SqlBaseParser#sampleMethod}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSampleByPercentile(SqlBaseParser.SampleByPercentileContext ctx);
	/**
	 * Visit a parse tree produced by the {@code sampleByRows}
	 * labeled alternative in {@link SqlBaseParser#sampleMethod}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSampleByRows(SqlBaseParser.SampleByRowsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code sampleByBucket}
	 * labeled alternative in {@link SqlBaseParser#sampleMethod}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSampleByBucket(SqlBaseParser.SampleByBucketContext ctx);
	/**
	 * Visit a parse tree produced by the {@code sampleByBytes}
	 * labeled alternative in {@link SqlBaseParser#sampleMethod}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSampleByBytes(SqlBaseParser.SampleByBytesContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#identifierList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifierList(SqlBaseParser.IdentifierListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#identifierSeq}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifierSeq(SqlBaseParser.IdentifierSeqContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#orderedIdentifierList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOrderedIdentifierList(SqlBaseParser.OrderedIdentifierListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#orderedIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOrderedIdentifier(SqlBaseParser.OrderedIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#identifierCommentList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifierCommentList(SqlBaseParser.IdentifierCommentListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#identifierComment}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifierComment(SqlBaseParser.IdentifierCommentContext ctx);
	/**
	 * Visit a parse tree produced by the {@code streamRelation}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStreamRelation(SqlBaseParser.StreamRelationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code tableName}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableName(SqlBaseParser.TableNameContext ctx);
	/**
	 * Visit a parse tree produced by the {@code aliasedQuery}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAliasedQuery(SqlBaseParser.AliasedQueryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code aliasedRelation}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAliasedRelation(SqlBaseParser.AliasedRelationContext ctx);
	/**
	 * Visit a parse tree produced by the {@code inlineTableDefault2}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInlineTableDefault2(SqlBaseParser.InlineTableDefault2Context ctx);
	/**
	 * Visit a parse tree produced by the {@code tableValuedFunction}
	 * labeled alternative in {@link SqlBaseParser#relationPrimary}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableValuedFunction(SqlBaseParser.TableValuedFunctionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#optionsClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOptionsClause(SqlBaseParser.OptionsClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#inlineTable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInlineTable(SqlBaseParser.InlineTableContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#functionTableSubqueryArgument}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionTableSubqueryArgument(SqlBaseParser.FunctionTableSubqueryArgumentContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#tableArgumentPartitioning}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableArgumentPartitioning(SqlBaseParser.TableArgumentPartitioningContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#functionTableNamedArgumentExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionTableNamedArgumentExpression(SqlBaseParser.FunctionTableNamedArgumentExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#functionTableReferenceArgument}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionTableReferenceArgument(SqlBaseParser.FunctionTableReferenceArgumentContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#functionTableArgument}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionTableArgument(SqlBaseParser.FunctionTableArgumentContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#functionTable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionTable(SqlBaseParser.FunctionTableContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#tableAlias}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableAlias(SqlBaseParser.TableAliasContext ctx);
	/**
	 * Visit a parse tree produced by the {@code rowFormatSerde}
	 * labeled alternative in {@link SqlBaseParser#rowFormat}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRowFormatSerde(SqlBaseParser.RowFormatSerdeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code rowFormatDelimited}
	 * labeled alternative in {@link SqlBaseParser#rowFormat}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRowFormatDelimited(SqlBaseParser.RowFormatDelimitedContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#multipartIdentifierList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMultipartIdentifierList(SqlBaseParser.MultipartIdentifierListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#multipartIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMultipartIdentifier(SqlBaseParser.MultipartIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#multipartIdentifierPropertyList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMultipartIdentifierPropertyList(SqlBaseParser.MultipartIdentifierPropertyListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#multipartIdentifierProperty}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMultipartIdentifierProperty(SqlBaseParser.MultipartIdentifierPropertyContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#tableIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableIdentifier(SqlBaseParser.TableIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#functionIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionIdentifier(SqlBaseParser.FunctionIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#namedExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNamedExpression(SqlBaseParser.NamedExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#namedExpressionSeq}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNamedExpressionSeq(SqlBaseParser.NamedExpressionSeqContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#partitionFieldList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPartitionFieldList(SqlBaseParser.PartitionFieldListContext ctx);
	/**
	 * Visit a parse tree produced by the {@code partitionTransform}
	 * labeled alternative in {@link SqlBaseParser#partitionField}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPartitionTransform(SqlBaseParser.PartitionTransformContext ctx);
	/**
	 * Visit a parse tree produced by the {@code partitionColumn}
	 * labeled alternative in {@link SqlBaseParser#partitionField}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPartitionColumn(SqlBaseParser.PartitionColumnContext ctx);
	/**
	 * Visit a parse tree produced by the {@code identityTransform}
	 * labeled alternative in {@link SqlBaseParser#transform}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentityTransform(SqlBaseParser.IdentityTransformContext ctx);
	/**
	 * Visit a parse tree produced by the {@code applyTransform}
	 * labeled alternative in {@link SqlBaseParser#transform}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitApplyTransform(SqlBaseParser.ApplyTransformContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#transformArgument}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTransformArgument(SqlBaseParser.TransformArgumentContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpression(SqlBaseParser.ExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#namedArgumentExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNamedArgumentExpression(SqlBaseParser.NamedArgumentExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#functionArgument}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionArgument(SqlBaseParser.FunctionArgumentContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#expressionSeq}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpressionSeq(SqlBaseParser.ExpressionSeqContext ctx);
	/**
	 * Visit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogicalNot(SqlBaseParser.LogicalNotContext ctx);
	/**
	 * Visit a parse tree produced by the {@code predicated}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPredicated(SqlBaseParser.PredicatedContext ctx);
	/**
	 * Visit a parse tree produced by the {@code exists}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExists(SqlBaseParser.ExistsContext ctx);
	/**
	 * Visit a parse tree produced by the {@code logicalBinary}
	 * labeled alternative in {@link SqlBaseParser#booleanExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLogicalBinary(SqlBaseParser.LogicalBinaryContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPredicate(SqlBaseParser.PredicateContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#errorCapturingNot}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitErrorCapturingNot(SqlBaseParser.ErrorCapturingNotContext ctx);
	/**
	 * Visit a parse tree produced by the {@code valueExpressionDefault}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValueExpressionDefault(SqlBaseParser.ValueExpressionDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code comparison}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComparison(SqlBaseParser.ComparisonContext ctx);
	/**
	 * Visit a parse tree produced by the {@code shiftExpression}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShiftExpression(SqlBaseParser.ShiftExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code arithmeticBinary}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext ctx);
	/**
	 * Visit a parse tree produced by the {@code arithmeticUnary}
	 * labeled alternative in {@link SqlBaseParser#valueExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#shiftOperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitShiftOperator(SqlBaseParser.ShiftOperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#datetimeUnit}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDatetimeUnit(SqlBaseParser.DatetimeUnitContext ctx);
	/**
	 * Visit a parse tree produced by the {@code struct}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStruct(SqlBaseParser.StructContext ctx);
	/**
	 * Visit a parse tree produced by the {@code dereference}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDereference(SqlBaseParser.DereferenceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code castByColon}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCastByColon(SqlBaseParser.CastByColonContext ctx);
	/**
	 * Visit a parse tree produced by the {@code timestampadd}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTimestampadd(SqlBaseParser.TimestampaddContext ctx);
	/**
	 * Visit a parse tree produced by the {@code substring}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubstring(SqlBaseParser.SubstringContext ctx);
	/**
	 * Visit a parse tree produced by the {@code cast}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCast(SqlBaseParser.CastContext ctx);
	/**
	 * Visit a parse tree produced by the {@code lambda}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLambda(SqlBaseParser.LambdaContext ctx);
	/**
	 * Visit a parse tree produced by the {@code parenthesizedExpression}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code any_value}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAny_value(SqlBaseParser.Any_valueContext ctx);
	/**
	 * Visit a parse tree produced by the {@code trim}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTrim(SqlBaseParser.TrimContext ctx);
	/**
	 * Visit a parse tree produced by the {@code semiStructuredExtract}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSemiStructuredExtract(SqlBaseParser.SemiStructuredExtractContext ctx);
	/**
	 * Visit a parse tree produced by the {@code simpleCase}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSimpleCase(SqlBaseParser.SimpleCaseContext ctx);
	/**
	 * Visit a parse tree produced by the {@code currentLike}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCurrentLike(SqlBaseParser.CurrentLikeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code columnReference}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumnReference(SqlBaseParser.ColumnReferenceContext ctx);
	/**
	 * Visit a parse tree produced by the {@code rowConstructor}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRowConstructor(SqlBaseParser.RowConstructorContext ctx);
	/**
	 * Visit a parse tree produced by the {@code last}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLast(SqlBaseParser.LastContext ctx);
	/**
	 * Visit a parse tree produced by the {@code star}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStar(SqlBaseParser.StarContext ctx);
	/**
	 * Visit a parse tree produced by the {@code overlay}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOverlay(SqlBaseParser.OverlayContext ctx);
	/**
	 * Visit a parse tree produced by the {@code subscript}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubscript(SqlBaseParser.SubscriptContext ctx);
	/**
	 * Visit a parse tree produced by the {@code timestampdiff}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTimestampdiff(SqlBaseParser.TimestampdiffContext ctx);
	/**
	 * Visit a parse tree produced by the {@code subqueryExpression}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code collate}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCollate(SqlBaseParser.CollateContext ctx);
	/**
	 * Visit a parse tree produced by the {@code constantDefault}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConstantDefault(SqlBaseParser.ConstantDefaultContext ctx);
	/**
	 * Visit a parse tree produced by the {@code extract}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExtract(SqlBaseParser.ExtractContext ctx);
	/**
	 * Visit a parse tree produced by the {@code functionCall}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionCall(SqlBaseParser.FunctionCallContext ctx);
	/**
	 * Visit a parse tree produced by the {@code searchedCase}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSearchedCase(SqlBaseParser.SearchedCaseContext ctx);
	/**
	 * Visit a parse tree produced by the {@code position}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPosition(SqlBaseParser.PositionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code first}
	 * labeled alternative in {@link SqlBaseParser#primaryExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFirst(SqlBaseParser.FirstContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#semiStructuredExtractionPath}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSemiStructuredExtractionPath(SqlBaseParser.SemiStructuredExtractionPathContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#jsonPathIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJsonPathIdentifier(SqlBaseParser.JsonPathIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#jsonPathBracketedIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJsonPathBracketedIdentifier(SqlBaseParser.JsonPathBracketedIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#jsonPathFirstPart}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJsonPathFirstPart(SqlBaseParser.JsonPathFirstPartContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#jsonPathParts}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitJsonPathParts(SqlBaseParser.JsonPathPartsContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#literalType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLiteralType(SqlBaseParser.LiteralTypeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code nullLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNullLiteral(SqlBaseParser.NullLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code posParameterLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPosParameterLiteral(SqlBaseParser.PosParameterLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code namedParameterLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNamedParameterLiteral(SqlBaseParser.NamedParameterLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code intervalLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIntervalLiteral(SqlBaseParser.IntervalLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code typeConstructor}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTypeConstructor(SqlBaseParser.TypeConstructorContext ctx);
	/**
	 * Visit a parse tree produced by the {@code numericLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNumericLiteral(SqlBaseParser.NumericLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code booleanLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBooleanLiteral(SqlBaseParser.BooleanLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code stringLiteral}
	 * labeled alternative in {@link SqlBaseParser#constant}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStringLiteral(SqlBaseParser.StringLiteralContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#comparisonOperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComparisonOperator(SqlBaseParser.ComparisonOperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#arithmeticOperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArithmeticOperator(SqlBaseParser.ArithmeticOperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#predicateOperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPredicateOperator(SqlBaseParser.PredicateOperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#booleanValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBooleanValue(SqlBaseParser.BooleanValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#interval}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitInterval(SqlBaseParser.IntervalContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#errorCapturingMultiUnitsInterval}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitErrorCapturingMultiUnitsInterval(SqlBaseParser.ErrorCapturingMultiUnitsIntervalContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#multiUnitsInterval}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMultiUnitsInterval(SqlBaseParser.MultiUnitsIntervalContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#errorCapturingUnitToUnitInterval}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitErrorCapturingUnitToUnitInterval(SqlBaseParser.ErrorCapturingUnitToUnitIntervalContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#unitToUnitInterval}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnitToUnitInterval(SqlBaseParser.UnitToUnitIntervalContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#intervalValue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIntervalValue(SqlBaseParser.IntervalValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#unitInMultiUnits}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnitInMultiUnits(SqlBaseParser.UnitInMultiUnitsContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#unitInUnitToUnit}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnitInUnitToUnit(SqlBaseParser.UnitInUnitToUnitContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#colPosition}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColPosition(SqlBaseParser.ColPositionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#collationSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCollationSpec(SqlBaseParser.CollationSpecContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#collateClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCollateClause(SqlBaseParser.CollateClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#nonTrivialPrimitiveType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNonTrivialPrimitiveType(SqlBaseParser.NonTrivialPrimitiveTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#trivialPrimitiveType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTrivialPrimitiveType(SqlBaseParser.TrivialPrimitiveTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#primitiveType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPrimitiveType(SqlBaseParser.PrimitiveTypeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code complexDataType}
	 * labeled alternative in {@link SqlBaseParser#dataType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComplexDataType(SqlBaseParser.ComplexDataTypeContext ctx);
	/**
	 * Visit a parse tree produced by the {@code primitiveDataType}
	 * labeled alternative in {@link SqlBaseParser#dataType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPrimitiveDataType(SqlBaseParser.PrimitiveDataTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#qualifiedColTypeWithPositionList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQualifiedColTypeWithPositionList(SqlBaseParser.QualifiedColTypeWithPositionListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#qualifiedColTypeWithPosition}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQualifiedColTypeWithPosition(SqlBaseParser.QualifiedColTypeWithPositionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#colDefinitionDescriptorWithPosition}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColDefinitionDescriptorWithPosition(SqlBaseParser.ColDefinitionDescriptorWithPositionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#defaultExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDefaultExpression(SqlBaseParser.DefaultExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#variableDefaultExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitVariableDefaultExpression(SqlBaseParser.VariableDefaultExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#colTypeList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColTypeList(SqlBaseParser.ColTypeListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#colType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColType(SqlBaseParser.ColTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#tableElementList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableElementList(SqlBaseParser.TableElementListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#tableElement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableElement(SqlBaseParser.TableElementContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#colDefinitionList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColDefinitionList(SqlBaseParser.ColDefinitionListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#colDefinition}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColDefinition(SqlBaseParser.ColDefinitionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#colDefinitionOption}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColDefinitionOption(SqlBaseParser.ColDefinitionOptionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code generatedColumn}
	 * labeled alternative in {@link SqlBaseParser#generationExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGeneratedColumn(SqlBaseParser.GeneratedColumnContext ctx);
	/**
	 * Visit a parse tree produced by the {@code identityColumn}
	 * labeled alternative in {@link SqlBaseParser#generationExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentityColumn(SqlBaseParser.IdentityColumnContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#identityColSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentityColSpec(SqlBaseParser.IdentityColSpecContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#sequenceGeneratorOption}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSequenceGeneratorOption(SqlBaseParser.SequenceGeneratorOptionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#sequenceGeneratorStartOrStep}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSequenceGeneratorStartOrStep(SqlBaseParser.SequenceGeneratorStartOrStepContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#complexColTypeList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComplexColTypeList(SqlBaseParser.ComplexColTypeListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#complexColType}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComplexColType(SqlBaseParser.ComplexColTypeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#routineCharacteristics}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRoutineCharacteristics(SqlBaseParser.RoutineCharacteristicsContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#routineLanguage}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRoutineLanguage(SqlBaseParser.RoutineLanguageContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#specificName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSpecificName(SqlBaseParser.SpecificNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#deterministic}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDeterministic(SqlBaseParser.DeterministicContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#sqlDataAccess}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSqlDataAccess(SqlBaseParser.SqlDataAccessContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#nullCall}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNullCall(SqlBaseParser.NullCallContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#rightsClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRightsClause(SqlBaseParser.RightsClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#whenClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWhenClause(SqlBaseParser.WhenClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#windowClause}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWindowClause(SqlBaseParser.WindowClauseContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#namedWindow}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNamedWindow(SqlBaseParser.NamedWindowContext ctx);
	/**
	 * Visit a parse tree produced by the {@code windowRef}
	 * labeled alternative in {@link SqlBaseParser#windowSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWindowRef(SqlBaseParser.WindowRefContext ctx);
	/**
	 * Visit a parse tree produced by the {@code windowDef}
	 * labeled alternative in {@link SqlBaseParser#windowSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWindowDef(SqlBaseParser.WindowDefContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#windowFrame}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWindowFrame(SqlBaseParser.WindowFrameContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#frameBound}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFrameBound(SqlBaseParser.FrameBoundContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#qualifiedNameList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQualifiedNameList(SqlBaseParser.QualifiedNameListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#functionName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctionName(SqlBaseParser.FunctionNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#qualifiedName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQualifiedName(SqlBaseParser.QualifiedNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#errorCapturingIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitErrorCapturingIdentifier(SqlBaseParser.ErrorCapturingIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by the {@code errorIdent}
	 * labeled alternative in {@link SqlBaseParser#errorCapturingIdentifierExtra}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitErrorIdent(SqlBaseParser.ErrorIdentContext ctx);
	/**
	 * Visit a parse tree produced by the {@code realIdent}
	 * labeled alternative in {@link SqlBaseParser#errorCapturingIdentifierExtra}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRealIdent(SqlBaseParser.RealIdentContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifier(SqlBaseParser.IdentifierContext ctx);
	/**
	 * Visit a parse tree produced by the {@code unquotedIdentifier}
	 * labeled alternative in {@link SqlBaseParser#strictIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by the {@code quotedIdentifierAlternative}
	 * labeled alternative in {@link SqlBaseParser#strictIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuotedIdentifierAlternative(SqlBaseParser.QuotedIdentifierAlternativeContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#quotedIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#backQuotedIdentifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBackQuotedIdentifier(SqlBaseParser.BackQuotedIdentifierContext ctx);
	/**
	 * Visit a parse tree produced by the {@code exponentLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExponentLiteral(SqlBaseParser.ExponentLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code decimalLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDecimalLiteral(SqlBaseParser.DecimalLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code legacyDecimalLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLegacyDecimalLiteral(SqlBaseParser.LegacyDecimalLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code integerLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIntegerLiteral(SqlBaseParser.IntegerLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code bigIntLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBigIntLiteral(SqlBaseParser.BigIntLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code smallIntLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSmallIntLiteral(SqlBaseParser.SmallIntLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code tinyIntLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTinyIntLiteral(SqlBaseParser.TinyIntLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code doubleLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDoubleLiteral(SqlBaseParser.DoubleLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code floatLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFloatLiteral(SqlBaseParser.FloatLiteralContext ctx);
	/**
	 * Visit a parse tree produced by the {@code bigDecimalLiteral}
	 * labeled alternative in {@link SqlBaseParser#number}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBigDecimalLiteral(SqlBaseParser.BigDecimalLiteralContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#columnConstraintDefinition}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumnConstraintDefinition(SqlBaseParser.ColumnConstraintDefinitionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#columnConstraint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumnConstraint(SqlBaseParser.ColumnConstraintContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#tableConstraintDefinition}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableConstraintDefinition(SqlBaseParser.TableConstraintDefinitionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#tableConstraint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTableConstraint(SqlBaseParser.TableConstraintContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#checkConstraint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCheckConstraint(SqlBaseParser.CheckConstraintContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#uniqueSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUniqueSpec(SqlBaseParser.UniqueSpecContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#uniqueConstraint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUniqueConstraint(SqlBaseParser.UniqueConstraintContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#referenceSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitReferenceSpec(SqlBaseParser.ReferenceSpecContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#foreignKeyConstraint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitForeignKeyConstraint(SqlBaseParser.ForeignKeyConstraintContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#constraintCharacteristic}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConstraintCharacteristic(SqlBaseParser.ConstraintCharacteristicContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#enforcedCharacteristic}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitEnforcedCharacteristic(SqlBaseParser.EnforcedCharacteristicContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#relyCharacteristic}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRelyCharacteristic(SqlBaseParser.RelyCharacteristicContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#alterColumnSpecList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAlterColumnSpecList(SqlBaseParser.AlterColumnSpecListContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#alterColumnSpec}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAlterColumnSpec(SqlBaseParser.AlterColumnSpecContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#alterColumnAction}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAlterColumnAction(SqlBaseParser.AlterColumnActionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#stringLit}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStringLit(SqlBaseParser.StringLitContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#comment}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComment(SqlBaseParser.CommentContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#version}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitVersion(SqlBaseParser.VersionContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#operatorPipeRightSide}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOperatorPipeRightSide(SqlBaseParser.OperatorPipeRightSideContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#operatorPipeSetAssignmentSeq}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOperatorPipeSetAssignmentSeq(SqlBaseParser.OperatorPipeSetAssignmentSeqContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#ansiNonReserved}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAnsiNonReserved(SqlBaseParser.AnsiNonReservedContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#strictNonReserved}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStrictNonReserved(SqlBaseParser.StrictNonReservedContext ctx);
	/**
	 * Visit a parse tree produced by {@link SqlBaseParser#nonReserved}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNonReserved(SqlBaseParser.NonReservedContext ctx);
}