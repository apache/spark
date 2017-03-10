// Generated from org/apache/spark/sql/catalyst/parser/SqlBase.g4 by ANTLR 4.5.3
package org.apache.spark.sql.catalyst.parser;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class SqlBaseParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.5.3", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, SELECT=8, FROM=9, 
		ADD=10, AS=11, ALL=12, DISTINCT=13, WHERE=14, GROUP=15, BY=16, GROUPING=17, 
		SETS=18, CUBE=19, ROLLUP=20, ORDER=21, HAVING=22, LIMIT=23, AT=24, OR=25, 
		AND=26, IN=27, NOT=28, NO=29, EXISTS=30, BETWEEN=31, LIKE=32, RLIKE=33, 
		IS=34, NULL=35, TRUE=36, FALSE=37, NULLS=38, ASC=39, DESC=40, FOR=41, 
		INTERVAL=42, CASE=43, WHEN=44, THEN=45, ELSE=46, END=47, JOIN=48, CROSS=49, 
		OUTER=50, INNER=51, LEFT=52, SEMI=53, RIGHT=54, FULL=55, NATURAL=56, ON=57, 
		LATERAL=58, WINDOW=59, OVER=60, PARTITION=61, RANGE=62, ROWS=63, UNBOUNDED=64, 
		PRECEDING=65, FOLLOWING=66, CURRENT=67, FIRST=68, AFTER=69, LAST=70, ROW=71, 
		WITH=72, VALUES=73, CREATE=74, TABLE=75, VIEW=76, REPLACE=77, INSERT=78, 
		DELETE=79, INTO=80, DESCRIBE=81, EXPLAIN=82, FORMAT=83, LOGICAL=84, CODEGEN=85, 
		CAST=86, SHOW=87, TABLES=88, COLUMNS=89, COLUMN=90, USE=91, PARTITIONS=92, 
		FUNCTIONS=93, DROP=94, UNION=95, EXCEPT=96, SETMINUS=97, INTERSECT=98, 
		TO=99, TABLESAMPLE=100, STRATIFY=101, ALTER=102, RENAME=103, ARRAY=104, 
		MAP=105, STRUCT=106, COMMENT=107, SET=108, RESET=109, DATA=110, START=111, 
		TRANSACTION=112, COMMIT=113, ROLLBACK=114, MACRO=115, IF=116, EQ=117, 
		NSEQ=118, NEQ=119, NEQJ=120, LT=121, LTE=122, GT=123, GTE=124, PLUS=125, 
		MINUS=126, ASTERISK=127, SLASH=128, PERCENT=129, DIV=130, TILDE=131, AMPERSAND=132, 
		PIPE=133, HAT=134, PERCENTLIT=135, BUCKET=136, OUT=137, OF=138, SORT=139, 
		CLUSTER=140, DISTRIBUTE=141, OVERWRITE=142, TRANSFORM=143, REDUCE=144, 
		USING=145, SERDE=146, SERDEPROPERTIES=147, RECORDREADER=148, RECORDWRITER=149, 
		DELIMITED=150, FIELDS=151, TERMINATED=152, COLLECTION=153, ITEMS=154, 
		KEYS=155, ESCAPED=156, LINES=157, SEPARATED=158, FUNCTION=159, EXTENDED=160, 
		REFRESH=161, CLEAR=162, CACHE=163, UNCACHE=164, LAZY=165, FORMATTED=166, 
		GLOBAL=167, TEMPORARY=168, OPTIONS=169, UNSET=170, TBLPROPERTIES=171, 
		DBPROPERTIES=172, BUCKETS=173, SKEWED=174, STORED=175, DIRECTORIES=176, 
		LOCATION=177, EXCHANGE=178, ARCHIVE=179, UNARCHIVE=180, FILEFORMAT=181, 
		TOUCH=182, COMPACT=183, CONCATENATE=184, CHANGE=185, CASCADE=186, RESTRICT=187, 
		CLUSTERED=188, SORTED=189, PURGE=190, INPUTFORMAT=191, OUTPUTFORMAT=192, 
		DATABASE=193, DATABASES=194, DFS=195, TRUNCATE=196, ANALYZE=197, COMPUTE=198, 
		LIST=199, STATISTICS=200, PARTITIONED=201, EXTERNAL=202, DEFINED=203, 
		REVOKE=204, GRANT=205, LOCK=206, UNLOCK=207, MSCK=208, REPAIR=209, RECOVER=210, 
		EXPORT=211, IMPORT=212, LOAD=213, ROLE=214, ROLES=215, COMPACTIONS=216, 
		PRINCIPALS=217, TRANSACTIONS=218, INDEX=219, INDEXES=220, LOCKS=221, OPTION=222, 
		ANTI=223, LOCAL=224, INPATH=225, CURRENT_DATE=226, CURRENT_TIMESTAMP=227, 
		STRING=228, BIGINT_LITERAL=229, SMALLINT_LITERAL=230, TINYINT_LITERAL=231, 
		BYTELENGTH_LITERAL=232, INTEGER_VALUE=233, DECIMAL_VALUE=234, DOUBLE_LITERAL=235, 
		BIGDECIMAL_LITERAL=236, IDENTIFIER=237, BACKQUOTED_IDENTIFIER=238, SIMPLE_COMMENT=239, 
		BRACKETED_COMMENT=240, WS=241, UNRECOGNIZED=242, DELIMITER=243;
	public static final int
		RULE_singleStatement = 0, RULE_singleExpression = 1, RULE_singleTableIdentifier = 2, 
		RULE_singleDataType = 3, RULE_statement = 4, RULE_unsupportedHiveNativeCommands = 5, 
		RULE_createTableHeader = 6, RULE_bucketSpec = 7, RULE_skewSpec = 8, RULE_locationSpec = 9, 
		RULE_query = 10, RULE_insertInto = 11, RULE_partitionSpecLocation = 12, 
		RULE_partitionSpec = 13, RULE_partitionVal = 14, RULE_describeFuncName = 15, 
		RULE_describeColName = 16, RULE_ctes = 17, RULE_namedQuery = 18, RULE_tableProvider = 19, 
		RULE_tablePropertyList = 20, RULE_tableProperty = 21, RULE_tablePropertyKey = 22, 
		RULE_tablePropertyValue = 23, RULE_constantList = 24, RULE_nestedConstantList = 25, 
		RULE_createFileFormat = 26, RULE_fileFormat = 27, RULE_storageHandler = 28, 
		RULE_resource = 29, RULE_queryNoWith = 30, RULE_queryOrganization = 31, 
		RULE_multiInsertQueryBody = 32, RULE_queryTerm = 33, RULE_queryPrimary = 34, 
		RULE_sortItem = 35, RULE_querySpecification = 36, RULE_fromClause = 37, 
		RULE_aggregation = 38, RULE_groupingSet = 39, RULE_lateralView = 40, RULE_setQuantifier = 41, 
		RULE_relation = 42, RULE_joinRelation = 43, RULE_joinType = 44, RULE_joinCriteria = 45, 
		RULE_sample = 46, RULE_identifierList = 47, RULE_identifierSeq = 48, RULE_orderedIdentifierList = 49, 
		RULE_orderedIdentifier = 50, RULE_identifierCommentList = 51, RULE_identifierComment = 52, 
		RULE_relationPrimary = 53, RULE_inlineTable = 54, RULE_rowFormat = 55, 
		RULE_tableIdentifier = 56, RULE_namedExpression = 57, RULE_namedExpressionSeq = 58, 
		RULE_expression = 59, RULE_booleanExpression = 60, RULE_predicated = 61, 
		RULE_predicate = 62, RULE_valueExpression = 63, RULE_primaryExpression = 64, 
		RULE_constant = 65, RULE_comparisonOperator = 66, RULE_arithmeticOperator = 67, 
		RULE_predicateOperator = 68, RULE_booleanValue = 69, RULE_interval = 70, 
		RULE_intervalField = 71, RULE_intervalValue = 72, RULE_colPosition = 73, 
		RULE_dataType = 74, RULE_colTypeList = 75, RULE_colType = 76, RULE_complexColTypeList = 77, 
		RULE_complexColType = 78, RULE_whenClause = 79, RULE_windows = 80, RULE_namedWindow = 81, 
		RULE_windowSpec = 82, RULE_windowFrame = 83, RULE_frameBound = 84, RULE_qualifiedName = 85, 
		RULE_identifier = 86, RULE_strictIdentifier = 87, RULE_quotedIdentifier = 88, 
		RULE_number = 89, RULE_nonReserved = 90;
	public static final String[] ruleNames = {
		"singleStatement", "singleExpression", "singleTableIdentifier", "singleDataType", 
		"statement", "unsupportedHiveNativeCommands", "createTableHeader", "bucketSpec", 
		"skewSpec", "locationSpec", "query", "insertInto", "partitionSpecLocation", 
		"partitionSpec", "partitionVal", "describeFuncName", "describeColName", 
		"ctes", "namedQuery", "tableProvider", "tablePropertyList", "tableProperty", 
		"tablePropertyKey", "tablePropertyValue", "constantList", "nestedConstantList", 
		"createFileFormat", "fileFormat", "storageHandler", "resource", "queryNoWith", 
		"queryOrganization", "multiInsertQueryBody", "queryTerm", "queryPrimary", 
		"sortItem", "querySpecification", "fromClause", "aggregation", "groupingSet", 
		"lateralView", "setQuantifier", "relation", "joinRelation", "joinType", 
		"joinCriteria", "sample", "identifierList", "identifierSeq", "orderedIdentifierList", 
		"orderedIdentifier", "identifierCommentList", "identifierComment", "relationPrimary", 
		"inlineTable", "rowFormat", "tableIdentifier", "namedExpression", "namedExpressionSeq", 
		"expression", "booleanExpression", "predicated", "predicate", "valueExpression", 
		"primaryExpression", "constant", "comparisonOperator", "arithmeticOperator", 
		"predicateOperator", "booleanValue", "interval", "intervalField", "intervalValue", 
		"colPosition", "dataType", "colTypeList", "colType", "complexColTypeList", 
		"complexColType", "whenClause", "windows", "namedWindow", "windowSpec", 
		"windowFrame", "frameBound", "qualifiedName", "identifier", "strictIdentifier", 
		"quotedIdentifier", "number", "nonReserved"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'('", "')'", "','", "'.'", "'['", "']'", "':'", "'SELECT'", "'FROM'", 
		"'ADD'", "'AS'", "'ALL'", "'DISTINCT'", "'WHERE'", "'GROUP'", "'BY'", 
		"'GROUPING'", "'SETS'", "'CUBE'", "'ROLLUP'", "'ORDER'", "'HAVING'", "'LIMIT'", 
		"'AT'", "'OR'", "'AND'", "'IN'", null, "'NO'", "'EXISTS'", "'BETWEEN'", 
		"'LIKE'", null, "'IS'", "'NULL'", "'TRUE'", "'FALSE'", "'NULLS'", "'ASC'", 
		"'DESC'", "'FOR'", "'INTERVAL'", "'CASE'", "'WHEN'", "'THEN'", "'ELSE'", 
		"'END'", "'JOIN'", "'CROSS'", "'OUTER'", "'INNER'", "'LEFT'", "'SEMI'", 
		"'RIGHT'", "'FULL'", "'NATURAL'", "'ON'", "'LATERAL'", "'WINDOW'", "'OVER'", 
		"'PARTITION'", "'RANGE'", "'ROWS'", "'UNBOUNDED'", "'PRECEDING'", "'FOLLOWING'", 
		"'CURRENT'", "'FIRST'", "'AFTER'", "'LAST'", "'ROW'", "'WITH'", "'VALUES'", 
		"'CREATE'", "'TABLE'", "'VIEW'", "'REPLACE'", "'INSERT'", "'DELETE'", 
		"'INTO'", "'DESCRIBE'", "'EXPLAIN'", "'FORMAT'", "'LOGICAL'", "'CODEGEN'", 
		"'CAST'", "'SHOW'", "'TABLES'", "'COLUMNS'", "'COLUMN'", "'USE'", "'PARTITIONS'", 
		"'FUNCTIONS'", "'DROP'", "'UNION'", "'EXCEPT'", "'MINUS'", "'INTERSECT'", 
		"'TO'", "'TABLESAMPLE'", "'STRATIFY'", "'ALTER'", "'RENAME'", "'ARRAY'", 
		"'MAP'", "'STRUCT'", "'COMMENT'", "'SET'", "'RESET'", "'DATA'", "'START'", 
		"'TRANSACTION'", "'COMMIT'", "'ROLLBACK'", "'MACRO'", "'IF'", null, "'<=>'", 
		"'<>'", "'!='", "'<'", null, "'>'", null, "'+'", "'-'", "'*'", "'/'", 
		"'%'", "'DIV'", "'~'", "'&'", "'|'", "'^'", "'PERCENT'", "'BUCKET'", "'OUT'", 
		"'OF'", "'SORT'", "'CLUSTER'", "'DISTRIBUTE'", "'OVERWRITE'", "'TRANSFORM'", 
		"'REDUCE'", "'USING'", "'SERDE'", "'SERDEPROPERTIES'", "'RECORDREADER'", 
		"'RECORDWRITER'", "'DELIMITED'", "'FIELDS'", "'TERMINATED'", "'COLLECTION'", 
		"'ITEMS'", "'KEYS'", "'ESCAPED'", "'LINES'", "'SEPARATED'", "'FUNCTION'", 
		"'EXTENDED'", "'REFRESH'", "'CLEAR'", "'CACHE'", "'UNCACHE'", "'LAZY'", 
		"'FORMATTED'", "'GLOBAL'", null, "'OPTIONS'", "'UNSET'", "'TBLPROPERTIES'", 
		"'DBPROPERTIES'", "'BUCKETS'", "'SKEWED'", "'STORED'", "'DIRECTORIES'", 
		"'LOCATION'", "'EXCHANGE'", "'ARCHIVE'", "'UNARCHIVE'", "'FILEFORMAT'", 
		"'TOUCH'", "'COMPACT'", "'CONCATENATE'", "'CHANGE'", "'CASCADE'", "'RESTRICT'", 
		"'CLUSTERED'", "'SORTED'", "'PURGE'", "'INPUTFORMAT'", "'OUTPUTFORMAT'", 
		null, null, "'DFS'", "'TRUNCATE'", "'ANALYZE'", "'COMPUTE'", "'LIST'", 
		"'STATISTICS'", "'PARTITIONED'", "'EXTERNAL'", "'DEFINED'", "'REVOKE'", 
		"'GRANT'", "'LOCK'", "'UNLOCK'", "'MSCK'", "'REPAIR'", "'RECOVER'", "'EXPORT'", 
		"'IMPORT'", "'LOAD'", "'ROLE'", "'ROLES'", "'COMPACTIONS'", "'PRINCIPALS'", 
		"'TRANSACTIONS'", "'INDEX'", "'INDEXES'", "'LOCKS'", "'OPTION'", "'ANTI'", 
		"'LOCAL'", "'INPATH'", "'CURRENT_DATE'", "'CURRENT_TIMESTAMP'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, "SELECT", "FROM", "ADD", 
		"AS", "ALL", "DISTINCT", "WHERE", "GROUP", "BY", "GROUPING", "SETS", "CUBE", 
		"ROLLUP", "ORDER", "HAVING", "LIMIT", "AT", "OR", "AND", "IN", "NOT", 
		"NO", "EXISTS", "BETWEEN", "LIKE", "RLIKE", "IS", "NULL", "TRUE", "FALSE", 
		"NULLS", "ASC", "DESC", "FOR", "INTERVAL", "CASE", "WHEN", "THEN", "ELSE", 
		"END", "JOIN", "CROSS", "OUTER", "INNER", "LEFT", "SEMI", "RIGHT", "FULL", 
		"NATURAL", "ON", "LATERAL", "WINDOW", "OVER", "PARTITION", "RANGE", "ROWS", 
		"UNBOUNDED", "PRECEDING", "FOLLOWING", "CURRENT", "FIRST", "AFTER", "LAST", 
		"ROW", "WITH", "VALUES", "CREATE", "TABLE", "VIEW", "REPLACE", "INSERT", 
		"DELETE", "INTO", "DESCRIBE", "EXPLAIN", "FORMAT", "LOGICAL", "CODEGEN", 
		"CAST", "SHOW", "TABLES", "COLUMNS", "COLUMN", "USE", "PARTITIONS", "FUNCTIONS", 
		"DROP", "UNION", "EXCEPT", "SETMINUS", "INTERSECT", "TO", "TABLESAMPLE", 
		"STRATIFY", "ALTER", "RENAME", "ARRAY", "MAP", "STRUCT", "COMMENT", "SET", 
		"RESET", "DATA", "START", "TRANSACTION", "COMMIT", "ROLLBACK", "MACRO", 
		"IF", "EQ", "NSEQ", "NEQ", "NEQJ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", 
		"ASTERISK", "SLASH", "PERCENT", "DIV", "TILDE", "AMPERSAND", "PIPE", "HAT", 
		"PERCENTLIT", "BUCKET", "OUT", "OF", "SORT", "CLUSTER", "DISTRIBUTE", 
		"OVERWRITE", "TRANSFORM", "REDUCE", "USING", "SERDE", "SERDEPROPERTIES", 
		"RECORDREADER", "RECORDWRITER", "DELIMITED", "FIELDS", "TERMINATED", "COLLECTION", 
		"ITEMS", "KEYS", "ESCAPED", "LINES", "SEPARATED", "FUNCTION", "EXTENDED", 
		"REFRESH", "CLEAR", "CACHE", "UNCACHE", "LAZY", "FORMATTED", "GLOBAL", 
		"TEMPORARY", "OPTIONS", "UNSET", "TBLPROPERTIES", "DBPROPERTIES", "BUCKETS", 
		"SKEWED", "STORED", "DIRECTORIES", "LOCATION", "EXCHANGE", "ARCHIVE", 
		"UNARCHIVE", "FILEFORMAT", "TOUCH", "COMPACT", "CONCATENATE", "CHANGE", 
		"CASCADE", "RESTRICT", "CLUSTERED", "SORTED", "PURGE", "INPUTFORMAT", 
		"OUTPUTFORMAT", "DATABASE", "DATABASES", "DFS", "TRUNCATE", "ANALYZE", 
		"COMPUTE", "LIST", "STATISTICS", "PARTITIONED", "EXTERNAL", "DEFINED", 
		"REVOKE", "GRANT", "LOCK", "UNLOCK", "MSCK", "REPAIR", "RECOVER", "EXPORT", 
		"IMPORT", "LOAD", "ROLE", "ROLES", "COMPACTIONS", "PRINCIPALS", "TRANSACTIONS", 
		"INDEX", "INDEXES", "LOCKS", "OPTION", "ANTI", "LOCAL", "INPATH", "CURRENT_DATE", 
		"CURRENT_TIMESTAMP", "STRING", "BIGINT_LITERAL", "SMALLINT_LITERAL", "TINYINT_LITERAL", 
		"BYTELENGTH_LITERAL", "INTEGER_VALUE", "DECIMAL_VALUE", "DOUBLE_LITERAL", 
		"BIGDECIMAL_LITERAL", "IDENTIFIER", "BACKQUOTED_IDENTIFIER", "SIMPLE_COMMENT", 
		"BRACKETED_COMMENT", "WS", "UNRECOGNIZED", "DELIMITER"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "SqlBase.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }


	  /**
	   * Verify whether current token is a valid decimal token (which contains dot).
	   * Returns true if the character that follows the token is not a digit or letter or underscore.
	   *
	   * For example:
	   * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
	   * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
	   * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
	   * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is folllowed
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

	public SqlBaseParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class SingleStatementContext extends ParserRuleContext {
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleStatementContext singleStatement() throws RecognitionException {
		SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_singleStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(182);
			statement();
			setState(183);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleExpressionContext extends ParserRuleContext {
		public NamedExpressionContext namedExpression() {
			return getRuleContext(NamedExpressionContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleExpressionContext singleExpression() throws RecognitionException {
		SingleExpressionContext _localctx = new SingleExpressionContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_singleExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(185);
			namedExpression();
			setState(186);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleTableIdentifierContext extends ParserRuleContext {
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleTableIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleTableIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleTableIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleTableIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleTableIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleTableIdentifierContext singleTableIdentifier() throws RecognitionException {
		SingleTableIdentifierContext _localctx = new SingleTableIdentifierContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_singleTableIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(188);
			tableIdentifier();
			setState(189);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleDataTypeContext extends ParserRuleContext {
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public TerminalNode EOF() { return getToken(SqlBaseParser.EOF, 0); }
		public SingleDataTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleDataType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleDataType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleDataType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleDataTypeContext singleDataType() throws RecognitionException {
		SingleDataTypeContext _localctx = new SingleDataTypeContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_singleDataType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(191);
			dataType();
			setState(192);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StatementContext extends ParserRuleContext {
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
	 
		public StatementContext() { }
		public void copyFrom(StatementContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ManageResourceContext extends StatementContext {
		public Token op;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode LIST() { return getToken(SqlBaseParser.LIST, 0); }
		public ManageResourceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterManageResource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitManageResource(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitManageResource(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateViewContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public IdentifierCommentListContext identifierCommentList() {
			return getRuleContext(IdentifierCommentListContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode PARTITIONED() { return getToken(SqlBaseParser.PARTITIONED, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode GLOBAL() { return getToken(SqlBaseParser.GLOBAL, 0); }
		public CreateViewContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateView(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RenameTableContext extends StatementContext {
		public TableIdentifierContext from;
		public TableIdentifierContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public List<TableIdentifierContext> tableIdentifier() {
			return getRuleContexts(TableIdentifierContext.class);
		}
		public TableIdentifierContext tableIdentifier(int i) {
			return getRuleContext(TableIdentifierContext.class,i);
		}
		public RenameTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRenameTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowPartitionsContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public ShowPartitionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowPartitions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowPartitions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowPartitions(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DescribeFunctionContext extends StatementContext {
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public DescribeFuncNameContext describeFuncName() {
			return getRuleContext(DescribeFuncNameContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public DescribeFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ResetConfigurationContext extends StatementContext {
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public ResetConfigurationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterResetConfiguration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitResetConfiguration(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitResetConfiguration(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowColumnsContext extends StatementContext {
		public IdentifierContext db;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public List<TerminalNode> FROM() { return getTokens(SqlBaseParser.FROM); }
		public TerminalNode FROM(int i) {
			return getToken(SqlBaseParser.FROM, i);
		}
		public List<TerminalNode> IN() { return getTokens(SqlBaseParser.IN); }
		public TerminalNode IN(int i) {
			return getToken(SqlBaseParser.IN, i);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ShowColumnsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowColumns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowColumns(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowColumns(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RepairTableContext extends StatementContext {
		public TerminalNode MSCK() { return getToken(SqlBaseParser.MSCK, 0); }
		public TerminalNode REPAIR() { return getToken(SqlBaseParser.REPAIR, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public RepairTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRepairTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRepairTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRepairTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetConfigurationContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public SetConfigurationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetConfiguration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetConfiguration(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetConfiguration(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class FailNativeCommandContext extends StatementContext {
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public UnsupportedHiveNativeCommandsContext unsupportedHiveNativeCommands() {
			return getRuleContext(UnsupportedHiveNativeCommandsContext.class,0);
		}
		public FailNativeCommandContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFailNativeCommand(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFailNativeCommand(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFailNativeCommand(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TruncateTableContext extends StatementContext {
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TruncateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTruncateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTruncateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTruncateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CacheTableContext extends StatementContext {
		public TerminalNode CACHE() { return getToken(SqlBaseParser.CACHE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode LAZY() { return getToken(SqlBaseParser.LAZY, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public CacheTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCacheTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCacheTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCacheTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RefreshResourceContext extends StatementContext {
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public RefreshResourceContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRefreshResource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRefreshResource(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRefreshResource(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AddTablePartitionContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public List<PartitionSpecLocationContext> partitionSpecLocation() {
			return getRuleContexts(PartitionSpecLocationContext.class);
		}
		public PartitionSpecLocationContext partitionSpecLocation(int i) {
			return getRuleContext(PartitionSpecLocationContext.class,i);
		}
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public List<PartitionSpecContext> partitionSpec() {
			return getRuleContexts(PartitionSpecContext.class);
		}
		public PartitionSpecContext partitionSpec(int i) {
			return getRuleContext(PartitionSpecContext.class,i);
		}
		public AddTablePartitionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAddTablePartition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAddTablePartition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAddTablePartition(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowCreateTableContext extends StatementContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public ShowCreateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowCreateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowCreateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowCreateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ClearCacheContext extends StatementContext {
		public TerminalNode CLEAR() { return getToken(SqlBaseParser.CLEAR, 0); }
		public TerminalNode CACHE() { return getToken(SqlBaseParser.CACHE, 0); }
		public ClearCacheContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterClearCache(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitClearCache(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitClearCache(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowDatabasesContext extends StatementContext {
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode DATABASES() { return getToken(SqlBaseParser.DATABASES, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ShowDatabasesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowDatabases(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowDatabases(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowDatabases(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UncacheTableContext extends StatementContext {
		public TerminalNode UNCACHE() { return getToken(SqlBaseParser.UNCACHE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public UncacheTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUncacheTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUncacheTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUncacheTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DescribeDatabaseContext extends StatementContext {
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public DescribeDatabaseContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeDatabase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeDatabase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeDatabase(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateTempViewUsingContext extends StatementContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TableProviderContext tableProvider() {
			return getRuleContext(TableProviderContext.class,0);
		}
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode GLOBAL() { return getToken(SqlBaseParser.GLOBAL, 0); }
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public CreateTempViewUsingContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTempViewUsing(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTempViewUsing(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateTempViewUsing(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RefreshTableContext extends StatementContext {
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public RefreshTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRefreshTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRefreshTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRefreshTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DropTableContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode PURGE() { return getToken(SqlBaseParser.PURGE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public DropTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RenameTablePartitionContext extends StatementContext {
		public PartitionSpecContext from;
		public PartitionSpecContext to;
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public List<PartitionSpecContext> partitionSpec() {
			return getRuleContexts(PartitionSpecContext.class);
		}
		public PartitionSpecContext partitionSpec(int i) {
			return getRuleContext(PartitionSpecContext.class,i);
		}
		public RenameTablePartitionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRenameTablePartition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRenameTablePartition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRenameTablePartition(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowTableContext extends StatementContext {
		public IdentifierContext db;
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ShowTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ExplainContext extends StatementContext {
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlBaseParser.FORMATTED, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode CODEGEN() { return getToken(SqlBaseParser.CODEGEN, 0); }
		public ExplainContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExplain(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExplain(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExplain(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LoadDataContext extends StatementContext {
		public Token path;
		public TerminalNode LOAD() { return getToken(SqlBaseParser.LOAD, 0); }
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode INPATH() { return getToken(SqlBaseParser.INPATH, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public LoadDataContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLoadData(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLoadData(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLoadData(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ChangeColumnContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode CHANGE() { return getToken(SqlBaseParser.CHANGE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColTypeContext colType() {
			return getRuleContext(ColTypeContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public ColPositionContext colPosition() {
			return getRuleContext(ColPositionContext.class,0);
		}
		public ChangeColumnContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterChangeColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitChangeColumn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitChangeColumn(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetDatabasePropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode DBPROPERTIES() { return getToken(SqlBaseParser.DBPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public SetDatabasePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetDatabaseProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetDatabaseProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetDatabaseProperties(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateTableContext extends StatementContext {
		public TablePropertyListContext options;
		public IdentifierListContext partitionColumnNames;
		public Token comment;
		public CreateTableHeaderContext createTableHeader() {
			return getRuleContext(CreateTableHeaderContext.class,0);
		}
		public TableProviderContext tableProvider() {
			return getRuleContext(TableProviderContext.class,0);
		}
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public TerminalNode PARTITIONED() { return getToken(SqlBaseParser.PARTITIONED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public BucketSpecContext bucketSpec() {
			return getRuleContext(BucketSpecContext.class,0);
		}
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public CreateTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UseContext extends StatementContext {
		public IdentifierContext db;
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public UseContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUse(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUse(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUse(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UnsetTablePropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode UNSET() { return getToken(SqlBaseParser.UNSET, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public UnsetTablePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnsetTableProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnsetTableProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnsetTableProperties(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DropDatabaseContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public DropDatabaseContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropDatabase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropDatabase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropDatabase(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DropTablePartitionsContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public List<PartitionSpecContext> partitionSpec() {
			return getRuleContexts(PartitionSpecContext.class);
		}
		public PartitionSpecContext partitionSpec(int i) {
			return getRuleContext(PartitionSpecContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode PURGE() { return getToken(SqlBaseParser.PURGE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public DropTablePartitionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropTablePartitions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropTablePartitions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropTablePartitions(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowTablesContext extends StatementContext {
		public IdentifierContext db;
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public ShowTablesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowTables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowTables(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowTables(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateDatabaseContext extends StatementContext {
		public Token comment;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode DBPROPERTIES() { return getToken(SqlBaseParser.DBPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public CreateDatabaseContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateDatabase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateDatabase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateDatabase(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateHiveTableContext extends StatementContext {
		public ColTypeListContext columns;
		public Token comment;
		public ColTypeListContext partitionColumns;
		public CreateTableHeaderContext createTableHeader() {
			return getRuleContext(CreateTableHeaderContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode PARTITIONED() { return getToken(SqlBaseParser.PARTITIONED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public BucketSpecContext bucketSpec() {
			return getRuleContext(BucketSpecContext.class,0);
		}
		public SkewSpecContext skewSpec() {
			return getRuleContext(SkewSpecContext.class,0);
		}
		public RowFormatContext rowFormat() {
			return getRuleContext(RowFormatContext.class,0);
		}
		public CreateFileFormatContext createFileFormat() {
			return getRuleContext(CreateFileFormatContext.class,0);
		}
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public List<ColTypeListContext> colTypeList() {
			return getRuleContexts(ColTypeListContext.class);
		}
		public ColTypeListContext colTypeList(int i) {
			return getRuleContext(ColTypeListContext.class,i);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public CreateHiveTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateHiveTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateHiveTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateHiveTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AlterViewQueryContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public AlterViewQueryContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAlterViewQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAlterViewQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAlterViewQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateFunctionContext extends StatementContext {
		public Token className;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<ResourceContext> resource() {
			return getRuleContexts(ResourceContext.class);
		}
		public ResourceContext resource(int i) {
			return getRuleContext(ResourceContext.class,i);
		}
		public CreateFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetTableSerDeContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public SetTableSerDeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetTableSerDe(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetTableSerDe(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetTableSerDe(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DropFunctionContext extends StatementContext {
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public DropFunctionContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDropFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDropFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDropFunction(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CreateTableLikeContext extends StatementContext {
		public TableIdentifierContext target;
		public TableIdentifierContext source;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public List<TableIdentifierContext> tableIdentifier() {
			return getRuleContexts(TableIdentifierContext.class);
		}
		public TableIdentifierContext tableIdentifier(int i) {
			return getRuleContext(TableIdentifierContext.class,i);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public CreateTableLikeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTableLike(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTableLike(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateTableLike(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetTablePropertiesContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public SetTablePropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetTableProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetTableProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetTableProperties(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class StatementDefaultContext extends StatementContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public StatementDefaultContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStatementDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStatementDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStatementDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AnalyzeContext extends StatementContext {
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode COMPUTE() { return getToken(SqlBaseParser.COMPUTE, 0); }
		public TerminalNode STATISTICS() { return getToken(SqlBaseParser.STATISTICS, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public AnalyzeContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAnalyze(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAnalyze(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAnalyze(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DescribeTableContext extends StatementContext {
		public Token option;
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public DescribeColNameContext describeColName() {
			return getRuleContext(DescribeColNameContext.class,0);
		}
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlBaseParser.FORMATTED, 0); }
		public DescribeTableContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetTableLocationContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public SetTableLocationContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetTableLocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetTableLocation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetTableLocation(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowTblPropertiesContext extends StatementContext {
		public TableIdentifierContext table;
		public TablePropertyKeyContext key;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TablePropertyKeyContext tablePropertyKey() {
			return getRuleContext(TablePropertyKeyContext.class,0);
		}
		public ShowTblPropertiesContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowTblProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowTblProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowTblProperties(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RecoverPartitionsContext extends StatementContext {
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode RECOVER() { return getToken(SqlBaseParser.RECOVER, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public RecoverPartitionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRecoverPartitions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRecoverPartitions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRecoverPartitions(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ShowFunctionsContext extends StatementContext {
		public Token pattern;
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ShowFunctionsContext(StatementContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterShowFunctions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitShowFunctions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitShowFunctions(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_statement);
		int _la;
		try {
			int _alt;
			setState(791);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,100,_ctx) ) {
			case 1:
				_localctx = new StatementDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(194);
				query();
				}
				break;
			case 2:
				_localctx = new UseContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(195);
				match(USE);
				setState(196);
				((UseContext)_localctx).db = identifier();
				}
				break;
			case 3:
				_localctx = new CreateDatabaseContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(197);
				match(CREATE);
				setState(198);
				match(DATABASE);
				setState(202);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
				case 1:
					{
					setState(199);
					match(IF);
					setState(200);
					match(NOT);
					setState(201);
					match(EXISTS);
					}
					break;
				}
				setState(204);
				identifier();
				setState(207);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(205);
					match(COMMENT);
					setState(206);
					((CreateDatabaseContext)_localctx).comment = match(STRING);
					}
				}

				setState(210);
				_la = _input.LA(1);
				if (_la==LOCATION) {
					{
					setState(209);
					locationSpec();
					}
				}

				setState(215);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(212);
					match(WITH);
					setState(213);
					match(DBPROPERTIES);
					setState(214);
					tablePropertyList();
					}
				}

				}
				break;
			case 4:
				_localctx = new SetDatabasePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(217);
				match(ALTER);
				setState(218);
				match(DATABASE);
				setState(219);
				identifier();
				setState(220);
				match(SET);
				setState(221);
				match(DBPROPERTIES);
				setState(222);
				tablePropertyList();
				}
				break;
			case 5:
				_localctx = new DropDatabaseContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(224);
				match(DROP);
				setState(225);
				match(DATABASE);
				setState(228);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
				case 1:
					{
					setState(226);
					match(IF);
					setState(227);
					match(EXISTS);
					}
					break;
				}
				setState(230);
				identifier();
				setState(232);
				_la = _input.LA(1);
				if (_la==CASCADE || _la==RESTRICT) {
					{
					setState(231);
					_la = _input.LA(1);
					if ( !(_la==CASCADE || _la==RESTRICT) ) {
					_errHandler.recoverInline(this);
					} else {
						consume();
					}
					}
				}

				}
				break;
			case 6:
				_localctx = new CreateTableContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(234);
				createTableHeader();
				setState(239);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(235);
					match(T__0);
					setState(236);
					colTypeList();
					setState(237);
					match(T__1);
					}
				}

				setState(241);
				tableProvider();
				setState(244);
				_la = _input.LA(1);
				if (_la==OPTIONS) {
					{
					setState(242);
					match(OPTIONS);
					setState(243);
					((CreateTableContext)_localctx).options = tablePropertyList();
					}
				}

				setState(249);
				_la = _input.LA(1);
				if (_la==PARTITIONED) {
					{
					setState(246);
					match(PARTITIONED);
					setState(247);
					match(BY);
					setState(248);
					((CreateTableContext)_localctx).partitionColumnNames = identifierList();
					}
				}

				setState(252);
				_la = _input.LA(1);
				if (_la==CLUSTERED) {
					{
					setState(251);
					bucketSpec();
					}
				}

				setState(255);
				_la = _input.LA(1);
				if (_la==LOCATION) {
					{
					setState(254);
					locationSpec();
					}
				}

				setState(259);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(257);
					match(COMMENT);
					setState(258);
					((CreateTableContext)_localctx).comment = match(STRING);
					}
				}

				setState(265);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << SELECT) | (1L << FROM) | (1L << AS))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (WITH - 72)) | (1L << (VALUES - 72)) | (1L << (TABLE - 72)) | (1L << (INSERT - 72)) | (1L << (MAP - 72)))) != 0) || _la==REDUCE) {
					{
					setState(262);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(261);
						match(AS);
						}
					}

					setState(264);
					query();
					}
				}

				}
				break;
			case 7:
				_localctx = new CreateHiveTableContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(267);
				createTableHeader();
				setState(272);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
				case 1:
					{
					setState(268);
					match(T__0);
					setState(269);
					((CreateHiveTableContext)_localctx).columns = colTypeList();
					setState(270);
					match(T__1);
					}
					break;
				}
				setState(276);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(274);
					match(COMMENT);
					setState(275);
					((CreateHiveTableContext)_localctx).comment = match(STRING);
					}
				}

				setState(284);
				_la = _input.LA(1);
				if (_la==PARTITIONED) {
					{
					setState(278);
					match(PARTITIONED);
					setState(279);
					match(BY);
					setState(280);
					match(T__0);
					setState(281);
					((CreateHiveTableContext)_localctx).partitionColumns = colTypeList();
					setState(282);
					match(T__1);
					}
				}

				setState(287);
				_la = _input.LA(1);
				if (_la==CLUSTERED) {
					{
					setState(286);
					bucketSpec();
					}
				}

				setState(290);
				_la = _input.LA(1);
				if (_la==SKEWED) {
					{
					setState(289);
					skewSpec();
					}
				}

				setState(293);
				_la = _input.LA(1);
				if (_la==ROW) {
					{
					setState(292);
					rowFormat();
					}
				}

				setState(296);
				_la = _input.LA(1);
				if (_la==STORED) {
					{
					setState(295);
					createFileFormat();
					}
				}

				setState(299);
				_la = _input.LA(1);
				if (_la==LOCATION) {
					{
					setState(298);
					locationSpec();
					}
				}

				setState(303);
				_la = _input.LA(1);
				if (_la==TBLPROPERTIES) {
					{
					setState(301);
					match(TBLPROPERTIES);
					setState(302);
					tablePropertyList();
					}
				}

				setState(309);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << SELECT) | (1L << FROM) | (1L << AS))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (WITH - 72)) | (1L << (VALUES - 72)) | (1L << (TABLE - 72)) | (1L << (INSERT - 72)) | (1L << (MAP - 72)))) != 0) || _la==REDUCE) {
					{
					setState(306);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(305);
						match(AS);
						}
					}

					setState(308);
					query();
					}
				}

				}
				break;
			case 8:
				_localctx = new CreateTableLikeContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(311);
				match(CREATE);
				setState(312);
				match(TABLE);
				setState(316);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,25,_ctx) ) {
				case 1:
					{
					setState(313);
					match(IF);
					setState(314);
					match(NOT);
					setState(315);
					match(EXISTS);
					}
					break;
				}
				setState(318);
				((CreateTableLikeContext)_localctx).target = tableIdentifier();
				setState(319);
				match(LIKE);
				setState(320);
				((CreateTableLikeContext)_localctx).source = tableIdentifier();
				setState(322);
				_la = _input.LA(1);
				if (_la==LOCATION) {
					{
					setState(321);
					locationSpec();
					}
				}

				}
				break;
			case 9:
				_localctx = new AnalyzeContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(324);
				match(ANALYZE);
				setState(325);
				match(TABLE);
				setState(326);
				tableIdentifier();
				setState(328);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(327);
					partitionSpec();
					}
				}

				setState(330);
				match(COMPUTE);
				setState(331);
				match(STATISTICS);
				setState(336);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,28,_ctx) ) {
				case 1:
					{
					setState(332);
					identifier();
					}
					break;
				case 2:
					{
					setState(333);
					match(FOR);
					setState(334);
					match(COLUMNS);
					setState(335);
					identifierSeq();
					}
					break;
				}
				}
				break;
			case 10:
				_localctx = new RenameTableContext(_localctx);
				enterOuterAlt(_localctx, 10);
				{
				setState(338);
				match(ALTER);
				setState(339);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(340);
				((RenameTableContext)_localctx).from = tableIdentifier();
				setState(341);
				match(RENAME);
				setState(342);
				match(TO);
				setState(343);
				((RenameTableContext)_localctx).to = tableIdentifier();
				}
				break;
			case 11:
				_localctx = new SetTablePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 11);
				{
				setState(345);
				match(ALTER);
				setState(346);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(347);
				tableIdentifier();
				setState(348);
				match(SET);
				setState(349);
				match(TBLPROPERTIES);
				setState(350);
				tablePropertyList();
				}
				break;
			case 12:
				_localctx = new UnsetTablePropertiesContext(_localctx);
				enterOuterAlt(_localctx, 12);
				{
				setState(352);
				match(ALTER);
				setState(353);
				_la = _input.LA(1);
				if ( !(_la==TABLE || _la==VIEW) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(354);
				tableIdentifier();
				setState(355);
				match(UNSET);
				setState(356);
				match(TBLPROPERTIES);
				setState(359);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(357);
					match(IF);
					setState(358);
					match(EXISTS);
					}
				}

				setState(361);
				tablePropertyList();
				}
				break;
			case 13:
				_localctx = new ChangeColumnContext(_localctx);
				enterOuterAlt(_localctx, 13);
				{
				setState(363);
				match(ALTER);
				setState(364);
				match(TABLE);
				setState(365);
				tableIdentifier();
				setState(367);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(366);
					partitionSpec();
					}
				}

				setState(369);
				match(CHANGE);
				setState(371);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
				case 1:
					{
					setState(370);
					match(COLUMN);
					}
					break;
				}
				setState(373);
				identifier();
				setState(374);
				colType();
				setState(376);
				_la = _input.LA(1);
				if (_la==FIRST || _la==AFTER) {
					{
					setState(375);
					colPosition();
					}
				}

				}
				break;
			case 14:
				_localctx = new SetTableSerDeContext(_localctx);
				enterOuterAlt(_localctx, 14);
				{
				setState(378);
				match(ALTER);
				setState(379);
				match(TABLE);
				setState(380);
				tableIdentifier();
				setState(382);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(381);
					partitionSpec();
					}
				}

				setState(384);
				match(SET);
				setState(385);
				match(SERDE);
				setState(386);
				match(STRING);
				setState(390);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(387);
					match(WITH);
					setState(388);
					match(SERDEPROPERTIES);
					setState(389);
					tablePropertyList();
					}
				}

				}
				break;
			case 15:
				_localctx = new SetTableSerDeContext(_localctx);
				enterOuterAlt(_localctx, 15);
				{
				setState(392);
				match(ALTER);
				setState(393);
				match(TABLE);
				setState(394);
				tableIdentifier();
				setState(396);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(395);
					partitionSpec();
					}
				}

				setState(398);
				match(SET);
				setState(399);
				match(SERDEPROPERTIES);
				setState(400);
				tablePropertyList();
				}
				break;
			case 16:
				_localctx = new AddTablePartitionContext(_localctx);
				enterOuterAlt(_localctx, 16);
				{
				setState(402);
				match(ALTER);
				setState(403);
				match(TABLE);
				setState(404);
				tableIdentifier();
				setState(405);
				match(ADD);
				setState(409);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(406);
					match(IF);
					setState(407);
					match(NOT);
					setState(408);
					match(EXISTS);
					}
				}

				setState(412); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(411);
					partitionSpecLocation();
					}
					}
					setState(414); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==PARTITION );
				}
				break;
			case 17:
				_localctx = new AddTablePartitionContext(_localctx);
				enterOuterAlt(_localctx, 17);
				{
				setState(416);
				match(ALTER);
				setState(417);
				match(VIEW);
				setState(418);
				tableIdentifier();
				setState(419);
				match(ADD);
				setState(423);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(420);
					match(IF);
					setState(421);
					match(NOT);
					setState(422);
					match(EXISTS);
					}
				}

				setState(426); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(425);
					partitionSpec();
					}
					}
					setState(428); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==PARTITION );
				}
				break;
			case 18:
				_localctx = new RenameTablePartitionContext(_localctx);
				enterOuterAlt(_localctx, 18);
				{
				setState(430);
				match(ALTER);
				setState(431);
				match(TABLE);
				setState(432);
				tableIdentifier();
				setState(433);
				((RenameTablePartitionContext)_localctx).from = partitionSpec();
				setState(434);
				match(RENAME);
				setState(435);
				match(TO);
				setState(436);
				((RenameTablePartitionContext)_localctx).to = partitionSpec();
				}
				break;
			case 19:
				_localctx = new DropTablePartitionsContext(_localctx);
				enterOuterAlt(_localctx, 19);
				{
				setState(438);
				match(ALTER);
				setState(439);
				match(TABLE);
				setState(440);
				tableIdentifier();
				setState(441);
				match(DROP);
				setState(444);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(442);
					match(IF);
					setState(443);
					match(EXISTS);
					}
				}

				setState(446);
				partitionSpec();
				setState(451);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(447);
					match(T__2);
					setState(448);
					partitionSpec();
					}
					}
					setState(453);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(455);
				_la = _input.LA(1);
				if (_la==PURGE) {
					{
					setState(454);
					match(PURGE);
					}
				}

				}
				break;
			case 20:
				_localctx = new DropTablePartitionsContext(_localctx);
				enterOuterAlt(_localctx, 20);
				{
				setState(457);
				match(ALTER);
				setState(458);
				match(VIEW);
				setState(459);
				tableIdentifier();
				setState(460);
				match(DROP);
				setState(463);
				_la = _input.LA(1);
				if (_la==IF) {
					{
					setState(461);
					match(IF);
					setState(462);
					match(EXISTS);
					}
				}

				setState(465);
				partitionSpec();
				setState(470);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(466);
					match(T__2);
					setState(467);
					partitionSpec();
					}
					}
					setState(472);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 21:
				_localctx = new SetTableLocationContext(_localctx);
				enterOuterAlt(_localctx, 21);
				{
				setState(473);
				match(ALTER);
				setState(474);
				match(TABLE);
				setState(475);
				tableIdentifier();
				setState(477);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(476);
					partitionSpec();
					}
				}

				setState(479);
				match(SET);
				setState(480);
				locationSpec();
				}
				break;
			case 22:
				_localctx = new RecoverPartitionsContext(_localctx);
				enterOuterAlt(_localctx, 22);
				{
				setState(482);
				match(ALTER);
				setState(483);
				match(TABLE);
				setState(484);
				tableIdentifier();
				setState(485);
				match(RECOVER);
				setState(486);
				match(PARTITIONS);
				}
				break;
			case 23:
				_localctx = new DropTableContext(_localctx);
				enterOuterAlt(_localctx, 23);
				{
				setState(488);
				match(DROP);
				setState(489);
				match(TABLE);
				setState(492);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,46,_ctx) ) {
				case 1:
					{
					setState(490);
					match(IF);
					setState(491);
					match(EXISTS);
					}
					break;
				}
				setState(494);
				tableIdentifier();
				setState(496);
				_la = _input.LA(1);
				if (_la==PURGE) {
					{
					setState(495);
					match(PURGE);
					}
				}

				}
				break;
			case 24:
				_localctx = new DropTableContext(_localctx);
				enterOuterAlt(_localctx, 24);
				{
				setState(498);
				match(DROP);
				setState(499);
				match(VIEW);
				setState(502);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,48,_ctx) ) {
				case 1:
					{
					setState(500);
					match(IF);
					setState(501);
					match(EXISTS);
					}
					break;
				}
				setState(504);
				tableIdentifier();
				}
				break;
			case 25:
				_localctx = new CreateViewContext(_localctx);
				enterOuterAlt(_localctx, 25);
				{
				setState(505);
				match(CREATE);
				setState(508);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(506);
					match(OR);
					setState(507);
					match(REPLACE);
					}
				}

				setState(514);
				_la = _input.LA(1);
				if (_la==GLOBAL || _la==TEMPORARY) {
					{
					setState(511);
					_la = _input.LA(1);
					if (_la==GLOBAL) {
						{
						setState(510);
						match(GLOBAL);
						}
					}

					setState(513);
					match(TEMPORARY);
					}
				}

				setState(516);
				match(VIEW);
				setState(520);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,52,_ctx) ) {
				case 1:
					{
					setState(517);
					match(IF);
					setState(518);
					match(NOT);
					setState(519);
					match(EXISTS);
					}
					break;
				}
				setState(522);
				tableIdentifier();
				setState(524);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(523);
					identifierCommentList();
					}
				}

				setState(528);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(526);
					match(COMMENT);
					setState(527);
					match(STRING);
					}
				}

				setState(533);
				_la = _input.LA(1);
				if (_la==PARTITIONED) {
					{
					setState(530);
					match(PARTITIONED);
					setState(531);
					match(ON);
					setState(532);
					identifierList();
					}
				}

				setState(537);
				_la = _input.LA(1);
				if (_la==TBLPROPERTIES) {
					{
					setState(535);
					match(TBLPROPERTIES);
					setState(536);
					tablePropertyList();
					}
				}

				setState(539);
				match(AS);
				setState(540);
				query();
				}
				break;
			case 26:
				_localctx = new CreateTempViewUsingContext(_localctx);
				enterOuterAlt(_localctx, 26);
				{
				setState(542);
				match(CREATE);
				setState(545);
				_la = _input.LA(1);
				if (_la==OR) {
					{
					setState(543);
					match(OR);
					setState(544);
					match(REPLACE);
					}
				}

				setState(548);
				_la = _input.LA(1);
				if (_la==GLOBAL) {
					{
					setState(547);
					match(GLOBAL);
					}
				}

				setState(550);
				match(TEMPORARY);
				setState(551);
				match(VIEW);
				setState(552);
				tableIdentifier();
				setState(557);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(553);
					match(T__0);
					setState(554);
					colTypeList();
					setState(555);
					match(T__1);
					}
				}

				setState(559);
				tableProvider();
				setState(562);
				_la = _input.LA(1);
				if (_la==OPTIONS) {
					{
					setState(560);
					match(OPTIONS);
					setState(561);
					tablePropertyList();
					}
				}

				}
				break;
			case 27:
				_localctx = new AlterViewQueryContext(_localctx);
				enterOuterAlt(_localctx, 27);
				{
				setState(564);
				match(ALTER);
				setState(565);
				match(VIEW);
				setState(566);
				tableIdentifier();
				setState(568);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(567);
					match(AS);
					}
				}

				setState(570);
				query();
				}
				break;
			case 28:
				_localctx = new CreateFunctionContext(_localctx);
				enterOuterAlt(_localctx, 28);
				{
				setState(572);
				match(CREATE);
				setState(574);
				_la = _input.LA(1);
				if (_la==TEMPORARY) {
					{
					setState(573);
					match(TEMPORARY);
					}
				}

				setState(576);
				match(FUNCTION);
				setState(577);
				qualifiedName();
				setState(578);
				match(AS);
				setState(579);
				((CreateFunctionContext)_localctx).className = match(STRING);
				setState(589);
				_la = _input.LA(1);
				if (_la==USING) {
					{
					setState(580);
					match(USING);
					setState(581);
					resource();
					setState(586);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(582);
						match(T__2);
						setState(583);
						resource();
						}
						}
						setState(588);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				}
				break;
			case 29:
				_localctx = new DropFunctionContext(_localctx);
				enterOuterAlt(_localctx, 29);
				{
				setState(591);
				match(DROP);
				setState(593);
				_la = _input.LA(1);
				if (_la==TEMPORARY) {
					{
					setState(592);
					match(TEMPORARY);
					}
				}

				setState(595);
				match(FUNCTION);
				setState(598);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,66,_ctx) ) {
				case 1:
					{
					setState(596);
					match(IF);
					setState(597);
					match(EXISTS);
					}
					break;
				}
				setState(600);
				qualifiedName();
				}
				break;
			case 30:
				_localctx = new ExplainContext(_localctx);
				enterOuterAlt(_localctx, 30);
				{
				setState(601);
				match(EXPLAIN);
				setState(603);
				_la = _input.LA(1);
				if (_la==LOGICAL || _la==CODEGEN || _la==EXTENDED || _la==FORMATTED) {
					{
					setState(602);
					_la = _input.LA(1);
					if ( !(_la==LOGICAL || _la==CODEGEN || _la==EXTENDED || _la==FORMATTED) ) {
					_errHandler.recoverInline(this);
					} else {
						consume();
					}
					}
				}

				setState(605);
				statement();
				}
				break;
			case 31:
				_localctx = new ShowTablesContext(_localctx);
				enterOuterAlt(_localctx, 31);
				{
				setState(606);
				match(SHOW);
				setState(607);
				match(TABLES);
				setState(610);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(608);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					} else {
						consume();
					}
					setState(609);
					((ShowTablesContext)_localctx).db = identifier();
					}
				}

				setState(616);
				_la = _input.LA(1);
				if (_la==LIKE || _la==STRING) {
					{
					setState(613);
					_la = _input.LA(1);
					if (_la==LIKE) {
						{
						setState(612);
						match(LIKE);
						}
					}

					setState(615);
					((ShowTablesContext)_localctx).pattern = match(STRING);
					}
				}

				}
				break;
			case 32:
				_localctx = new ShowTableContext(_localctx);
				enterOuterAlt(_localctx, 32);
				{
				setState(618);
				match(SHOW);
				setState(619);
				match(TABLE);
				setState(620);
				match(EXTENDED);
				setState(623);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(621);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					} else {
						consume();
					}
					setState(622);
					((ShowTableContext)_localctx).db = identifier();
					}
				}

				setState(625);
				match(LIKE);
				setState(626);
				((ShowTableContext)_localctx).pattern = match(STRING);
				setState(628);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(627);
					partitionSpec();
					}
				}

				}
				break;
			case 33:
				_localctx = new ShowDatabasesContext(_localctx);
				enterOuterAlt(_localctx, 33);
				{
				setState(630);
				match(SHOW);
				setState(631);
				match(DATABASES);
				setState(634);
				_la = _input.LA(1);
				if (_la==LIKE) {
					{
					setState(632);
					match(LIKE);
					setState(633);
					((ShowDatabasesContext)_localctx).pattern = match(STRING);
					}
				}

				}
				break;
			case 34:
				_localctx = new ShowTblPropertiesContext(_localctx);
				enterOuterAlt(_localctx, 34);
				{
				setState(636);
				match(SHOW);
				setState(637);
				match(TBLPROPERTIES);
				setState(638);
				((ShowTblPropertiesContext)_localctx).table = tableIdentifier();
				setState(643);
				_la = _input.LA(1);
				if (_la==T__0) {
					{
					setState(639);
					match(T__0);
					setState(640);
					((ShowTblPropertiesContext)_localctx).key = tablePropertyKey();
					setState(641);
					match(T__1);
					}
				}

				}
				break;
			case 35:
				_localctx = new ShowColumnsContext(_localctx);
				enterOuterAlt(_localctx, 35);
				{
				setState(645);
				match(SHOW);
				setState(646);
				match(COLUMNS);
				setState(647);
				_la = _input.LA(1);
				if ( !(_la==FROM || _la==IN) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(648);
				tableIdentifier();
				setState(651);
				_la = _input.LA(1);
				if (_la==FROM || _la==IN) {
					{
					setState(649);
					_la = _input.LA(1);
					if ( !(_la==FROM || _la==IN) ) {
					_errHandler.recoverInline(this);
					} else {
						consume();
					}
					setState(650);
					((ShowColumnsContext)_localctx).db = identifier();
					}
				}

				}
				break;
			case 36:
				_localctx = new ShowPartitionsContext(_localctx);
				enterOuterAlt(_localctx, 36);
				{
				setState(653);
				match(SHOW);
				setState(654);
				match(PARTITIONS);
				setState(655);
				tableIdentifier();
				setState(657);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(656);
					partitionSpec();
					}
				}

				}
				break;
			case 37:
				_localctx = new ShowFunctionsContext(_localctx);
				enterOuterAlt(_localctx, 37);
				{
				setState(659);
				match(SHOW);
				setState(661);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,77,_ctx) ) {
				case 1:
					{
					setState(660);
					identifier();
					}
					break;
				}
				setState(663);
				match(FUNCTIONS);
				setState(671);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << LATERAL) | (1L << WINDOW) | (1L << OVER) | (1L << PARTITION) | (1L << RANGE) | (1L << ROWS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (FIRST - 64)) | (1L << (AFTER - 64)) | (1L << (LAST - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (SETMINUS - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IF - 64)))) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & ((1L << (DIV - 130)) | (1L << (PERCENTLIT - 130)) | (1L << (BUCKET - 130)) | (1L << (OUT - 130)) | (1L << (OF - 130)) | (1L << (SORT - 130)) | (1L << (CLUSTER - 130)) | (1L << (DISTRIBUTE - 130)) | (1L << (OVERWRITE - 130)) | (1L << (TRANSFORM - 130)) | (1L << (REDUCE - 130)) | (1L << (USING - 130)) | (1L << (SERDE - 130)) | (1L << (SERDEPROPERTIES - 130)) | (1L << (RECORDREADER - 130)) | (1L << (RECORDWRITER - 130)) | (1L << (DELIMITED - 130)) | (1L << (FIELDS - 130)) | (1L << (TERMINATED - 130)) | (1L << (COLLECTION - 130)) | (1L << (ITEMS - 130)) | (1L << (KEYS - 130)) | (1L << (ESCAPED - 130)) | (1L << (LINES - 130)) | (1L << (SEPARATED - 130)) | (1L << (FUNCTION - 130)) | (1L << (EXTENDED - 130)) | (1L << (REFRESH - 130)) | (1L << (CLEAR - 130)) | (1L << (CACHE - 130)) | (1L << (UNCACHE - 130)) | (1L << (LAZY - 130)) | (1L << (FORMATTED - 130)) | (1L << (GLOBAL - 130)) | (1L << (TEMPORARY - 130)) | (1L << (OPTIONS - 130)) | (1L << (UNSET - 130)) | (1L << (TBLPROPERTIES - 130)) | (1L << (DBPROPERTIES - 130)) | (1L << (BUCKETS - 130)) | (1L << (SKEWED - 130)) | (1L << (STORED - 130)) | (1L << (DIRECTORIES - 130)) | (1L << (LOCATION - 130)) | (1L << (EXCHANGE - 130)) | (1L << (ARCHIVE - 130)) | (1L << (UNARCHIVE - 130)) | (1L << (FILEFORMAT - 130)) | (1L << (TOUCH - 130)) | (1L << (COMPACT - 130)) | (1L << (CONCATENATE - 130)) | (1L << (CHANGE - 130)) | (1L << (CASCADE - 130)) | (1L << (RESTRICT - 130)) | (1L << (CLUSTERED - 130)) | (1L << (SORTED - 130)) | (1L << (PURGE - 130)) | (1L << (INPUTFORMAT - 130)) | (1L << (OUTPUTFORMAT - 130)) | (1L << (DATABASE - 130)))) != 0) || ((((_la - 194)) & ~0x3f) == 0 && ((1L << (_la - 194)) & ((1L << (DATABASES - 194)) | (1L << (DFS - 194)) | (1L << (TRUNCATE - 194)) | (1L << (ANALYZE - 194)) | (1L << (COMPUTE - 194)) | (1L << (LIST - 194)) | (1L << (STATISTICS - 194)) | (1L << (PARTITIONED - 194)) | (1L << (EXTERNAL - 194)) | (1L << (DEFINED - 194)) | (1L << (REVOKE - 194)) | (1L << (GRANT - 194)) | (1L << (LOCK - 194)) | (1L << (UNLOCK - 194)) | (1L << (MSCK - 194)) | (1L << (REPAIR - 194)) | (1L << (RECOVER - 194)) | (1L << (EXPORT - 194)) | (1L << (IMPORT - 194)) | (1L << (LOAD - 194)) | (1L << (ROLE - 194)) | (1L << (ROLES - 194)) | (1L << (COMPACTIONS - 194)) | (1L << (PRINCIPALS - 194)) | (1L << (TRANSACTIONS - 194)) | (1L << (INDEX - 194)) | (1L << (INDEXES - 194)) | (1L << (LOCKS - 194)) | (1L << (OPTION - 194)) | (1L << (ANTI - 194)) | (1L << (LOCAL - 194)) | (1L << (INPATH - 194)) | (1L << (CURRENT_DATE - 194)) | (1L << (CURRENT_TIMESTAMP - 194)) | (1L << (STRING - 194)) | (1L << (IDENTIFIER - 194)) | (1L << (BACKQUOTED_IDENTIFIER - 194)))) != 0)) {
					{
					setState(665);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,78,_ctx) ) {
					case 1:
						{
						setState(664);
						match(LIKE);
						}
						break;
					}
					setState(669);
					switch (_input.LA(1)) {
					case SELECT:
					case FROM:
					case ADD:
					case AS:
					case ALL:
					case DISTINCT:
					case WHERE:
					case GROUP:
					case BY:
					case GROUPING:
					case SETS:
					case CUBE:
					case ROLLUP:
					case ORDER:
					case HAVING:
					case LIMIT:
					case AT:
					case OR:
					case AND:
					case IN:
					case NOT:
					case NO:
					case EXISTS:
					case BETWEEN:
					case LIKE:
					case RLIKE:
					case IS:
					case NULL:
					case TRUE:
					case FALSE:
					case NULLS:
					case ASC:
					case DESC:
					case FOR:
					case INTERVAL:
					case CASE:
					case WHEN:
					case THEN:
					case ELSE:
					case END:
					case JOIN:
					case CROSS:
					case OUTER:
					case INNER:
					case LEFT:
					case SEMI:
					case RIGHT:
					case FULL:
					case NATURAL:
					case ON:
					case LATERAL:
					case WINDOW:
					case OVER:
					case PARTITION:
					case RANGE:
					case ROWS:
					case UNBOUNDED:
					case PRECEDING:
					case FOLLOWING:
					case CURRENT:
					case FIRST:
					case AFTER:
					case LAST:
					case ROW:
					case WITH:
					case VALUES:
					case CREATE:
					case TABLE:
					case VIEW:
					case REPLACE:
					case INSERT:
					case DELETE:
					case INTO:
					case DESCRIBE:
					case EXPLAIN:
					case FORMAT:
					case LOGICAL:
					case CODEGEN:
					case CAST:
					case SHOW:
					case TABLES:
					case COLUMNS:
					case COLUMN:
					case USE:
					case PARTITIONS:
					case FUNCTIONS:
					case DROP:
					case UNION:
					case EXCEPT:
					case SETMINUS:
					case INTERSECT:
					case TO:
					case TABLESAMPLE:
					case STRATIFY:
					case ALTER:
					case RENAME:
					case ARRAY:
					case MAP:
					case STRUCT:
					case COMMENT:
					case SET:
					case RESET:
					case DATA:
					case START:
					case TRANSACTION:
					case COMMIT:
					case ROLLBACK:
					case MACRO:
					case IF:
					case DIV:
					case PERCENTLIT:
					case BUCKET:
					case OUT:
					case OF:
					case SORT:
					case CLUSTER:
					case DISTRIBUTE:
					case OVERWRITE:
					case TRANSFORM:
					case REDUCE:
					case USING:
					case SERDE:
					case SERDEPROPERTIES:
					case RECORDREADER:
					case RECORDWRITER:
					case DELIMITED:
					case FIELDS:
					case TERMINATED:
					case COLLECTION:
					case ITEMS:
					case KEYS:
					case ESCAPED:
					case LINES:
					case SEPARATED:
					case FUNCTION:
					case EXTENDED:
					case REFRESH:
					case CLEAR:
					case CACHE:
					case UNCACHE:
					case LAZY:
					case FORMATTED:
					case GLOBAL:
					case TEMPORARY:
					case OPTIONS:
					case UNSET:
					case TBLPROPERTIES:
					case DBPROPERTIES:
					case BUCKETS:
					case SKEWED:
					case STORED:
					case DIRECTORIES:
					case LOCATION:
					case EXCHANGE:
					case ARCHIVE:
					case UNARCHIVE:
					case FILEFORMAT:
					case TOUCH:
					case COMPACT:
					case CONCATENATE:
					case CHANGE:
					case CASCADE:
					case RESTRICT:
					case CLUSTERED:
					case SORTED:
					case PURGE:
					case INPUTFORMAT:
					case OUTPUTFORMAT:
					case DATABASE:
					case DATABASES:
					case DFS:
					case TRUNCATE:
					case ANALYZE:
					case COMPUTE:
					case LIST:
					case STATISTICS:
					case PARTITIONED:
					case EXTERNAL:
					case DEFINED:
					case REVOKE:
					case GRANT:
					case LOCK:
					case UNLOCK:
					case MSCK:
					case REPAIR:
					case RECOVER:
					case EXPORT:
					case IMPORT:
					case LOAD:
					case ROLE:
					case ROLES:
					case COMPACTIONS:
					case PRINCIPALS:
					case TRANSACTIONS:
					case INDEX:
					case INDEXES:
					case LOCKS:
					case OPTION:
					case ANTI:
					case LOCAL:
					case INPATH:
					case CURRENT_DATE:
					case CURRENT_TIMESTAMP:
					case IDENTIFIER:
					case BACKQUOTED_IDENTIFIER:
						{
						setState(667);
						qualifiedName();
						}
						break;
					case STRING:
						{
						setState(668);
						((ShowFunctionsContext)_localctx).pattern = match(STRING);
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					}
				}

				}
				break;
			case 38:
				_localctx = new ShowCreateTableContext(_localctx);
				enterOuterAlt(_localctx, 38);
				{
				setState(673);
				match(SHOW);
				setState(674);
				match(CREATE);
				setState(675);
				match(TABLE);
				setState(676);
				tableIdentifier();
				}
				break;
			case 39:
				_localctx = new DescribeFunctionContext(_localctx);
				enterOuterAlt(_localctx, 39);
				{
				setState(677);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(678);
				match(FUNCTION);
				setState(680);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,81,_ctx) ) {
				case 1:
					{
					setState(679);
					match(EXTENDED);
					}
					break;
				}
				setState(682);
				describeFuncName();
				}
				break;
			case 40:
				_localctx = new DescribeDatabaseContext(_localctx);
				enterOuterAlt(_localctx, 40);
				{
				setState(683);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(684);
				match(DATABASE);
				setState(686);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,82,_ctx) ) {
				case 1:
					{
					setState(685);
					match(EXTENDED);
					}
					break;
				}
				setState(688);
				identifier();
				}
				break;
			case 41:
				_localctx = new DescribeTableContext(_localctx);
				enterOuterAlt(_localctx, 41);
				{
				setState(689);
				_la = _input.LA(1);
				if ( !(_la==DESC || _la==DESCRIBE) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(691);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,83,_ctx) ) {
				case 1:
					{
					setState(690);
					match(TABLE);
					}
					break;
				}
				setState(694);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,84,_ctx) ) {
				case 1:
					{
					setState(693);
					((DescribeTableContext)_localctx).option = _input.LT(1);
					_la = _input.LA(1);
					if ( !(_la==EXTENDED || _la==FORMATTED) ) {
						((DescribeTableContext)_localctx).option = (Token)_errHandler.recoverInline(this);
					} else {
						consume();
					}
					}
					break;
				}
				setState(696);
				tableIdentifier();
				setState(698);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,85,_ctx) ) {
				case 1:
					{
					setState(697);
					partitionSpec();
					}
					break;
				}
				setState(701);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << LATERAL) | (1L << WINDOW) | (1L << OVER) | (1L << PARTITION) | (1L << RANGE) | (1L << ROWS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (FIRST - 64)) | (1L << (AFTER - 64)) | (1L << (LAST - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (SETMINUS - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IF - 64)))) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & ((1L << (DIV - 130)) | (1L << (PERCENTLIT - 130)) | (1L << (BUCKET - 130)) | (1L << (OUT - 130)) | (1L << (OF - 130)) | (1L << (SORT - 130)) | (1L << (CLUSTER - 130)) | (1L << (DISTRIBUTE - 130)) | (1L << (OVERWRITE - 130)) | (1L << (TRANSFORM - 130)) | (1L << (REDUCE - 130)) | (1L << (USING - 130)) | (1L << (SERDE - 130)) | (1L << (SERDEPROPERTIES - 130)) | (1L << (RECORDREADER - 130)) | (1L << (RECORDWRITER - 130)) | (1L << (DELIMITED - 130)) | (1L << (FIELDS - 130)) | (1L << (TERMINATED - 130)) | (1L << (COLLECTION - 130)) | (1L << (ITEMS - 130)) | (1L << (KEYS - 130)) | (1L << (ESCAPED - 130)) | (1L << (LINES - 130)) | (1L << (SEPARATED - 130)) | (1L << (FUNCTION - 130)) | (1L << (EXTENDED - 130)) | (1L << (REFRESH - 130)) | (1L << (CLEAR - 130)) | (1L << (CACHE - 130)) | (1L << (UNCACHE - 130)) | (1L << (LAZY - 130)) | (1L << (FORMATTED - 130)) | (1L << (GLOBAL - 130)) | (1L << (TEMPORARY - 130)) | (1L << (OPTIONS - 130)) | (1L << (UNSET - 130)) | (1L << (TBLPROPERTIES - 130)) | (1L << (DBPROPERTIES - 130)) | (1L << (BUCKETS - 130)) | (1L << (SKEWED - 130)) | (1L << (STORED - 130)) | (1L << (DIRECTORIES - 130)) | (1L << (LOCATION - 130)) | (1L << (EXCHANGE - 130)) | (1L << (ARCHIVE - 130)) | (1L << (UNARCHIVE - 130)) | (1L << (FILEFORMAT - 130)) | (1L << (TOUCH - 130)) | (1L << (COMPACT - 130)) | (1L << (CONCATENATE - 130)) | (1L << (CHANGE - 130)) | (1L << (CASCADE - 130)) | (1L << (RESTRICT - 130)) | (1L << (CLUSTERED - 130)) | (1L << (SORTED - 130)) | (1L << (PURGE - 130)) | (1L << (INPUTFORMAT - 130)) | (1L << (OUTPUTFORMAT - 130)) | (1L << (DATABASE - 130)))) != 0) || ((((_la - 194)) & ~0x3f) == 0 && ((1L << (_la - 194)) & ((1L << (DATABASES - 194)) | (1L << (DFS - 194)) | (1L << (TRUNCATE - 194)) | (1L << (ANALYZE - 194)) | (1L << (COMPUTE - 194)) | (1L << (LIST - 194)) | (1L << (STATISTICS - 194)) | (1L << (PARTITIONED - 194)) | (1L << (EXTERNAL - 194)) | (1L << (DEFINED - 194)) | (1L << (REVOKE - 194)) | (1L << (GRANT - 194)) | (1L << (LOCK - 194)) | (1L << (UNLOCK - 194)) | (1L << (MSCK - 194)) | (1L << (REPAIR - 194)) | (1L << (RECOVER - 194)) | (1L << (EXPORT - 194)) | (1L << (IMPORT - 194)) | (1L << (LOAD - 194)) | (1L << (ROLE - 194)) | (1L << (ROLES - 194)) | (1L << (COMPACTIONS - 194)) | (1L << (PRINCIPALS - 194)) | (1L << (TRANSACTIONS - 194)) | (1L << (INDEX - 194)) | (1L << (INDEXES - 194)) | (1L << (LOCKS - 194)) | (1L << (OPTION - 194)) | (1L << (ANTI - 194)) | (1L << (LOCAL - 194)) | (1L << (INPATH - 194)) | (1L << (CURRENT_DATE - 194)) | (1L << (CURRENT_TIMESTAMP - 194)) | (1L << (IDENTIFIER - 194)) | (1L << (BACKQUOTED_IDENTIFIER - 194)))) != 0)) {
					{
					setState(700);
					describeColName();
					}
				}

				}
				break;
			case 42:
				_localctx = new RefreshTableContext(_localctx);
				enterOuterAlt(_localctx, 42);
				{
				setState(703);
				match(REFRESH);
				setState(704);
				match(TABLE);
				setState(705);
				tableIdentifier();
				}
				break;
			case 43:
				_localctx = new RefreshResourceContext(_localctx);
				enterOuterAlt(_localctx, 43);
				{
				setState(706);
				match(REFRESH);
				setState(710);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,87,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(707);
						matchWildcard();
						}
						} 
					}
					setState(712);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,87,_ctx);
				}
				}
				break;
			case 44:
				_localctx = new CacheTableContext(_localctx);
				enterOuterAlt(_localctx, 44);
				{
				setState(713);
				match(CACHE);
				setState(715);
				_la = _input.LA(1);
				if (_la==LAZY) {
					{
					setState(714);
					match(LAZY);
					}
				}

				setState(717);
				match(TABLE);
				setState(718);
				tableIdentifier();
				setState(723);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << SELECT) | (1L << FROM) | (1L << AS))) != 0) || ((((_la - 72)) & ~0x3f) == 0 && ((1L << (_la - 72)) & ((1L << (WITH - 72)) | (1L << (VALUES - 72)) | (1L << (TABLE - 72)) | (1L << (INSERT - 72)) | (1L << (MAP - 72)))) != 0) || _la==REDUCE) {
					{
					setState(720);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(719);
						match(AS);
						}
					}

					setState(722);
					query();
					}
				}

				}
				break;
			case 45:
				_localctx = new UncacheTableContext(_localctx);
				enterOuterAlt(_localctx, 45);
				{
				setState(725);
				match(UNCACHE);
				setState(726);
				match(TABLE);
				setState(729);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,91,_ctx) ) {
				case 1:
					{
					setState(727);
					match(IF);
					setState(728);
					match(EXISTS);
					}
					break;
				}
				setState(731);
				tableIdentifier();
				}
				break;
			case 46:
				_localctx = new ClearCacheContext(_localctx);
				enterOuterAlt(_localctx, 46);
				{
				setState(732);
				match(CLEAR);
				setState(733);
				match(CACHE);
				}
				break;
			case 47:
				_localctx = new LoadDataContext(_localctx);
				enterOuterAlt(_localctx, 47);
				{
				setState(734);
				match(LOAD);
				setState(735);
				match(DATA);
				setState(737);
				_la = _input.LA(1);
				if (_la==LOCAL) {
					{
					setState(736);
					match(LOCAL);
					}
				}

				setState(739);
				match(INPATH);
				setState(740);
				((LoadDataContext)_localctx).path = match(STRING);
				setState(742);
				_la = _input.LA(1);
				if (_la==OVERWRITE) {
					{
					setState(741);
					match(OVERWRITE);
					}
				}

				setState(744);
				match(INTO);
				setState(745);
				match(TABLE);
				setState(746);
				tableIdentifier();
				setState(748);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(747);
					partitionSpec();
					}
				}

				}
				break;
			case 48:
				_localctx = new TruncateTableContext(_localctx);
				enterOuterAlt(_localctx, 48);
				{
				setState(750);
				match(TRUNCATE);
				setState(751);
				match(TABLE);
				setState(752);
				tableIdentifier();
				setState(754);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(753);
					partitionSpec();
					}
				}

				}
				break;
			case 49:
				_localctx = new RepairTableContext(_localctx);
				enterOuterAlt(_localctx, 49);
				{
				setState(756);
				match(MSCK);
				setState(757);
				match(REPAIR);
				setState(758);
				match(TABLE);
				setState(759);
				tableIdentifier();
				}
				break;
			case 50:
				_localctx = new ManageResourceContext(_localctx);
				enterOuterAlt(_localctx, 50);
				{
				setState(760);
				((ManageResourceContext)_localctx).op = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ADD || _la==LIST) ) {
					((ManageResourceContext)_localctx).op = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(761);
				identifier();
				setState(765);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,96,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(762);
						matchWildcard();
						}
						} 
					}
					setState(767);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,96,_ctx);
				}
				}
				break;
			case 51:
				_localctx = new FailNativeCommandContext(_localctx);
				enterOuterAlt(_localctx, 51);
				{
				setState(768);
				match(SET);
				setState(769);
				match(ROLE);
				setState(773);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,97,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(770);
						matchWildcard();
						}
						} 
					}
					setState(775);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,97,_ctx);
				}
				}
				break;
			case 52:
				_localctx = new SetConfigurationContext(_localctx);
				enterOuterAlt(_localctx, 52);
				{
				setState(776);
				match(SET);
				setState(780);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,98,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(777);
						matchWildcard();
						}
						} 
					}
					setState(782);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,98,_ctx);
				}
				}
				break;
			case 53:
				_localctx = new ResetConfigurationContext(_localctx);
				enterOuterAlt(_localctx, 53);
				{
				setState(783);
				match(RESET);
				}
				break;
			case 54:
				_localctx = new FailNativeCommandContext(_localctx);
				enterOuterAlt(_localctx, 54);
				{
				setState(784);
				unsupportedHiveNativeCommands();
				setState(788);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,99,_ctx);
				while ( _alt!=1 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1+1 ) {
						{
						{
						setState(785);
						matchWildcard();
						}
						} 
					}
					setState(790);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,99,_ctx);
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UnsupportedHiveNativeCommandsContext extends ParserRuleContext {
		public Token kw1;
		public Token kw2;
		public Token kw3;
		public Token kw4;
		public Token kw5;
		public Token kw6;
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode PRINCIPALS() { return getToken(SqlBaseParser.PRINCIPALS, 0); }
		public TerminalNode ROLES() { return getToken(SqlBaseParser.ROLES, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode EXPORT() { return getToken(SqlBaseParser.EXPORT, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TerminalNode IMPORT() { return getToken(SqlBaseParser.IMPORT, 0); }
		public TerminalNode COMPACTIONS() { return getToken(SqlBaseParser.COMPACTIONS, 0); }
		public TerminalNode TRANSACTIONS() { return getToken(SqlBaseParser.TRANSACTIONS, 0); }
		public TerminalNode INDEXES() { return getToken(SqlBaseParser.INDEXES, 0); }
		public TerminalNode LOCKS() { return getToken(SqlBaseParser.LOCKS, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode LOCK() { return getToken(SqlBaseParser.LOCK, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public TerminalNode UNLOCK() { return getToken(SqlBaseParser.UNLOCK, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode MACRO() { return getToken(SqlBaseParser.MACRO, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode CLUSTERED() { return getToken(SqlBaseParser.CLUSTERED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public TerminalNode SORTED() { return getToken(SqlBaseParser.SORTED, 0); }
		public TerminalNode SKEWED() { return getToken(SqlBaseParser.SKEWED, 0); }
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlBaseParser.DIRECTORIES, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode LOCATION() { return getToken(SqlBaseParser.LOCATION, 0); }
		public TerminalNode EXCHANGE() { return getToken(SqlBaseParser.EXCHANGE, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode ARCHIVE() { return getToken(SqlBaseParser.ARCHIVE, 0); }
		public TerminalNode UNARCHIVE() { return getToken(SqlBaseParser.UNARCHIVE, 0); }
		public TerminalNode TOUCH() { return getToken(SqlBaseParser.TOUCH, 0); }
		public TerminalNode COMPACT() { return getToken(SqlBaseParser.COMPACT, 0); }
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode CONCATENATE() { return getToken(SqlBaseParser.CONCATENATE, 0); }
		public TerminalNode FILEFORMAT() { return getToken(SqlBaseParser.FILEFORMAT, 0); }
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode DFS() { return getToken(SqlBaseParser.DFS, 0); }
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public UnsupportedHiveNativeCommandsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unsupportedHiveNativeCommands; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnsupportedHiveNativeCommands(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnsupportedHiveNativeCommands(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnsupportedHiveNativeCommands(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UnsupportedHiveNativeCommandsContext unsupportedHiveNativeCommands() throws RecognitionException {
		UnsupportedHiveNativeCommandsContext _localctx = new UnsupportedHiveNativeCommandsContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_unsupportedHiveNativeCommands);
		int _la;
		try {
			setState(972);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,109,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(793);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(CREATE);
				setState(794);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(795);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DROP);
				setState(796);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(797);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(GRANT);
				setState(799);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,101,_ctx) ) {
				case 1:
					{
					setState(798);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
					}
					break;
				}
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(801);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(REVOKE);
				setState(803);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,102,_ctx) ) {
				case 1:
					{
					setState(802);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
					}
					break;
				}
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(805);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(806);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(GRANT);
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(807);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(808);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLE);
				setState(810);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,103,_ctx) ) {
				case 1:
					{
					setState(809);
					((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(GRANT);
					}
					break;
				}
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(812);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(813);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(PRINCIPALS);
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(814);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(815);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(ROLES);
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(816);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(817);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(CURRENT);
				setState(818);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(ROLES);
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(819);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(EXPORT);
				setState(820);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(821);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(IMPORT);
				setState(822);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(823);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(824);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(COMPACTIONS);
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(825);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(826);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(CREATE);
				setState(827);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(TABLE);
				}
				break;
			case 14:
				enterOuterAlt(_localctx, 14);
				{
				setState(828);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(829);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TRANSACTIONS);
				}
				break;
			case 15:
				enterOuterAlt(_localctx, 15);
				{
				setState(830);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(831);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEXES);
				}
				break;
			case 16:
				enterOuterAlt(_localctx, 16);
				{
				setState(832);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(SHOW);
				setState(833);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(LOCKS);
				}
				break;
			case 17:
				enterOuterAlt(_localctx, 17);
				{
				setState(834);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(CREATE);
				setState(835);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEX);
				}
				break;
			case 18:
				enterOuterAlt(_localctx, 18);
				{
				setState(836);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DROP);
				setState(837);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEX);
				}
				break;
			case 19:
				enterOuterAlt(_localctx, 19);
				{
				setState(838);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(839);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(INDEX);
				}
				break;
			case 20:
				enterOuterAlt(_localctx, 20);
				{
				setState(840);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(LOCK);
				setState(841);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 21:
				enterOuterAlt(_localctx, 21);
				{
				setState(842);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(LOCK);
				setState(843);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(DATABASE);
				}
				break;
			case 22:
				enterOuterAlt(_localctx, 22);
				{
				setState(844);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(UNLOCK);
				setState(845);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				}
				break;
			case 23:
				enterOuterAlt(_localctx, 23);
				{
				setState(846);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(UNLOCK);
				setState(847);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(DATABASE);
				}
				break;
			case 24:
				enterOuterAlt(_localctx, 24);
				{
				setState(848);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(CREATE);
				setState(849);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TEMPORARY);
				setState(850);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(MACRO);
				}
				break;
			case 25:
				enterOuterAlt(_localctx, 25);
				{
				setState(851);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DROP);
				setState(852);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TEMPORARY);
				setState(853);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(MACRO);
				}
				break;
			case 26:
				enterOuterAlt(_localctx, 26);
				{
				setState(854);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(855);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(856);
				tableIdentifier();
				setState(857);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(858);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(CLUSTERED);
				}
				break;
			case 27:
				enterOuterAlt(_localctx, 27);
				{
				setState(860);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(861);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(862);
				tableIdentifier();
				setState(863);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(CLUSTERED);
				setState(864);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(BY);
				}
				break;
			case 28:
				enterOuterAlt(_localctx, 28);
				{
				setState(866);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(867);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(868);
				tableIdentifier();
				setState(869);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(870);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(SORTED);
				}
				break;
			case 29:
				enterOuterAlt(_localctx, 29);
				{
				setState(872);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(873);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(874);
				tableIdentifier();
				setState(875);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(SKEWED);
				setState(876);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(BY);
				}
				break;
			case 30:
				enterOuterAlt(_localctx, 30);
				{
				setState(878);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(879);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(880);
				tableIdentifier();
				setState(881);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(882);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(SKEWED);
				}
				break;
			case 31:
				enterOuterAlt(_localctx, 31);
				{
				setState(884);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(885);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(886);
				tableIdentifier();
				setState(887);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(NOT);
				setState(888);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(STORED);
				setState(889);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw5 = match(AS);
				setState(890);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw6 = match(DIRECTORIES);
				}
				break;
			case 32:
				enterOuterAlt(_localctx, 32);
				{
				setState(892);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(893);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(894);
				tableIdentifier();
				setState(895);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(SET);
				setState(896);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(SKEWED);
				setState(897);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw5 = match(LOCATION);
				}
				break;
			case 33:
				enterOuterAlt(_localctx, 33);
				{
				setState(899);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(900);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(901);
				tableIdentifier();
				setState(902);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(EXCHANGE);
				setState(903);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(PARTITION);
				}
				break;
			case 34:
				enterOuterAlt(_localctx, 34);
				{
				setState(905);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(906);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(907);
				tableIdentifier();
				setState(908);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(ARCHIVE);
				setState(909);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(PARTITION);
				}
				break;
			case 35:
				enterOuterAlt(_localctx, 35);
				{
				setState(911);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(912);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(913);
				tableIdentifier();
				setState(914);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(UNARCHIVE);
				setState(915);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(PARTITION);
				}
				break;
			case 36:
				enterOuterAlt(_localctx, 36);
				{
				setState(917);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(918);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(919);
				tableIdentifier();
				setState(920);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(TOUCH);
				}
				break;
			case 37:
				enterOuterAlt(_localctx, 37);
				{
				setState(922);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(923);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(924);
				tableIdentifier();
				setState(926);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(925);
					partitionSpec();
					}
				}

				setState(928);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(COMPACT);
				}
				break;
			case 38:
				enterOuterAlt(_localctx, 38);
				{
				setState(930);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(931);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(932);
				tableIdentifier();
				setState(934);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(933);
					partitionSpec();
					}
				}

				setState(936);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(CONCATENATE);
				}
				break;
			case 39:
				enterOuterAlt(_localctx, 39);
				{
				setState(938);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(939);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(940);
				tableIdentifier();
				setState(942);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(941);
					partitionSpec();
					}
				}

				setState(944);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(SET);
				setState(945);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(FILEFORMAT);
				}
				break;
			case 40:
				enterOuterAlt(_localctx, 40);
				{
				setState(947);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(948);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(949);
				tableIdentifier();
				setState(951);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(950);
					partitionSpec();
					}
				}

				setState(953);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(ADD);
				setState(954);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(COLUMNS);
				}
				break;
			case 41:
				enterOuterAlt(_localctx, 41);
				{
				setState(956);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ALTER);
				setState(957);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TABLE);
				setState(958);
				tableIdentifier();
				setState(960);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(959);
					partitionSpec();
					}
				}

				setState(962);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw3 = match(REPLACE);
				setState(963);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw4 = match(COLUMNS);
				}
				break;
			case 42:
				enterOuterAlt(_localctx, 42);
				{
				setState(965);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(START);
				setState(966);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(TRANSACTION);
				}
				break;
			case 43:
				enterOuterAlt(_localctx, 43);
				{
				setState(967);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(COMMIT);
				}
				break;
			case 44:
				enterOuterAlt(_localctx, 44);
				{
				setState(968);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(ROLLBACK);
				}
				break;
			case 45:
				enterOuterAlt(_localctx, 45);
				{
				setState(969);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DFS);
				}
				break;
			case 46:
				enterOuterAlt(_localctx, 46);
				{
				setState(970);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw1 = match(DELETE);
				setState(971);
				((UnsupportedHiveNativeCommandsContext)_localctx).kw2 = match(FROM);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateTableHeaderContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode EXTERNAL() { return getToken(SqlBaseParser.EXTERNAL, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public CreateTableHeaderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createTableHeader; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateTableHeader(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateTableHeader(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateTableHeader(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateTableHeaderContext createTableHeader() throws RecognitionException {
		CreateTableHeaderContext _localctx = new CreateTableHeaderContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_createTableHeader);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(974);
			match(CREATE);
			setState(976);
			_la = _input.LA(1);
			if (_la==TEMPORARY) {
				{
				setState(975);
				match(TEMPORARY);
				}
			}

			setState(979);
			_la = _input.LA(1);
			if (_la==EXTERNAL) {
				{
				setState(978);
				match(EXTERNAL);
				}
			}

			setState(981);
			match(TABLE);
			setState(985);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,112,_ctx) ) {
			case 1:
				{
				setState(982);
				match(IF);
				setState(983);
				match(NOT);
				setState(984);
				match(EXISTS);
				}
				break;
			}
			setState(987);
			tableIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BucketSpecContext extends ParserRuleContext {
		public TerminalNode CLUSTERED() { return getToken(SqlBaseParser.CLUSTERED, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode BUCKETS() { return getToken(SqlBaseParser.BUCKETS, 0); }
		public TerminalNode SORTED() { return getToken(SqlBaseParser.SORTED, 0); }
		public OrderedIdentifierListContext orderedIdentifierList() {
			return getRuleContext(OrderedIdentifierListContext.class,0);
		}
		public BucketSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_bucketSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBucketSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBucketSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBucketSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BucketSpecContext bucketSpec() throws RecognitionException {
		BucketSpecContext _localctx = new BucketSpecContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_bucketSpec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(989);
			match(CLUSTERED);
			setState(990);
			match(BY);
			setState(991);
			identifierList();
			setState(995);
			_la = _input.LA(1);
			if (_la==SORTED) {
				{
				setState(992);
				match(SORTED);
				setState(993);
				match(BY);
				setState(994);
				orderedIdentifierList();
				}
			}

			setState(997);
			match(INTO);
			setState(998);
			match(INTEGER_VALUE);
			setState(999);
			match(BUCKETS);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SkewSpecContext extends ParserRuleContext {
		public TerminalNode SKEWED() { return getToken(SqlBaseParser.SKEWED, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public ConstantListContext constantList() {
			return getRuleContext(ConstantListContext.class,0);
		}
		public NestedConstantListContext nestedConstantList() {
			return getRuleContext(NestedConstantListContext.class,0);
		}
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlBaseParser.DIRECTORIES, 0); }
		public SkewSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_skewSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSkewSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSkewSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSkewSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SkewSpecContext skewSpec() throws RecognitionException {
		SkewSpecContext _localctx = new SkewSpecContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_skewSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1001);
			match(SKEWED);
			setState(1002);
			match(BY);
			setState(1003);
			identifierList();
			setState(1004);
			match(ON);
			setState(1007);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,114,_ctx) ) {
			case 1:
				{
				setState(1005);
				constantList();
				}
				break;
			case 2:
				{
				setState(1006);
				nestedConstantList();
				}
				break;
			}
			setState(1012);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,115,_ctx) ) {
			case 1:
				{
				setState(1009);
				match(STORED);
				setState(1010);
				match(AS);
				setState(1011);
				match(DIRECTORIES);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LocationSpecContext extends ParserRuleContext {
		public TerminalNode LOCATION() { return getToken(SqlBaseParser.LOCATION, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public LocationSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_locationSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLocationSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLocationSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLocationSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LocationSpecContext locationSpec() throws RecognitionException {
		LocationSpecContext _localctx = new LocationSpecContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_locationSpec);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1014);
			match(LOCATION);
			setState(1015);
			match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryContext extends ParserRuleContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public CtesContext ctes() {
			return getRuleContext(CtesContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_query);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1018);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(1017);
				ctes();
				}
			}

			setState(1020);
			queryNoWith();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InsertIntoContext extends ParserRuleContext {
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public InsertIntoContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertInto; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInsertInto(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInsertInto(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInsertInto(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InsertIntoContext insertInto() throws RecognitionException {
		InsertIntoContext _localctx = new InsertIntoContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_insertInto);
		int _la;
		try {
			setState(1043);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,121,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1022);
				match(INSERT);
				setState(1023);
				match(OVERWRITE);
				setState(1024);
				match(TABLE);
				setState(1025);
				tableIdentifier();
				setState(1032);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1026);
					partitionSpec();
					setState(1030);
					_la = _input.LA(1);
					if (_la==IF) {
						{
						setState(1027);
						match(IF);
						setState(1028);
						match(NOT);
						setState(1029);
						match(EXISTS);
						}
					}

					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1034);
				match(INSERT);
				setState(1035);
				match(INTO);
				setState(1037);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,119,_ctx) ) {
				case 1:
					{
					setState(1036);
					match(TABLE);
					}
					break;
				}
				setState(1039);
				tableIdentifier();
				setState(1041);
				_la = _input.LA(1);
				if (_la==PARTITION) {
					{
					setState(1040);
					partitionSpec();
					}
				}

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PartitionSpecLocationContext extends ParserRuleContext {
		public PartitionSpecContext partitionSpec() {
			return getRuleContext(PartitionSpecContext.class,0);
		}
		public LocationSpecContext locationSpec() {
			return getRuleContext(LocationSpecContext.class,0);
		}
		public PartitionSpecLocationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionSpecLocation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPartitionSpecLocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPartitionSpecLocation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPartitionSpecLocation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PartitionSpecLocationContext partitionSpecLocation() throws RecognitionException {
		PartitionSpecLocationContext _localctx = new PartitionSpecLocationContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_partitionSpecLocation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1045);
			partitionSpec();
			setState(1047);
			_la = _input.LA(1);
			if (_la==LOCATION) {
				{
				setState(1046);
				locationSpec();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PartitionSpecContext extends ParserRuleContext {
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public List<PartitionValContext> partitionVal() {
			return getRuleContexts(PartitionValContext.class);
		}
		public PartitionValContext partitionVal(int i) {
			return getRuleContext(PartitionValContext.class,i);
		}
		public PartitionSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionSpec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPartitionSpec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPartitionSpec(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPartitionSpec(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PartitionSpecContext partitionSpec() throws RecognitionException {
		PartitionSpecContext _localctx = new PartitionSpecContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_partitionSpec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1049);
			match(PARTITION);
			setState(1050);
			match(T__0);
			setState(1051);
			partitionVal();
			setState(1056);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1052);
				match(T__2);
				setState(1053);
				partitionVal();
				}
				}
				setState(1058);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1059);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PartitionValContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public PartitionValContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionVal; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPartitionVal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPartitionVal(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPartitionVal(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PartitionValContext partitionVal() throws RecognitionException {
		PartitionValContext _localctx = new PartitionValContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_partitionVal);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1061);
			identifier();
			setState(1064);
			_la = _input.LA(1);
			if (_la==EQ) {
				{
				setState(1062);
				match(EQ);
				setState(1063);
				constant();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DescribeFuncNameContext extends ParserRuleContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public ArithmeticOperatorContext arithmeticOperator() {
			return getRuleContext(ArithmeticOperatorContext.class,0);
		}
		public PredicateOperatorContext predicateOperator() {
			return getRuleContext(PredicateOperatorContext.class,0);
		}
		public DescribeFuncNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_describeFuncName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeFuncName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeFuncName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeFuncName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DescribeFuncNameContext describeFuncName() throws RecognitionException {
		DescribeFuncNameContext _localctx = new DescribeFuncNameContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_describeFuncName);
		try {
			setState(1071);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,125,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1066);
				qualifiedName();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1067);
				match(STRING);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1068);
				comparisonOperator();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1069);
				arithmeticOperator();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1070);
				predicateOperator();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DescribeColNameContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public DescribeColNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_describeColName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDescribeColName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDescribeColName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDescribeColName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DescribeColNameContext describeColName() throws RecognitionException {
		DescribeColNameContext _localctx = new DescribeColNameContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_describeColName);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1073);
			identifier();
			setState(1081);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(1074);
				match(T__3);
				setState(1077);
				switch (_input.LA(1)) {
				case SELECT:
				case FROM:
				case ADD:
				case AS:
				case ALL:
				case DISTINCT:
				case WHERE:
				case GROUP:
				case BY:
				case GROUPING:
				case SETS:
				case CUBE:
				case ROLLUP:
				case ORDER:
				case HAVING:
				case LIMIT:
				case AT:
				case OR:
				case AND:
				case IN:
				case NOT:
				case NO:
				case EXISTS:
				case BETWEEN:
				case LIKE:
				case RLIKE:
				case IS:
				case NULL:
				case TRUE:
				case FALSE:
				case NULLS:
				case ASC:
				case DESC:
				case FOR:
				case INTERVAL:
				case CASE:
				case WHEN:
				case THEN:
				case ELSE:
				case END:
				case JOIN:
				case CROSS:
				case OUTER:
				case INNER:
				case LEFT:
				case SEMI:
				case RIGHT:
				case FULL:
				case NATURAL:
				case ON:
				case LATERAL:
				case WINDOW:
				case OVER:
				case PARTITION:
				case RANGE:
				case ROWS:
				case UNBOUNDED:
				case PRECEDING:
				case FOLLOWING:
				case CURRENT:
				case FIRST:
				case AFTER:
				case LAST:
				case ROW:
				case WITH:
				case VALUES:
				case CREATE:
				case TABLE:
				case VIEW:
				case REPLACE:
				case INSERT:
				case DELETE:
				case INTO:
				case DESCRIBE:
				case EXPLAIN:
				case FORMAT:
				case LOGICAL:
				case CODEGEN:
				case CAST:
				case SHOW:
				case TABLES:
				case COLUMNS:
				case COLUMN:
				case USE:
				case PARTITIONS:
				case FUNCTIONS:
				case DROP:
				case UNION:
				case EXCEPT:
				case SETMINUS:
				case INTERSECT:
				case TO:
				case TABLESAMPLE:
				case STRATIFY:
				case ALTER:
				case RENAME:
				case ARRAY:
				case MAP:
				case STRUCT:
				case COMMENT:
				case SET:
				case RESET:
				case DATA:
				case START:
				case TRANSACTION:
				case COMMIT:
				case ROLLBACK:
				case MACRO:
				case IF:
				case DIV:
				case PERCENTLIT:
				case BUCKET:
				case OUT:
				case OF:
				case SORT:
				case CLUSTER:
				case DISTRIBUTE:
				case OVERWRITE:
				case TRANSFORM:
				case REDUCE:
				case USING:
				case SERDE:
				case SERDEPROPERTIES:
				case RECORDREADER:
				case RECORDWRITER:
				case DELIMITED:
				case FIELDS:
				case TERMINATED:
				case COLLECTION:
				case ITEMS:
				case KEYS:
				case ESCAPED:
				case LINES:
				case SEPARATED:
				case FUNCTION:
				case EXTENDED:
				case REFRESH:
				case CLEAR:
				case CACHE:
				case UNCACHE:
				case LAZY:
				case FORMATTED:
				case GLOBAL:
				case TEMPORARY:
				case OPTIONS:
				case UNSET:
				case TBLPROPERTIES:
				case DBPROPERTIES:
				case BUCKETS:
				case SKEWED:
				case STORED:
				case DIRECTORIES:
				case LOCATION:
				case EXCHANGE:
				case ARCHIVE:
				case UNARCHIVE:
				case FILEFORMAT:
				case TOUCH:
				case COMPACT:
				case CONCATENATE:
				case CHANGE:
				case CASCADE:
				case RESTRICT:
				case CLUSTERED:
				case SORTED:
				case PURGE:
				case INPUTFORMAT:
				case OUTPUTFORMAT:
				case DATABASE:
				case DATABASES:
				case DFS:
				case TRUNCATE:
				case ANALYZE:
				case COMPUTE:
				case LIST:
				case STATISTICS:
				case PARTITIONED:
				case EXTERNAL:
				case DEFINED:
				case REVOKE:
				case GRANT:
				case LOCK:
				case UNLOCK:
				case MSCK:
				case REPAIR:
				case RECOVER:
				case EXPORT:
				case IMPORT:
				case LOAD:
				case ROLE:
				case ROLES:
				case COMPACTIONS:
				case PRINCIPALS:
				case TRANSACTIONS:
				case INDEX:
				case INDEXES:
				case LOCKS:
				case OPTION:
				case ANTI:
				case LOCAL:
				case INPATH:
				case CURRENT_DATE:
				case CURRENT_TIMESTAMP:
				case IDENTIFIER:
				case BACKQUOTED_IDENTIFIER:
					{
					setState(1075);
					identifier();
					}
					break;
				case STRING:
					{
					setState(1076);
					match(STRING);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				}
				setState(1083);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CtesContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public List<NamedQueryContext> namedQuery() {
			return getRuleContexts(NamedQueryContext.class);
		}
		public NamedQueryContext namedQuery(int i) {
			return getRuleContext(NamedQueryContext.class,i);
		}
		public CtesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ctes; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCtes(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCtes(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCtes(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CtesContext ctes() throws RecognitionException {
		CtesContext _localctx = new CtesContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_ctes);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1084);
			match(WITH);
			setState(1085);
			namedQuery();
			setState(1090);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1086);
				match(T__2);
				setState(1087);
				namedQuery();
				}
				}
				setState(1092);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamedQueryContext extends ParserRuleContext {
		public IdentifierContext name;
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public NamedQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedQueryContext namedQuery() throws RecognitionException {
		NamedQueryContext _localctx = new NamedQueryContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_namedQuery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1093);
			((NamedQueryContext)_localctx).name = identifier();
			setState(1095);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(1094);
				match(AS);
				}
			}

			setState(1097);
			match(T__0);
			setState(1098);
			query();
			setState(1099);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableProviderContext extends ParserRuleContext {
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public TableProviderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableProvider; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableProvider(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableProvider(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableProvider(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableProviderContext tableProvider() throws RecognitionException {
		TableProviderContext _localctx = new TableProviderContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_tableProvider);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1101);
			match(USING);
			setState(1102);
			qualifiedName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablePropertyListContext extends ParserRuleContext {
		public List<TablePropertyContext> tableProperty() {
			return getRuleContexts(TablePropertyContext.class);
		}
		public TablePropertyContext tableProperty(int i) {
			return getRuleContext(TablePropertyContext.class,i);
		}
		public TablePropertyListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tablePropertyList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTablePropertyList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTablePropertyList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTablePropertyList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TablePropertyListContext tablePropertyList() throws RecognitionException {
		TablePropertyListContext _localctx = new TablePropertyListContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_tablePropertyList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1104);
			match(T__0);
			setState(1105);
			tableProperty();
			setState(1110);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1106);
				match(T__2);
				setState(1107);
				tableProperty();
				}
				}
				setState(1112);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1113);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablePropertyContext extends ParserRuleContext {
		public TablePropertyKeyContext key;
		public TablePropertyValueContext value;
		public TablePropertyKeyContext tablePropertyKey() {
			return getRuleContext(TablePropertyKeyContext.class,0);
		}
		public TablePropertyValueContext tablePropertyValue() {
			return getRuleContext(TablePropertyValueContext.class,0);
		}
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public TablePropertyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableProperty; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableProperty(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableProperty(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableProperty(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TablePropertyContext tableProperty() throws RecognitionException {
		TablePropertyContext _localctx = new TablePropertyContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_tableProperty);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1115);
			((TablePropertyContext)_localctx).key = tablePropertyKey();
			setState(1120);
			_la = _input.LA(1);
			if (_la==TRUE || _la==FALSE || _la==EQ || ((((_la - 228)) & ~0x3f) == 0 && ((1L << (_la - 228)) & ((1L << (STRING - 228)) | (1L << (INTEGER_VALUE - 228)) | (1L << (DECIMAL_VALUE - 228)))) != 0)) {
				{
				setState(1117);
				_la = _input.LA(1);
				if (_la==EQ) {
					{
					setState(1116);
					match(EQ);
					}
				}

				setState(1119);
				((TablePropertyContext)_localctx).value = tablePropertyValue();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablePropertyKeyContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TablePropertyKeyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tablePropertyKey; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTablePropertyKey(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTablePropertyKey(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTablePropertyKey(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TablePropertyKeyContext tablePropertyKey() throws RecognitionException {
		TablePropertyKeyContext _localctx = new TablePropertyKeyContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_tablePropertyKey);
		int _la;
		try {
			setState(1131);
			switch (_input.LA(1)) {
			case SELECT:
			case FROM:
			case ADD:
			case AS:
			case ALL:
			case DISTINCT:
			case WHERE:
			case GROUP:
			case BY:
			case GROUPING:
			case SETS:
			case CUBE:
			case ROLLUP:
			case ORDER:
			case HAVING:
			case LIMIT:
			case AT:
			case OR:
			case AND:
			case IN:
			case NOT:
			case NO:
			case EXISTS:
			case BETWEEN:
			case LIKE:
			case RLIKE:
			case IS:
			case NULL:
			case TRUE:
			case FALSE:
			case NULLS:
			case ASC:
			case DESC:
			case FOR:
			case INTERVAL:
			case CASE:
			case WHEN:
			case THEN:
			case ELSE:
			case END:
			case JOIN:
			case CROSS:
			case OUTER:
			case INNER:
			case LEFT:
			case SEMI:
			case RIGHT:
			case FULL:
			case NATURAL:
			case ON:
			case LATERAL:
			case WINDOW:
			case OVER:
			case PARTITION:
			case RANGE:
			case ROWS:
			case UNBOUNDED:
			case PRECEDING:
			case FOLLOWING:
			case CURRENT:
			case FIRST:
			case AFTER:
			case LAST:
			case ROW:
			case WITH:
			case VALUES:
			case CREATE:
			case TABLE:
			case VIEW:
			case REPLACE:
			case INSERT:
			case DELETE:
			case INTO:
			case DESCRIBE:
			case EXPLAIN:
			case FORMAT:
			case LOGICAL:
			case CODEGEN:
			case CAST:
			case SHOW:
			case TABLES:
			case COLUMNS:
			case COLUMN:
			case USE:
			case PARTITIONS:
			case FUNCTIONS:
			case DROP:
			case UNION:
			case EXCEPT:
			case SETMINUS:
			case INTERSECT:
			case TO:
			case TABLESAMPLE:
			case STRATIFY:
			case ALTER:
			case RENAME:
			case ARRAY:
			case MAP:
			case STRUCT:
			case COMMENT:
			case SET:
			case RESET:
			case DATA:
			case START:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case MACRO:
			case IF:
			case DIV:
			case PERCENTLIT:
			case BUCKET:
			case OUT:
			case OF:
			case SORT:
			case CLUSTER:
			case DISTRIBUTE:
			case OVERWRITE:
			case TRANSFORM:
			case REDUCE:
			case USING:
			case SERDE:
			case SERDEPROPERTIES:
			case RECORDREADER:
			case RECORDWRITER:
			case DELIMITED:
			case FIELDS:
			case TERMINATED:
			case COLLECTION:
			case ITEMS:
			case KEYS:
			case ESCAPED:
			case LINES:
			case SEPARATED:
			case FUNCTION:
			case EXTENDED:
			case REFRESH:
			case CLEAR:
			case CACHE:
			case UNCACHE:
			case LAZY:
			case FORMATTED:
			case GLOBAL:
			case TEMPORARY:
			case OPTIONS:
			case UNSET:
			case TBLPROPERTIES:
			case DBPROPERTIES:
			case BUCKETS:
			case SKEWED:
			case STORED:
			case DIRECTORIES:
			case LOCATION:
			case EXCHANGE:
			case ARCHIVE:
			case UNARCHIVE:
			case FILEFORMAT:
			case TOUCH:
			case COMPACT:
			case CONCATENATE:
			case CHANGE:
			case CASCADE:
			case RESTRICT:
			case CLUSTERED:
			case SORTED:
			case PURGE:
			case INPUTFORMAT:
			case OUTPUTFORMAT:
			case DATABASE:
			case DATABASES:
			case DFS:
			case TRUNCATE:
			case ANALYZE:
			case COMPUTE:
			case LIST:
			case STATISTICS:
			case PARTITIONED:
			case EXTERNAL:
			case DEFINED:
			case REVOKE:
			case GRANT:
			case LOCK:
			case UNLOCK:
			case MSCK:
			case REPAIR:
			case RECOVER:
			case EXPORT:
			case IMPORT:
			case LOAD:
			case ROLE:
			case ROLES:
			case COMPACTIONS:
			case PRINCIPALS:
			case TRANSACTIONS:
			case INDEX:
			case INDEXES:
			case LOCKS:
			case OPTION:
			case ANTI:
			case LOCAL:
			case INPATH:
			case CURRENT_DATE:
			case CURRENT_TIMESTAMP:
			case IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(1122);
				identifier();
				setState(1127);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(1123);
					match(T__3);
					setState(1124);
					identifier();
					}
					}
					setState(1129);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1130);
				match(STRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablePropertyValueContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TablePropertyValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tablePropertyValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTablePropertyValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTablePropertyValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTablePropertyValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TablePropertyValueContext tablePropertyValue() throws RecognitionException {
		TablePropertyValueContext _localctx = new TablePropertyValueContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_tablePropertyValue);
		try {
			setState(1137);
			switch (_input.LA(1)) {
			case INTEGER_VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1133);
				match(INTEGER_VALUE);
				}
				break;
			case DECIMAL_VALUE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1134);
				match(DECIMAL_VALUE);
				}
				break;
			case TRUE:
			case FALSE:
				enterOuterAlt(_localctx, 3);
				{
				setState(1135);
				booleanValue();
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 4);
				{
				setState(1136);
				match(STRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConstantListContext extends ParserRuleContext {
		public List<ConstantContext> constant() {
			return getRuleContexts(ConstantContext.class);
		}
		public ConstantContext constant(int i) {
			return getRuleContext(ConstantContext.class,i);
		}
		public ConstantListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constantList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterConstantList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitConstantList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitConstantList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstantListContext constantList() throws RecognitionException {
		ConstantListContext _localctx = new ConstantListContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_constantList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1139);
			match(T__0);
			setState(1140);
			constant();
			setState(1145);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1141);
				match(T__2);
				setState(1142);
				constant();
				}
				}
				setState(1147);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1148);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NestedConstantListContext extends ParserRuleContext {
		public List<ConstantListContext> constantList() {
			return getRuleContexts(ConstantListContext.class);
		}
		public ConstantListContext constantList(int i) {
			return getRuleContext(ConstantListContext.class,i);
		}
		public NestedConstantListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nestedConstantList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNestedConstantList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNestedConstantList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNestedConstantList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NestedConstantListContext nestedConstantList() throws RecognitionException {
		NestedConstantListContext _localctx = new NestedConstantListContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_nestedConstantList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1150);
			match(T__0);
			setState(1151);
			constantList();
			setState(1156);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1152);
				match(T__2);
				setState(1153);
				constantList();
				}
				}
				setState(1158);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1159);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateFileFormatContext extends ParserRuleContext {
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public FileFormatContext fileFormat() {
			return getRuleContext(FileFormatContext.class,0);
		}
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public StorageHandlerContext storageHandler() {
			return getRuleContext(StorageHandlerContext.class,0);
		}
		public CreateFileFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createFileFormat; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCreateFileFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCreateFileFormat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCreateFileFormat(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateFileFormatContext createFileFormat() throws RecognitionException {
		CreateFileFormatContext _localctx = new CreateFileFormatContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_createFileFormat);
		try {
			setState(1167);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,138,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1161);
				match(STORED);
				setState(1162);
				match(AS);
				setState(1163);
				fileFormat();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1164);
				match(STORED);
				setState(1165);
				match(BY);
				setState(1166);
				storageHandler();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FileFormatContext extends ParserRuleContext {
		public FileFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fileFormat; }
	 
		public FileFormatContext() { }
		public void copyFrom(FileFormatContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class TableFileFormatContext extends FileFormatContext {
		public Token inFmt;
		public Token outFmt;
		public TerminalNode INPUTFORMAT() { return getToken(SqlBaseParser.INPUTFORMAT, 0); }
		public TerminalNode OUTPUTFORMAT() { return getToken(SqlBaseParser.OUTPUTFORMAT, 0); }
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public TableFileFormatContext(FileFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableFileFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableFileFormat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableFileFormat(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class GenericFileFormatContext extends FileFormatContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public GenericFileFormatContext(FileFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGenericFileFormat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGenericFileFormat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGenericFileFormat(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FileFormatContext fileFormat() throws RecognitionException {
		FileFormatContext _localctx = new FileFormatContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_fileFormat);
		try {
			setState(1174);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,139,_ctx) ) {
			case 1:
				_localctx = new TableFileFormatContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1169);
				match(INPUTFORMAT);
				setState(1170);
				((TableFileFormatContext)_localctx).inFmt = match(STRING);
				setState(1171);
				match(OUTPUTFORMAT);
				setState(1172);
				((TableFileFormatContext)_localctx).outFmt = match(STRING);
				}
				break;
			case 2:
				_localctx = new GenericFileFormatContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1173);
				identifier();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StorageHandlerContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public StorageHandlerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_storageHandler; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStorageHandler(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStorageHandler(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStorageHandler(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StorageHandlerContext storageHandler() throws RecognitionException {
		StorageHandlerContext _localctx = new StorageHandlerContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_storageHandler);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1176);
			match(STRING);
			setState(1180);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,140,_ctx) ) {
			case 1:
				{
				setState(1177);
				match(WITH);
				setState(1178);
				match(SERDEPROPERTIES);
				setState(1179);
				tablePropertyList();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ResourceContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ResourceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_resource; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterResource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitResource(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitResource(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ResourceContext resource() throws RecognitionException {
		ResourceContext _localctx = new ResourceContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_resource);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1182);
			identifier();
			setState(1183);
			match(STRING);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryNoWithContext extends ParserRuleContext {
		public QueryNoWithContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryNoWith; }
	 
		public QueryNoWithContext() { }
		public void copyFrom(QueryNoWithContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class SingleInsertQueryContext extends QueryNoWithContext {
		public QueryTermContext queryTerm() {
			return getRuleContext(QueryTermContext.class,0);
		}
		public QueryOrganizationContext queryOrganization() {
			return getRuleContext(QueryOrganizationContext.class,0);
		}
		public InsertIntoContext insertInto() {
			return getRuleContext(InsertIntoContext.class,0);
		}
		public SingleInsertQueryContext(QueryNoWithContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSingleInsertQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSingleInsertQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSingleInsertQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class MultiInsertQueryContext extends QueryNoWithContext {
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public List<MultiInsertQueryBodyContext> multiInsertQueryBody() {
			return getRuleContexts(MultiInsertQueryBodyContext.class);
		}
		public MultiInsertQueryBodyContext multiInsertQueryBody(int i) {
			return getRuleContext(MultiInsertQueryBodyContext.class,i);
		}
		public MultiInsertQueryContext(QueryNoWithContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMultiInsertQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMultiInsertQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMultiInsertQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryNoWithContext queryNoWith() throws RecognitionException {
		QueryNoWithContext _localctx = new QueryNoWithContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_queryNoWith);
		int _la;
		try {
			setState(1197);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,143,_ctx) ) {
			case 1:
				_localctx = new SingleInsertQueryContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1186);
				_la = _input.LA(1);
				if (_la==INSERT) {
					{
					setState(1185);
					insertInto();
					}
				}

				setState(1188);
				queryTerm(0);
				setState(1189);
				queryOrganization();
				}
				break;
			case 2:
				_localctx = new MultiInsertQueryContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1191);
				fromClause();
				setState(1193); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1192);
					multiInsertQueryBody();
					}
					}
					setState(1195); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==SELECT || _la==FROM || _la==INSERT || _la==MAP || _la==REDUCE );
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryOrganizationContext extends ParserRuleContext {
		public SortItemContext sortItem;
		public List<SortItemContext> order = new ArrayList<SortItemContext>();
		public ExpressionContext expression;
		public List<ExpressionContext> clusterBy = new ArrayList<ExpressionContext>();
		public List<ExpressionContext> distributeBy = new ArrayList<ExpressionContext>();
		public List<SortItemContext> sort = new ArrayList<SortItemContext>();
		public ExpressionContext limit;
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public TerminalNode CLUSTER() { return getToken(SqlBaseParser.CLUSTER, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlBaseParser.DISTRIBUTE, 0); }
		public TerminalNode SORT() { return getToken(SqlBaseParser.SORT, 0); }
		public WindowsContext windows() {
			return getRuleContext(WindowsContext.class,0);
		}
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public QueryOrganizationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryOrganization; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryOrganization(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryOrganization(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryOrganization(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryOrganizationContext queryOrganization() throws RecognitionException {
		QueryOrganizationContext _localctx = new QueryOrganizationContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_queryOrganization);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1209);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(1199);
				match(ORDER);
				setState(1200);
				match(BY);
				setState(1201);
				((QueryOrganizationContext)_localctx).sortItem = sortItem();
				((QueryOrganizationContext)_localctx).order.add(((QueryOrganizationContext)_localctx).sortItem);
				setState(1206);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1202);
					match(T__2);
					setState(1203);
					((QueryOrganizationContext)_localctx).sortItem = sortItem();
					((QueryOrganizationContext)_localctx).order.add(((QueryOrganizationContext)_localctx).sortItem);
					}
					}
					setState(1208);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1221);
			_la = _input.LA(1);
			if (_la==CLUSTER) {
				{
				setState(1211);
				match(CLUSTER);
				setState(1212);
				match(BY);
				setState(1213);
				((QueryOrganizationContext)_localctx).expression = expression();
				((QueryOrganizationContext)_localctx).clusterBy.add(((QueryOrganizationContext)_localctx).expression);
				setState(1218);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1214);
					match(T__2);
					setState(1215);
					((QueryOrganizationContext)_localctx).expression = expression();
					((QueryOrganizationContext)_localctx).clusterBy.add(((QueryOrganizationContext)_localctx).expression);
					}
					}
					setState(1220);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1233);
			_la = _input.LA(1);
			if (_la==DISTRIBUTE) {
				{
				setState(1223);
				match(DISTRIBUTE);
				setState(1224);
				match(BY);
				setState(1225);
				((QueryOrganizationContext)_localctx).expression = expression();
				((QueryOrganizationContext)_localctx).distributeBy.add(((QueryOrganizationContext)_localctx).expression);
				setState(1230);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1226);
					match(T__2);
					setState(1227);
					((QueryOrganizationContext)_localctx).expression = expression();
					((QueryOrganizationContext)_localctx).distributeBy.add(((QueryOrganizationContext)_localctx).expression);
					}
					}
					setState(1232);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1245);
			_la = _input.LA(1);
			if (_la==SORT) {
				{
				setState(1235);
				match(SORT);
				setState(1236);
				match(BY);
				setState(1237);
				((QueryOrganizationContext)_localctx).sortItem = sortItem();
				((QueryOrganizationContext)_localctx).sort.add(((QueryOrganizationContext)_localctx).sortItem);
				setState(1242);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1238);
					match(T__2);
					setState(1239);
					((QueryOrganizationContext)_localctx).sortItem = sortItem();
					((QueryOrganizationContext)_localctx).sort.add(((QueryOrganizationContext)_localctx).sortItem);
					}
					}
					setState(1244);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1248);
			_la = _input.LA(1);
			if (_la==WINDOW) {
				{
				setState(1247);
				windows();
				}
			}

			setState(1252);
			_la = _input.LA(1);
			if (_la==LIMIT) {
				{
				setState(1250);
				match(LIMIT);
				setState(1251);
				((QueryOrganizationContext)_localctx).limit = expression();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MultiInsertQueryBodyContext extends ParserRuleContext {
		public QuerySpecificationContext querySpecification() {
			return getRuleContext(QuerySpecificationContext.class,0);
		}
		public QueryOrganizationContext queryOrganization() {
			return getRuleContext(QueryOrganizationContext.class,0);
		}
		public InsertIntoContext insertInto() {
			return getRuleContext(InsertIntoContext.class,0);
		}
		public MultiInsertQueryBodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiInsertQueryBody; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterMultiInsertQueryBody(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitMultiInsertQueryBody(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitMultiInsertQueryBody(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultiInsertQueryBodyContext multiInsertQueryBody() throws RecognitionException {
		MultiInsertQueryBodyContext _localctx = new MultiInsertQueryBodyContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_multiInsertQueryBody);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1255);
			_la = _input.LA(1);
			if (_la==INSERT) {
				{
				setState(1254);
				insertInto();
				}
			}

			setState(1257);
			querySpecification();
			setState(1258);
			queryOrganization();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryTermContext extends ParserRuleContext {
		public QueryTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryTerm; }
	 
		public QueryTermContext() { }
		public void copyFrom(QueryTermContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class QueryTermDefaultContext extends QueryTermContext {
		public QueryPrimaryContext queryPrimary() {
			return getRuleContext(QueryPrimaryContext.class,0);
		}
		public QueryTermDefaultContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryTermDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryTermDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryTermDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SetOperationContext extends QueryTermContext {
		public QueryTermContext left;
		public Token operator;
		public QueryTermContext right;
		public List<QueryTermContext> queryTerm() {
			return getRuleContexts(QueryTermContext.class);
		}
		public QueryTermContext queryTerm(int i) {
			return getRuleContext(QueryTermContext.class,i);
		}
		public TerminalNode INTERSECT() { return getToken(SqlBaseParser.INTERSECT, 0); }
		public TerminalNode UNION() { return getToken(SqlBaseParser.UNION, 0); }
		public TerminalNode EXCEPT() { return getToken(SqlBaseParser.EXCEPT, 0); }
		public TerminalNode SETMINUS() { return getToken(SqlBaseParser.SETMINUS, 0); }
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public SetOperationContext(QueryTermContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetOperation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetOperation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetOperation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryTermContext queryTerm() throws RecognitionException {
		return queryTerm(0);
	}

	private QueryTermContext queryTerm(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		QueryTermContext _localctx = new QueryTermContext(_ctx, _parentState);
		QueryTermContext _prevctx = _localctx;
		int _startState = 66;
		enterRecursionRule(_localctx, 66, RULE_queryTerm, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new QueryTermDefaultContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(1261);
			queryPrimary();
			}
			_ctx.stop = _input.LT(-1);
			setState(1271);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,156,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
					((SetOperationContext)_localctx).left = _prevctx;
					pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
					setState(1263);
					if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
					setState(1264);
					((SetOperationContext)_localctx).operator = _input.LT(1);
					_la = _input.LA(1);
					if ( !(((((_la - 95)) & ~0x3f) == 0 && ((1L << (_la - 95)) & ((1L << (UNION - 95)) | (1L << (EXCEPT - 95)) | (1L << (SETMINUS - 95)) | (1L << (INTERSECT - 95)))) != 0)) ) {
						((SetOperationContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
					} else {
						consume();
					}
					setState(1266);
					_la = _input.LA(1);
					if (_la==ALL || _la==DISTINCT) {
						{
						setState(1265);
						setQuantifier();
						}
					}

					setState(1268);
					((SetOperationContext)_localctx).right = queryTerm(2);
					}
					} 
				}
				setState(1273);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,156,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class QueryPrimaryContext extends ParserRuleContext {
		public QueryPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryPrimary; }
	 
		public QueryPrimaryContext() { }
		public void copyFrom(QueryPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class QueryPrimaryDefaultContext extends QueryPrimaryContext {
		public QuerySpecificationContext querySpecification() {
			return getRuleContext(QuerySpecificationContext.class,0);
		}
		public QueryPrimaryDefaultContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQueryPrimaryDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQueryPrimaryDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQueryPrimaryDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class InlineTableDefault1Context extends QueryPrimaryContext {
		public InlineTableContext inlineTable() {
			return getRuleContext(InlineTableContext.class,0);
		}
		public InlineTableDefault1Context(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInlineTableDefault1(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInlineTableDefault1(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInlineTableDefault1(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TableContext extends QueryPrimaryContext {
		public TerminalNode TABLE() { return getToken(SqlBaseParser.TABLE, 0); }
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public TableContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTable(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SubqueryContext extends QueryPrimaryContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public SubqueryContext(QueryPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubquery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubquery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubquery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryPrimaryContext queryPrimary() throws RecognitionException {
		QueryPrimaryContext _localctx = new QueryPrimaryContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_queryPrimary);
		try {
			setState(1282);
			switch (_input.LA(1)) {
			case SELECT:
			case FROM:
			case MAP:
			case REDUCE:
				_localctx = new QueryPrimaryDefaultContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1274);
				querySpecification();
				}
				break;
			case TABLE:
				_localctx = new TableContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1275);
				match(TABLE);
				setState(1276);
				tableIdentifier();
				}
				break;
			case VALUES:
				_localctx = new InlineTableDefault1Context(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1277);
				inlineTable();
				}
				break;
			case T__0:
				_localctx = new SubqueryContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1278);
				match(T__0);
				setState(1279);
				queryNoWith();
				setState(1280);
				match(T__1);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SortItemContext extends ParserRuleContext {
		public Token ordering;
		public Token nullOrder;
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public SortItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sortItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSortItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSortItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSortItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SortItemContext sortItem() throws RecognitionException {
		SortItemContext _localctx = new SortItemContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_sortItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1284);
			expression();
			setState(1286);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(1285);
				((SortItemContext)_localctx).ordering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((SortItemContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
			}

			setState(1290);
			_la = _input.LA(1);
			if (_la==NULLS) {
				{
				setState(1288);
				match(NULLS);
				setState(1289);
				((SortItemContext)_localctx).nullOrder = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==FIRST || _la==LAST) ) {
					((SortItemContext)_localctx).nullOrder = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QuerySpecificationContext extends ParserRuleContext {
		public Token kind;
		public RowFormatContext inRowFormat;
		public Token recordWriter;
		public Token script;
		public RowFormatContext outRowFormat;
		public Token recordReader;
		public BooleanExpressionContext where;
		public BooleanExpressionContext having;
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public TerminalNode RECORDWRITER() { return getToken(SqlBaseParser.RECORDWRITER, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode RECORDREADER() { return getToken(SqlBaseParser.RECORDREADER, 0); }
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public NamedExpressionSeqContext namedExpressionSeq() {
			return getRuleContext(NamedExpressionSeqContext.class,0);
		}
		public List<RowFormatContext> rowFormat() {
			return getRuleContexts(RowFormatContext.class);
		}
		public RowFormatContext rowFormat(int i) {
			return getRuleContext(RowFormatContext.class,i);
		}
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode TRANSFORM() { return getToken(SqlBaseParser.TRANSFORM, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode REDUCE() { return getToken(SqlBaseParser.REDUCE, 0); }
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public ColTypeListContext colTypeList() {
			return getRuleContext(ColTypeListContext.class,0);
		}
		public List<LateralViewContext> lateralView() {
			return getRuleContexts(LateralViewContext.class);
		}
		public LateralViewContext lateralView(int i) {
			return getRuleContext(LateralViewContext.class,i);
		}
		public AggregationContext aggregation() {
			return getRuleContext(AggregationContext.class,0);
		}
		public TerminalNode HAVING() { return getToken(SqlBaseParser.HAVING, 0); }
		public WindowsContext windows() {
			return getRuleContext(WindowsContext.class,0);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public QuerySpecificationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_querySpecification; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuerySpecification(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuerySpecification(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuerySpecification(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuerySpecificationContext querySpecification() throws RecognitionException {
		QuerySpecificationContext _localctx = new QuerySpecificationContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_querySpecification);
		int _la;
		try {
			int _alt;
			setState(1379);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,180,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				{
				setState(1302);
				switch (_input.LA(1)) {
				case SELECT:
					{
					setState(1292);
					match(SELECT);
					setState(1293);
					((QuerySpecificationContext)_localctx).kind = match(TRANSFORM);
					setState(1294);
					match(T__0);
					setState(1295);
					namedExpressionSeq();
					setState(1296);
					match(T__1);
					}
					break;
				case MAP:
					{
					setState(1298);
					((QuerySpecificationContext)_localctx).kind = match(MAP);
					setState(1299);
					namedExpressionSeq();
					}
					break;
				case REDUCE:
					{
					setState(1300);
					((QuerySpecificationContext)_localctx).kind = match(REDUCE);
					setState(1301);
					namedExpressionSeq();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				setState(1305);
				_la = _input.LA(1);
				if (_la==ROW) {
					{
					setState(1304);
					((QuerySpecificationContext)_localctx).inRowFormat = rowFormat();
					}
				}

				setState(1309);
				_la = _input.LA(1);
				if (_la==RECORDWRITER) {
					{
					setState(1307);
					match(RECORDWRITER);
					setState(1308);
					((QuerySpecificationContext)_localctx).recordWriter = match(STRING);
					}
				}

				setState(1311);
				match(USING);
				setState(1312);
				((QuerySpecificationContext)_localctx).script = match(STRING);
				setState(1325);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,165,_ctx) ) {
				case 1:
					{
					setState(1313);
					match(AS);
					setState(1323);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,164,_ctx) ) {
					case 1:
						{
						setState(1314);
						identifierSeq();
						}
						break;
					case 2:
						{
						setState(1315);
						colTypeList();
						}
						break;
					case 3:
						{
						{
						setState(1316);
						match(T__0);
						setState(1319);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,163,_ctx) ) {
						case 1:
							{
							setState(1317);
							identifierSeq();
							}
							break;
						case 2:
							{
							setState(1318);
							colTypeList();
							}
							break;
						}
						setState(1321);
						match(T__1);
						}
						}
						break;
					}
					}
					break;
				}
				setState(1328);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,166,_ctx) ) {
				case 1:
					{
					setState(1327);
					((QuerySpecificationContext)_localctx).outRowFormat = rowFormat();
					}
					break;
				}
				setState(1332);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,167,_ctx) ) {
				case 1:
					{
					setState(1330);
					match(RECORDREADER);
					setState(1331);
					((QuerySpecificationContext)_localctx).recordReader = match(STRING);
					}
					break;
				}
				setState(1335);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,168,_ctx) ) {
				case 1:
					{
					setState(1334);
					fromClause();
					}
					break;
				}
				setState(1339);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,169,_ctx) ) {
				case 1:
					{
					setState(1337);
					match(WHERE);
					setState(1338);
					((QuerySpecificationContext)_localctx).where = booleanExpression(0);
					}
					break;
				}
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1357);
				switch (_input.LA(1)) {
				case SELECT:
					{
					setState(1341);
					((QuerySpecificationContext)_localctx).kind = match(SELECT);
					setState(1343);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,170,_ctx) ) {
					case 1:
						{
						setState(1342);
						setQuantifier();
						}
						break;
					}
					setState(1345);
					namedExpressionSeq();
					setState(1347);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,171,_ctx) ) {
					case 1:
						{
						setState(1346);
						fromClause();
						}
						break;
					}
					}
					break;
				case FROM:
					{
					setState(1349);
					fromClause();
					setState(1355);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,173,_ctx) ) {
					case 1:
						{
						setState(1350);
						((QuerySpecificationContext)_localctx).kind = match(SELECT);
						setState(1352);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,172,_ctx) ) {
						case 1:
							{
							setState(1351);
							setQuantifier();
							}
							break;
						}
						setState(1354);
						namedExpressionSeq();
						}
						break;
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(1362);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,175,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1359);
						lateralView();
						}
						} 
					}
					setState(1364);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,175,_ctx);
				}
				setState(1367);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,176,_ctx) ) {
				case 1:
					{
					setState(1365);
					match(WHERE);
					setState(1366);
					((QuerySpecificationContext)_localctx).where = booleanExpression(0);
					}
					break;
				}
				setState(1370);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,177,_ctx) ) {
				case 1:
					{
					setState(1369);
					aggregation();
					}
					break;
				}
				setState(1374);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,178,_ctx) ) {
				case 1:
					{
					setState(1372);
					match(HAVING);
					setState(1373);
					((QuerySpecificationContext)_localctx).having = booleanExpression(0);
					}
					break;
				}
				setState(1377);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,179,_ctx) ) {
				case 1:
					{
					setState(1376);
					windows();
					}
					break;
				}
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FromClauseContext extends ParserRuleContext {
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public List<RelationContext> relation() {
			return getRuleContexts(RelationContext.class);
		}
		public RelationContext relation(int i) {
			return getRuleContext(RelationContext.class,i);
		}
		public List<LateralViewContext> lateralView() {
			return getRuleContexts(LateralViewContext.class);
		}
		public LateralViewContext lateralView(int i) {
			return getRuleContext(LateralViewContext.class,i);
		}
		public FromClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fromClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFromClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFromClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFromClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FromClauseContext fromClause() throws RecognitionException {
		FromClauseContext _localctx = new FromClauseContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_fromClause);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1381);
			match(FROM);
			setState(1382);
			relation();
			setState(1387);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,181,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1383);
					match(T__2);
					setState(1384);
					relation();
					}
					} 
				}
				setState(1389);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,181,_ctx);
			}
			setState(1393);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,182,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1390);
					lateralView();
					}
					} 
				}
				setState(1395);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,182,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AggregationContext extends ParserRuleContext {
		public ExpressionContext expression;
		public List<ExpressionContext> groupingExpressions = new ArrayList<ExpressionContext>();
		public Token kind;
		public TerminalNode GROUP() { return getToken(SqlBaseParser.GROUP, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public List<GroupingSetContext> groupingSet() {
			return getRuleContexts(GroupingSetContext.class);
		}
		public GroupingSetContext groupingSet(int i) {
			return getRuleContext(GroupingSetContext.class,i);
		}
		public TerminalNode ROLLUP() { return getToken(SqlBaseParser.ROLLUP, 0); }
		public TerminalNode CUBE() { return getToken(SqlBaseParser.CUBE, 0); }
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public AggregationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggregation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAggregation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAggregation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAggregation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AggregationContext aggregation() throws RecognitionException {
		AggregationContext _localctx = new AggregationContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_aggregation);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1396);
			match(GROUP);
			setState(1397);
			match(BY);
			setState(1398);
			((AggregationContext)_localctx).expression = expression();
			((AggregationContext)_localctx).groupingExpressions.add(((AggregationContext)_localctx).expression);
			setState(1403);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,183,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1399);
					match(T__2);
					setState(1400);
					((AggregationContext)_localctx).expression = expression();
					((AggregationContext)_localctx).groupingExpressions.add(((AggregationContext)_localctx).expression);
					}
					} 
				}
				setState(1405);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,183,_ctx);
			}
			setState(1423);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,185,_ctx) ) {
			case 1:
				{
				setState(1406);
				match(WITH);
				setState(1407);
				((AggregationContext)_localctx).kind = match(ROLLUP);
				}
				break;
			case 2:
				{
				setState(1408);
				match(WITH);
				setState(1409);
				((AggregationContext)_localctx).kind = match(CUBE);
				}
				break;
			case 3:
				{
				setState(1410);
				((AggregationContext)_localctx).kind = match(GROUPING);
				setState(1411);
				match(SETS);
				setState(1412);
				match(T__0);
				setState(1413);
				groupingSet();
				setState(1418);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1414);
					match(T__2);
					setState(1415);
					groupingSet();
					}
					}
					setState(1420);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1421);
				match(T__1);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroupingSetContext extends ParserRuleContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public GroupingSetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupingSet; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterGroupingSet(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitGroupingSet(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitGroupingSet(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupingSetContext groupingSet() throws RecognitionException {
		GroupingSetContext _localctx = new GroupingSetContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_groupingSet);
		int _la;
		try {
			setState(1438);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,188,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1425);
				match(T__0);
				setState(1434);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << LATERAL) | (1L << WINDOW) | (1L << OVER) | (1L << PARTITION) | (1L << RANGE) | (1L << ROWS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (FIRST - 64)) | (1L << (AFTER - 64)) | (1L << (LAST - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (SETMINUS - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IF - 64)) | (1L << (PLUS - 64)) | (1L << (MINUS - 64)) | (1L << (ASTERISK - 64)))) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & ((1L << (DIV - 130)) | (1L << (TILDE - 130)) | (1L << (PERCENTLIT - 130)) | (1L << (BUCKET - 130)) | (1L << (OUT - 130)) | (1L << (OF - 130)) | (1L << (SORT - 130)) | (1L << (CLUSTER - 130)) | (1L << (DISTRIBUTE - 130)) | (1L << (OVERWRITE - 130)) | (1L << (TRANSFORM - 130)) | (1L << (REDUCE - 130)) | (1L << (USING - 130)) | (1L << (SERDE - 130)) | (1L << (SERDEPROPERTIES - 130)) | (1L << (RECORDREADER - 130)) | (1L << (RECORDWRITER - 130)) | (1L << (DELIMITED - 130)) | (1L << (FIELDS - 130)) | (1L << (TERMINATED - 130)) | (1L << (COLLECTION - 130)) | (1L << (ITEMS - 130)) | (1L << (KEYS - 130)) | (1L << (ESCAPED - 130)) | (1L << (LINES - 130)) | (1L << (SEPARATED - 130)) | (1L << (FUNCTION - 130)) | (1L << (EXTENDED - 130)) | (1L << (REFRESH - 130)) | (1L << (CLEAR - 130)) | (1L << (CACHE - 130)) | (1L << (UNCACHE - 130)) | (1L << (LAZY - 130)) | (1L << (FORMATTED - 130)) | (1L << (GLOBAL - 130)) | (1L << (TEMPORARY - 130)) | (1L << (OPTIONS - 130)) | (1L << (UNSET - 130)) | (1L << (TBLPROPERTIES - 130)) | (1L << (DBPROPERTIES - 130)) | (1L << (BUCKETS - 130)) | (1L << (SKEWED - 130)) | (1L << (STORED - 130)) | (1L << (DIRECTORIES - 130)) | (1L << (LOCATION - 130)) | (1L << (EXCHANGE - 130)) | (1L << (ARCHIVE - 130)) | (1L << (UNARCHIVE - 130)) | (1L << (FILEFORMAT - 130)) | (1L << (TOUCH - 130)) | (1L << (COMPACT - 130)) | (1L << (CONCATENATE - 130)) | (1L << (CHANGE - 130)) | (1L << (CASCADE - 130)) | (1L << (RESTRICT - 130)) | (1L << (CLUSTERED - 130)) | (1L << (SORTED - 130)) | (1L << (PURGE - 130)) | (1L << (INPUTFORMAT - 130)) | (1L << (OUTPUTFORMAT - 130)) | (1L << (DATABASE - 130)))) != 0) || ((((_la - 194)) & ~0x3f) == 0 && ((1L << (_la - 194)) & ((1L << (DATABASES - 194)) | (1L << (DFS - 194)) | (1L << (TRUNCATE - 194)) | (1L << (ANALYZE - 194)) | (1L << (COMPUTE - 194)) | (1L << (LIST - 194)) | (1L << (STATISTICS - 194)) | (1L << (PARTITIONED - 194)) | (1L << (EXTERNAL - 194)) | (1L << (DEFINED - 194)) | (1L << (REVOKE - 194)) | (1L << (GRANT - 194)) | (1L << (LOCK - 194)) | (1L << (UNLOCK - 194)) | (1L << (MSCK - 194)) | (1L << (REPAIR - 194)) | (1L << (RECOVER - 194)) | (1L << (EXPORT - 194)) | (1L << (IMPORT - 194)) | (1L << (LOAD - 194)) | (1L << (ROLE - 194)) | (1L << (ROLES - 194)) | (1L << (COMPACTIONS - 194)) | (1L << (PRINCIPALS - 194)) | (1L << (TRANSACTIONS - 194)) | (1L << (INDEX - 194)) | (1L << (INDEXES - 194)) | (1L << (LOCKS - 194)) | (1L << (OPTION - 194)) | (1L << (ANTI - 194)) | (1L << (LOCAL - 194)) | (1L << (INPATH - 194)) | (1L << (CURRENT_DATE - 194)) | (1L << (CURRENT_TIMESTAMP - 194)) | (1L << (STRING - 194)) | (1L << (BIGINT_LITERAL - 194)) | (1L << (SMALLINT_LITERAL - 194)) | (1L << (TINYINT_LITERAL - 194)) | (1L << (INTEGER_VALUE - 194)) | (1L << (DECIMAL_VALUE - 194)) | (1L << (DOUBLE_LITERAL - 194)) | (1L << (BIGDECIMAL_LITERAL - 194)) | (1L << (IDENTIFIER - 194)) | (1L << (BACKQUOTED_IDENTIFIER - 194)))) != 0)) {
					{
					setState(1426);
					expression();
					setState(1431);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(1427);
						match(T__2);
						setState(1428);
						expression();
						}
						}
						setState(1433);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1436);
				match(T__1);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1437);
				expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LateralViewContext extends ParserRuleContext {
		public IdentifierContext tblName;
		public IdentifierContext identifier;
		public List<IdentifierContext> colName = new ArrayList<IdentifierContext>();
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public LateralViewContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_lateralView; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLateralView(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLateralView(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLateralView(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LateralViewContext lateralView() throws RecognitionException {
		LateralViewContext _localctx = new LateralViewContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_lateralView);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1440);
			match(LATERAL);
			setState(1441);
			match(VIEW);
			setState(1443);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,189,_ctx) ) {
			case 1:
				{
				setState(1442);
				match(OUTER);
				}
				break;
			}
			setState(1445);
			qualifiedName();
			setState(1446);
			match(T__0);
			setState(1455);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << LATERAL) | (1L << WINDOW) | (1L << OVER) | (1L << PARTITION) | (1L << RANGE) | (1L << ROWS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (FIRST - 64)) | (1L << (AFTER - 64)) | (1L << (LAST - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (SETMINUS - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IF - 64)) | (1L << (PLUS - 64)) | (1L << (MINUS - 64)) | (1L << (ASTERISK - 64)))) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & ((1L << (DIV - 130)) | (1L << (TILDE - 130)) | (1L << (PERCENTLIT - 130)) | (1L << (BUCKET - 130)) | (1L << (OUT - 130)) | (1L << (OF - 130)) | (1L << (SORT - 130)) | (1L << (CLUSTER - 130)) | (1L << (DISTRIBUTE - 130)) | (1L << (OVERWRITE - 130)) | (1L << (TRANSFORM - 130)) | (1L << (REDUCE - 130)) | (1L << (USING - 130)) | (1L << (SERDE - 130)) | (1L << (SERDEPROPERTIES - 130)) | (1L << (RECORDREADER - 130)) | (1L << (RECORDWRITER - 130)) | (1L << (DELIMITED - 130)) | (1L << (FIELDS - 130)) | (1L << (TERMINATED - 130)) | (1L << (COLLECTION - 130)) | (1L << (ITEMS - 130)) | (1L << (KEYS - 130)) | (1L << (ESCAPED - 130)) | (1L << (LINES - 130)) | (1L << (SEPARATED - 130)) | (1L << (FUNCTION - 130)) | (1L << (EXTENDED - 130)) | (1L << (REFRESH - 130)) | (1L << (CLEAR - 130)) | (1L << (CACHE - 130)) | (1L << (UNCACHE - 130)) | (1L << (LAZY - 130)) | (1L << (FORMATTED - 130)) | (1L << (GLOBAL - 130)) | (1L << (TEMPORARY - 130)) | (1L << (OPTIONS - 130)) | (1L << (UNSET - 130)) | (1L << (TBLPROPERTIES - 130)) | (1L << (DBPROPERTIES - 130)) | (1L << (BUCKETS - 130)) | (1L << (SKEWED - 130)) | (1L << (STORED - 130)) | (1L << (DIRECTORIES - 130)) | (1L << (LOCATION - 130)) | (1L << (EXCHANGE - 130)) | (1L << (ARCHIVE - 130)) | (1L << (UNARCHIVE - 130)) | (1L << (FILEFORMAT - 130)) | (1L << (TOUCH - 130)) | (1L << (COMPACT - 130)) | (1L << (CONCATENATE - 130)) | (1L << (CHANGE - 130)) | (1L << (CASCADE - 130)) | (1L << (RESTRICT - 130)) | (1L << (CLUSTERED - 130)) | (1L << (SORTED - 130)) | (1L << (PURGE - 130)) | (1L << (INPUTFORMAT - 130)) | (1L << (OUTPUTFORMAT - 130)) | (1L << (DATABASE - 130)))) != 0) || ((((_la - 194)) & ~0x3f) == 0 && ((1L << (_la - 194)) & ((1L << (DATABASES - 194)) | (1L << (DFS - 194)) | (1L << (TRUNCATE - 194)) | (1L << (ANALYZE - 194)) | (1L << (COMPUTE - 194)) | (1L << (LIST - 194)) | (1L << (STATISTICS - 194)) | (1L << (PARTITIONED - 194)) | (1L << (EXTERNAL - 194)) | (1L << (DEFINED - 194)) | (1L << (REVOKE - 194)) | (1L << (GRANT - 194)) | (1L << (LOCK - 194)) | (1L << (UNLOCK - 194)) | (1L << (MSCK - 194)) | (1L << (REPAIR - 194)) | (1L << (RECOVER - 194)) | (1L << (EXPORT - 194)) | (1L << (IMPORT - 194)) | (1L << (LOAD - 194)) | (1L << (ROLE - 194)) | (1L << (ROLES - 194)) | (1L << (COMPACTIONS - 194)) | (1L << (PRINCIPALS - 194)) | (1L << (TRANSACTIONS - 194)) | (1L << (INDEX - 194)) | (1L << (INDEXES - 194)) | (1L << (LOCKS - 194)) | (1L << (OPTION - 194)) | (1L << (ANTI - 194)) | (1L << (LOCAL - 194)) | (1L << (INPATH - 194)) | (1L << (CURRENT_DATE - 194)) | (1L << (CURRENT_TIMESTAMP - 194)) | (1L << (STRING - 194)) | (1L << (BIGINT_LITERAL - 194)) | (1L << (SMALLINT_LITERAL - 194)) | (1L << (TINYINT_LITERAL - 194)) | (1L << (INTEGER_VALUE - 194)) | (1L << (DECIMAL_VALUE - 194)) | (1L << (DOUBLE_LITERAL - 194)) | (1L << (BIGDECIMAL_LITERAL - 194)) | (1L << (IDENTIFIER - 194)) | (1L << (BACKQUOTED_IDENTIFIER - 194)))) != 0)) {
				{
				setState(1447);
				expression();
				setState(1452);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1448);
					match(T__2);
					setState(1449);
					expression();
					}
					}
					setState(1454);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1457);
			match(T__1);
			setState(1458);
			((LateralViewContext)_localctx).tblName = identifier();
			setState(1470);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,194,_ctx) ) {
			case 1:
				{
				setState(1460);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,192,_ctx) ) {
				case 1:
					{
					setState(1459);
					match(AS);
					}
					break;
				}
				setState(1462);
				((LateralViewContext)_localctx).identifier = identifier();
				((LateralViewContext)_localctx).colName.add(((LateralViewContext)_localctx).identifier);
				setState(1467);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,193,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1463);
						match(T__2);
						setState(1464);
						((LateralViewContext)_localctx).identifier = identifier();
						((LateralViewContext)_localctx).colName.add(((LateralViewContext)_localctx).identifier);
						}
						} 
					}
					setState(1469);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,193,_ctx);
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SetQuantifierContext extends ParserRuleContext {
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public SetQuantifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setQuantifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSetQuantifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSetQuantifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSetQuantifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SetQuantifierContext setQuantifier() throws RecognitionException {
		SetQuantifierContext _localctx = new SetQuantifierContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_setQuantifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1472);
			_la = _input.LA(1);
			if ( !(_la==ALL || _la==DISTINCT) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationContext extends ParserRuleContext {
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public List<JoinRelationContext> joinRelation() {
			return getRuleContexts(JoinRelationContext.class);
		}
		public JoinRelationContext joinRelation(int i) {
			return getRuleContext(JoinRelationContext.class,i);
		}
		public RelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationContext relation() throws RecognitionException {
		RelationContext _localctx = new RelationContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_relation);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1474);
			relationPrimary();
			setState(1478);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,195,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1475);
					joinRelation();
					}
					} 
				}
				setState(1480);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,195,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinRelationContext extends ParserRuleContext {
		public RelationPrimaryContext right;
		public TerminalNode JOIN() { return getToken(SqlBaseParser.JOIN, 0); }
		public RelationPrimaryContext relationPrimary() {
			return getRuleContext(RelationPrimaryContext.class,0);
		}
		public JoinTypeContext joinType() {
			return getRuleContext(JoinTypeContext.class,0);
		}
		public JoinCriteriaContext joinCriteria() {
			return getRuleContext(JoinCriteriaContext.class,0);
		}
		public TerminalNode NATURAL() { return getToken(SqlBaseParser.NATURAL, 0); }
		public JoinRelationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinRelation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinRelation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinRelationContext joinRelation() throws RecognitionException {
		JoinRelationContext _localctx = new JoinRelationContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_joinRelation);
		try {
			setState(1492);
			switch (_input.LA(1)) {
			case JOIN:
			case CROSS:
			case INNER:
			case LEFT:
			case RIGHT:
			case FULL:
			case ANTI:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(1481);
				joinType();
				}
				setState(1482);
				match(JOIN);
				setState(1483);
				((JoinRelationContext)_localctx).right = relationPrimary();
				setState(1485);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,196,_ctx) ) {
				case 1:
					{
					setState(1484);
					joinCriteria();
					}
					break;
				}
				}
				break;
			case NATURAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(1487);
				match(NATURAL);
				setState(1488);
				joinType();
				setState(1489);
				match(JOIN);
				setState(1490);
				((JoinRelationContext)_localctx).right = relationPrimary();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinTypeContext extends ParserRuleContext {
		public TerminalNode INNER() { return getToken(SqlBaseParser.INNER, 0); }
		public TerminalNode CROSS() { return getToken(SqlBaseParser.CROSS, 0); }
		public TerminalNode LEFT() { return getToken(SqlBaseParser.LEFT, 0); }
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public TerminalNode SEMI() { return getToken(SqlBaseParser.SEMI, 0); }
		public TerminalNode RIGHT() { return getToken(SqlBaseParser.RIGHT, 0); }
		public TerminalNode FULL() { return getToken(SqlBaseParser.FULL, 0); }
		public TerminalNode ANTI() { return getToken(SqlBaseParser.ANTI, 0); }
		public JoinTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinTypeContext joinType() throws RecognitionException {
		JoinTypeContext _localctx = new JoinTypeContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_joinType);
		int _la;
		try {
			setState(1516);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,203,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1495);
				_la = _input.LA(1);
				if (_la==INNER) {
					{
					setState(1494);
					match(INNER);
					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1497);
				match(CROSS);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1498);
				match(LEFT);
				setState(1500);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1499);
					match(OUTER);
					}
				}

				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1502);
				match(LEFT);
				setState(1503);
				match(SEMI);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1504);
				match(RIGHT);
				setState(1506);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1505);
					match(OUTER);
					}
				}

				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(1508);
				match(FULL);
				setState(1510);
				_la = _input.LA(1);
				if (_la==OUTER) {
					{
					setState(1509);
					match(OUTER);
					}
				}

				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(1513);
				_la = _input.LA(1);
				if (_la==LEFT) {
					{
					setState(1512);
					match(LEFT);
					}
				}

				setState(1515);
				match(ANTI);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinCriteriaContext extends ParserRuleContext {
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public JoinCriteriaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinCriteria; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterJoinCriteria(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitJoinCriteria(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitJoinCriteria(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinCriteriaContext joinCriteria() throws RecognitionException {
		JoinCriteriaContext _localctx = new JoinCriteriaContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_joinCriteria);
		int _la;
		try {
			setState(1532);
			switch (_input.LA(1)) {
			case ON:
				enterOuterAlt(_localctx, 1);
				{
				setState(1518);
				match(ON);
				setState(1519);
				booleanExpression(0);
				}
				break;
			case USING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1520);
				match(USING);
				setState(1521);
				match(T__0);
				setState(1522);
				identifier();
				setState(1527);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1523);
					match(T__2);
					setState(1524);
					identifier();
					}
					}
					setState(1529);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1530);
				match(T__1);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SampleContext extends ParserRuleContext {
		public Token percentage;
		public Token sampleType;
		public Token numerator;
		public Token denominator;
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public TerminalNode BYTELENGTH_LITERAL() { return getToken(SqlBaseParser.BYTELENGTH_LITERAL, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode OUT() { return getToken(SqlBaseParser.OUT, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public TerminalNode PERCENTLIT() { return getToken(SqlBaseParser.PERCENTLIT, 0); }
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode BUCKET() { return getToken(SqlBaseParser.BUCKET, 0); }
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(SqlBaseParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(SqlBaseParser.INTEGER_VALUE, i);
		}
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public SampleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sample; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSample(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSample(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSample(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SampleContext sample() throws RecognitionException {
		SampleContext _localctx = new SampleContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_sample);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1534);
			match(TABLESAMPLE);
			setState(1535);
			match(T__0);
			setState(1557);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,208,_ctx) ) {
			case 1:
				{
				{
				setState(1536);
				((SampleContext)_localctx).percentage = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==INTEGER_VALUE || _la==DECIMAL_VALUE) ) {
					((SampleContext)_localctx).percentage = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(1537);
				((SampleContext)_localctx).sampleType = match(PERCENTLIT);
				}
				}
				break;
			case 2:
				{
				{
				setState(1538);
				expression();
				setState(1539);
				((SampleContext)_localctx).sampleType = match(ROWS);
				}
				}
				break;
			case 3:
				{
				setState(1541);
				((SampleContext)_localctx).sampleType = match(BYTELENGTH_LITERAL);
				}
				break;
			case 4:
				{
				{
				setState(1542);
				((SampleContext)_localctx).sampleType = match(BUCKET);
				setState(1543);
				((SampleContext)_localctx).numerator = match(INTEGER_VALUE);
				setState(1544);
				match(OUT);
				setState(1545);
				match(OF);
				setState(1546);
				((SampleContext)_localctx).denominator = match(INTEGER_VALUE);
				setState(1555);
				_la = _input.LA(1);
				if (_la==ON) {
					{
					setState(1547);
					match(ON);
					setState(1553);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,206,_ctx) ) {
					case 1:
						{
						setState(1548);
						identifier();
						}
						break;
					case 2:
						{
						setState(1549);
						qualifiedName();
						setState(1550);
						match(T__0);
						setState(1551);
						match(T__1);
						}
						break;
					}
					}
				}

				}
				}
				break;
			}
			setState(1559);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierListContext extends ParserRuleContext {
		public IdentifierSeqContext identifierSeq() {
			return getRuleContext(IdentifierSeqContext.class,0);
		}
		public IdentifierListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifierList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifierList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIdentifierList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierListContext identifierList() throws RecognitionException {
		IdentifierListContext _localctx = new IdentifierListContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_identifierList);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1561);
			match(T__0);
			setState(1562);
			identifierSeq();
			setState(1563);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierSeqContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public IdentifierSeqContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierSeq; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifierSeq(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifierSeq(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIdentifierSeq(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierSeqContext identifierSeq() throws RecognitionException {
		IdentifierSeqContext _localctx = new IdentifierSeqContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_identifierSeq);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1565);
			identifier();
			setState(1570);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,209,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1566);
					match(T__2);
					setState(1567);
					identifier();
					}
					} 
				}
				setState(1572);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,209,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OrderedIdentifierListContext extends ParserRuleContext {
		public List<OrderedIdentifierContext> orderedIdentifier() {
			return getRuleContexts(OrderedIdentifierContext.class);
		}
		public OrderedIdentifierContext orderedIdentifier(int i) {
			return getRuleContext(OrderedIdentifierContext.class,i);
		}
		public OrderedIdentifierListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderedIdentifierList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterOrderedIdentifierList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitOrderedIdentifierList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitOrderedIdentifierList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OrderedIdentifierListContext orderedIdentifierList() throws RecognitionException {
		OrderedIdentifierListContext _localctx = new OrderedIdentifierListContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_orderedIdentifierList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1573);
			match(T__0);
			setState(1574);
			orderedIdentifier();
			setState(1579);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1575);
				match(T__2);
				setState(1576);
				orderedIdentifier();
				}
				}
				setState(1581);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1582);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OrderedIdentifierContext extends ParserRuleContext {
		public Token ordering;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public OrderedIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderedIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterOrderedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitOrderedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitOrderedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OrderedIdentifierContext orderedIdentifier() throws RecognitionException {
		OrderedIdentifierContext _localctx = new OrderedIdentifierContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_orderedIdentifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1584);
			identifier();
			setState(1586);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(1585);
				((OrderedIdentifierContext)_localctx).ordering = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
					((OrderedIdentifierContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierCommentListContext extends ParserRuleContext {
		public List<IdentifierCommentContext> identifierComment() {
			return getRuleContexts(IdentifierCommentContext.class);
		}
		public IdentifierCommentContext identifierComment(int i) {
			return getRuleContext(IdentifierCommentContext.class,i);
		}
		public IdentifierCommentListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierCommentList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifierCommentList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifierCommentList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIdentifierCommentList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierCommentListContext identifierCommentList() throws RecognitionException {
		IdentifierCommentListContext _localctx = new IdentifierCommentListContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_identifierCommentList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1588);
			match(T__0);
			setState(1589);
			identifierComment();
			setState(1594);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(1590);
				match(T__2);
				setState(1591);
				identifierComment();
				}
				}
				setState(1596);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1597);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierCommentContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public IdentifierCommentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierComment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifierComment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifierComment(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIdentifierComment(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierCommentContext identifierComment() throws RecognitionException {
		IdentifierCommentContext _localctx = new IdentifierCommentContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_identifierComment);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1599);
			identifier();
			setState(1602);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(1600);
				match(COMMENT);
				setState(1601);
				match(STRING);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationPrimaryContext extends ParserRuleContext {
		public RelationPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationPrimary; }
	 
		public RelationPrimaryContext() { }
		public void copyFrom(RelationPrimaryContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class InlineTableDefault2Context extends RelationPrimaryContext {
		public InlineTableContext inlineTable() {
			return getRuleContext(InlineTableContext.class,0);
		}
		public InlineTableDefault2Context(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInlineTableDefault2(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInlineTableDefault2(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInlineTableDefault2(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AliasedQueryContext extends RelationPrimaryContext {
		public QueryNoWithContext queryNoWith() {
			return getRuleContext(QueryNoWithContext.class,0);
		}
		public SampleContext sample() {
			return getRuleContext(SampleContext.class,0);
		}
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public AliasedQueryContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAliasedQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAliasedQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAliasedQuery(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class AliasedRelationContext extends RelationPrimaryContext {
		public RelationContext relation() {
			return getRuleContext(RelationContext.class,0);
		}
		public SampleContext sample() {
			return getRuleContext(SampleContext.class,0);
		}
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public AliasedRelationContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterAliasedRelation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitAliasedRelation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitAliasedRelation(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TableNameContext extends RelationPrimaryContext {
		public TableIdentifierContext tableIdentifier() {
			return getRuleContext(TableIdentifierContext.class,0);
		}
		public SampleContext sample() {
			return getRuleContext(SampleContext.class,0);
		}
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TableNameContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableName(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TableValuedFunctionContext extends RelationPrimaryContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TableValuedFunctionContext(RelationPrimaryContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableValuedFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableValuedFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableValuedFunction(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationPrimaryContext relationPrimary() throws RecognitionException {
		RelationPrimaryContext _localctx = new RelationPrimaryContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_relationPrimary);
		int _la;
		try {
			setState(1653);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,225,_ctx) ) {
			case 1:
				_localctx = new TableNameContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1604);
				tableIdentifier();
				setState(1606);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,214,_ctx) ) {
				case 1:
					{
					setState(1605);
					sample();
					}
					break;
				}
				setState(1612);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,216,_ctx) ) {
				case 1:
					{
					setState(1609);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,215,_ctx) ) {
					case 1:
						{
						setState(1608);
						match(AS);
						}
						break;
					}
					setState(1611);
					strictIdentifier();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new AliasedQueryContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1614);
				match(T__0);
				setState(1615);
				queryNoWith();
				setState(1616);
				match(T__1);
				setState(1618);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,217,_ctx) ) {
				case 1:
					{
					setState(1617);
					sample();
					}
					break;
				}
				setState(1624);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,219,_ctx) ) {
				case 1:
					{
					setState(1621);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,218,_ctx) ) {
					case 1:
						{
						setState(1620);
						match(AS);
						}
						break;
					}
					setState(1623);
					strictIdentifier();
					}
					break;
				}
				}
				break;
			case 3:
				_localctx = new AliasedRelationContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1626);
				match(T__0);
				setState(1627);
				relation();
				setState(1628);
				match(T__1);
				setState(1630);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,220,_ctx) ) {
				case 1:
					{
					setState(1629);
					sample();
					}
					break;
				}
				setState(1636);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,222,_ctx) ) {
				case 1:
					{
					setState(1633);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,221,_ctx) ) {
					case 1:
						{
						setState(1632);
						match(AS);
						}
						break;
					}
					setState(1635);
					strictIdentifier();
					}
					break;
				}
				}
				break;
			case 4:
				_localctx = new InlineTableDefault2Context(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1638);
				inlineTable();
				}
				break;
			case 5:
				_localctx = new TableValuedFunctionContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1639);
				identifier();
				setState(1640);
				match(T__0);
				setState(1649);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << LATERAL) | (1L << WINDOW) | (1L << OVER) | (1L << PARTITION) | (1L << RANGE) | (1L << ROWS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (FIRST - 64)) | (1L << (AFTER - 64)) | (1L << (LAST - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (SETMINUS - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IF - 64)) | (1L << (PLUS - 64)) | (1L << (MINUS - 64)) | (1L << (ASTERISK - 64)))) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & ((1L << (DIV - 130)) | (1L << (TILDE - 130)) | (1L << (PERCENTLIT - 130)) | (1L << (BUCKET - 130)) | (1L << (OUT - 130)) | (1L << (OF - 130)) | (1L << (SORT - 130)) | (1L << (CLUSTER - 130)) | (1L << (DISTRIBUTE - 130)) | (1L << (OVERWRITE - 130)) | (1L << (TRANSFORM - 130)) | (1L << (REDUCE - 130)) | (1L << (USING - 130)) | (1L << (SERDE - 130)) | (1L << (SERDEPROPERTIES - 130)) | (1L << (RECORDREADER - 130)) | (1L << (RECORDWRITER - 130)) | (1L << (DELIMITED - 130)) | (1L << (FIELDS - 130)) | (1L << (TERMINATED - 130)) | (1L << (COLLECTION - 130)) | (1L << (ITEMS - 130)) | (1L << (KEYS - 130)) | (1L << (ESCAPED - 130)) | (1L << (LINES - 130)) | (1L << (SEPARATED - 130)) | (1L << (FUNCTION - 130)) | (1L << (EXTENDED - 130)) | (1L << (REFRESH - 130)) | (1L << (CLEAR - 130)) | (1L << (CACHE - 130)) | (1L << (UNCACHE - 130)) | (1L << (LAZY - 130)) | (1L << (FORMATTED - 130)) | (1L << (GLOBAL - 130)) | (1L << (TEMPORARY - 130)) | (1L << (OPTIONS - 130)) | (1L << (UNSET - 130)) | (1L << (TBLPROPERTIES - 130)) | (1L << (DBPROPERTIES - 130)) | (1L << (BUCKETS - 130)) | (1L << (SKEWED - 130)) | (1L << (STORED - 130)) | (1L << (DIRECTORIES - 130)) | (1L << (LOCATION - 130)) | (1L << (EXCHANGE - 130)) | (1L << (ARCHIVE - 130)) | (1L << (UNARCHIVE - 130)) | (1L << (FILEFORMAT - 130)) | (1L << (TOUCH - 130)) | (1L << (COMPACT - 130)) | (1L << (CONCATENATE - 130)) | (1L << (CHANGE - 130)) | (1L << (CASCADE - 130)) | (1L << (RESTRICT - 130)) | (1L << (CLUSTERED - 130)) | (1L << (SORTED - 130)) | (1L << (PURGE - 130)) | (1L << (INPUTFORMAT - 130)) | (1L << (OUTPUTFORMAT - 130)) | (1L << (DATABASE - 130)))) != 0) || ((((_la - 194)) & ~0x3f) == 0 && ((1L << (_la - 194)) & ((1L << (DATABASES - 194)) | (1L << (DFS - 194)) | (1L << (TRUNCATE - 194)) | (1L << (ANALYZE - 194)) | (1L << (COMPUTE - 194)) | (1L << (LIST - 194)) | (1L << (STATISTICS - 194)) | (1L << (PARTITIONED - 194)) | (1L << (EXTERNAL - 194)) | (1L << (DEFINED - 194)) | (1L << (REVOKE - 194)) | (1L << (GRANT - 194)) | (1L << (LOCK - 194)) | (1L << (UNLOCK - 194)) | (1L << (MSCK - 194)) | (1L << (REPAIR - 194)) | (1L << (RECOVER - 194)) | (1L << (EXPORT - 194)) | (1L << (IMPORT - 194)) | (1L << (LOAD - 194)) | (1L << (ROLE - 194)) | (1L << (ROLES - 194)) | (1L << (COMPACTIONS - 194)) | (1L << (PRINCIPALS - 194)) | (1L << (TRANSACTIONS - 194)) | (1L << (INDEX - 194)) | (1L << (INDEXES - 194)) | (1L << (LOCKS - 194)) | (1L << (OPTION - 194)) | (1L << (ANTI - 194)) | (1L << (LOCAL - 194)) | (1L << (INPATH - 194)) | (1L << (CURRENT_DATE - 194)) | (1L << (CURRENT_TIMESTAMP - 194)) | (1L << (STRING - 194)) | (1L << (BIGINT_LITERAL - 194)) | (1L << (SMALLINT_LITERAL - 194)) | (1L << (TINYINT_LITERAL - 194)) | (1L << (INTEGER_VALUE - 194)) | (1L << (DECIMAL_VALUE - 194)) | (1L << (DOUBLE_LITERAL - 194)) | (1L << (BIGDECIMAL_LITERAL - 194)) | (1L << (IDENTIFIER - 194)) | (1L << (BACKQUOTED_IDENTIFIER - 194)))) != 0)) {
					{
					setState(1641);
					expression();
					setState(1646);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(1642);
						match(T__2);
						setState(1643);
						expression();
						}
						}
						setState(1648);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1651);
				match(T__1);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InlineTableContext extends ParserRuleContext {
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public InlineTableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_inlineTable; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInlineTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInlineTable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInlineTable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InlineTableContext inlineTable() throws RecognitionException {
		InlineTableContext _localctx = new InlineTableContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_inlineTable);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1655);
			match(VALUES);
			setState(1656);
			expression();
			setState(1661);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,226,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1657);
					match(T__2);
					setState(1658);
					expression();
					}
					} 
				}
				setState(1663);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,226,_ctx);
			}
			setState(1671);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,229,_ctx) ) {
			case 1:
				{
				setState(1665);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,227,_ctx) ) {
				case 1:
					{
					setState(1664);
					match(AS);
					}
					break;
				}
				setState(1667);
				identifier();
				setState(1669);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,228,_ctx) ) {
				case 1:
					{
					setState(1668);
					identifierList();
					}
					break;
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RowFormatContext extends ParserRuleContext {
		public RowFormatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rowFormat; }
	 
		public RowFormatContext() { }
		public void copyFrom(RowFormatContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class RowFormatSerdeContext extends RowFormatContext {
		public Token name;
		public TablePropertyListContext props;
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TerminalNode WITH() { return getToken(SqlBaseParser.WITH, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TablePropertyListContext tablePropertyList() {
			return getRuleContext(TablePropertyListContext.class,0);
		}
		public RowFormatSerdeContext(RowFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowFormatSerde(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowFormatSerde(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRowFormatSerde(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RowFormatDelimitedContext extends RowFormatContext {
		public Token fieldsTerminatedBy;
		public Token escapedBy;
		public Token collectionItemsTerminatedBy;
		public Token keysTerminatedBy;
		public Token linesSeparatedBy;
		public Token nullDefinedAs;
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode DELIMITED() { return getToken(SqlBaseParser.DELIMITED, 0); }
		public TerminalNode FIELDS() { return getToken(SqlBaseParser.FIELDS, 0); }
		public List<TerminalNode> TERMINATED() { return getTokens(SqlBaseParser.TERMINATED); }
		public TerminalNode TERMINATED(int i) {
			return getToken(SqlBaseParser.TERMINATED, i);
		}
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public TerminalNode COLLECTION() { return getToken(SqlBaseParser.COLLECTION, 0); }
		public TerminalNode ITEMS() { return getToken(SqlBaseParser.ITEMS, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode KEYS() { return getToken(SqlBaseParser.KEYS, 0); }
		public TerminalNode LINES() { return getToken(SqlBaseParser.LINES, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode DEFINED() { return getToken(SqlBaseParser.DEFINED, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public TerminalNode ESCAPED() { return getToken(SqlBaseParser.ESCAPED, 0); }
		public RowFormatDelimitedContext(RowFormatContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowFormatDelimited(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowFormatDelimited(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRowFormatDelimited(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RowFormatContext rowFormat() throws RecognitionException {
		RowFormatContext _localctx = new RowFormatContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_rowFormat);
		try {
			setState(1722);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,237,_ctx) ) {
			case 1:
				_localctx = new RowFormatSerdeContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1673);
				match(ROW);
				setState(1674);
				match(FORMAT);
				setState(1675);
				match(SERDE);
				setState(1676);
				((RowFormatSerdeContext)_localctx).name = match(STRING);
				setState(1680);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,230,_ctx) ) {
				case 1:
					{
					setState(1677);
					match(WITH);
					setState(1678);
					match(SERDEPROPERTIES);
					setState(1679);
					((RowFormatSerdeContext)_localctx).props = tablePropertyList();
					}
					break;
				}
				}
				break;
			case 2:
				_localctx = new RowFormatDelimitedContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1682);
				match(ROW);
				setState(1683);
				match(FORMAT);
				setState(1684);
				match(DELIMITED);
				setState(1694);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,232,_ctx) ) {
				case 1:
					{
					setState(1685);
					match(FIELDS);
					setState(1686);
					match(TERMINATED);
					setState(1687);
					match(BY);
					setState(1688);
					((RowFormatDelimitedContext)_localctx).fieldsTerminatedBy = match(STRING);
					setState(1692);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,231,_ctx) ) {
					case 1:
						{
						setState(1689);
						match(ESCAPED);
						setState(1690);
						match(BY);
						setState(1691);
						((RowFormatDelimitedContext)_localctx).escapedBy = match(STRING);
						}
						break;
					}
					}
					break;
				}
				setState(1701);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,233,_ctx) ) {
				case 1:
					{
					setState(1696);
					match(COLLECTION);
					setState(1697);
					match(ITEMS);
					setState(1698);
					match(TERMINATED);
					setState(1699);
					match(BY);
					setState(1700);
					((RowFormatDelimitedContext)_localctx).collectionItemsTerminatedBy = match(STRING);
					}
					break;
				}
				setState(1708);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,234,_ctx) ) {
				case 1:
					{
					setState(1703);
					match(MAP);
					setState(1704);
					match(KEYS);
					setState(1705);
					match(TERMINATED);
					setState(1706);
					match(BY);
					setState(1707);
					((RowFormatDelimitedContext)_localctx).keysTerminatedBy = match(STRING);
					}
					break;
				}
				setState(1714);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,235,_ctx) ) {
				case 1:
					{
					setState(1710);
					match(LINES);
					setState(1711);
					match(TERMINATED);
					setState(1712);
					match(BY);
					setState(1713);
					((RowFormatDelimitedContext)_localctx).linesSeparatedBy = match(STRING);
					}
					break;
				}
				setState(1720);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,236,_ctx) ) {
				case 1:
					{
					setState(1716);
					match(NULL);
					setState(1717);
					match(DEFINED);
					setState(1718);
					match(AS);
					setState(1719);
					((RowFormatDelimitedContext)_localctx).nullDefinedAs = match(STRING);
					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableIdentifierContext extends ParserRuleContext {
		public IdentifierContext db;
		public IdentifierContext table;
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TableIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tableIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTableIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTableIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTableIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TableIdentifierContext tableIdentifier() throws RecognitionException {
		TableIdentifierContext _localctx = new TableIdentifierContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_tableIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1727);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,238,_ctx) ) {
			case 1:
				{
				setState(1724);
				((TableIdentifierContext)_localctx).db = identifier();
				setState(1725);
				match(T__3);
				}
				break;
			}
			setState(1729);
			((TableIdentifierContext)_localctx).table = identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamedExpressionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public IdentifierListContext identifierList() {
			return getRuleContext(IdentifierListContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public NamedExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedExpressionContext namedExpression() throws RecognitionException {
		NamedExpressionContext _localctx = new NamedExpressionContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_namedExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1731);
			expression();
			setState(1739);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,241,_ctx) ) {
			case 1:
				{
				setState(1733);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,239,_ctx) ) {
				case 1:
					{
					setState(1732);
					match(AS);
					}
					break;
				}
				setState(1737);
				switch (_input.LA(1)) {
				case SELECT:
				case FROM:
				case ADD:
				case AS:
				case ALL:
				case DISTINCT:
				case WHERE:
				case GROUP:
				case BY:
				case GROUPING:
				case SETS:
				case CUBE:
				case ROLLUP:
				case ORDER:
				case HAVING:
				case LIMIT:
				case AT:
				case OR:
				case AND:
				case IN:
				case NOT:
				case NO:
				case EXISTS:
				case BETWEEN:
				case LIKE:
				case RLIKE:
				case IS:
				case NULL:
				case TRUE:
				case FALSE:
				case NULLS:
				case ASC:
				case DESC:
				case FOR:
				case INTERVAL:
				case CASE:
				case WHEN:
				case THEN:
				case ELSE:
				case END:
				case JOIN:
				case CROSS:
				case OUTER:
				case INNER:
				case LEFT:
				case SEMI:
				case RIGHT:
				case FULL:
				case NATURAL:
				case ON:
				case LATERAL:
				case WINDOW:
				case OVER:
				case PARTITION:
				case RANGE:
				case ROWS:
				case UNBOUNDED:
				case PRECEDING:
				case FOLLOWING:
				case CURRENT:
				case FIRST:
				case AFTER:
				case LAST:
				case ROW:
				case WITH:
				case VALUES:
				case CREATE:
				case TABLE:
				case VIEW:
				case REPLACE:
				case INSERT:
				case DELETE:
				case INTO:
				case DESCRIBE:
				case EXPLAIN:
				case FORMAT:
				case LOGICAL:
				case CODEGEN:
				case CAST:
				case SHOW:
				case TABLES:
				case COLUMNS:
				case COLUMN:
				case USE:
				case PARTITIONS:
				case FUNCTIONS:
				case DROP:
				case UNION:
				case EXCEPT:
				case SETMINUS:
				case INTERSECT:
				case TO:
				case TABLESAMPLE:
				case STRATIFY:
				case ALTER:
				case RENAME:
				case ARRAY:
				case MAP:
				case STRUCT:
				case COMMENT:
				case SET:
				case RESET:
				case DATA:
				case START:
				case TRANSACTION:
				case COMMIT:
				case ROLLBACK:
				case MACRO:
				case IF:
				case DIV:
				case PERCENTLIT:
				case BUCKET:
				case OUT:
				case OF:
				case SORT:
				case CLUSTER:
				case DISTRIBUTE:
				case OVERWRITE:
				case TRANSFORM:
				case REDUCE:
				case USING:
				case SERDE:
				case SERDEPROPERTIES:
				case RECORDREADER:
				case RECORDWRITER:
				case DELIMITED:
				case FIELDS:
				case TERMINATED:
				case COLLECTION:
				case ITEMS:
				case KEYS:
				case ESCAPED:
				case LINES:
				case SEPARATED:
				case FUNCTION:
				case EXTENDED:
				case REFRESH:
				case CLEAR:
				case CACHE:
				case UNCACHE:
				case LAZY:
				case FORMATTED:
				case GLOBAL:
				case TEMPORARY:
				case OPTIONS:
				case UNSET:
				case TBLPROPERTIES:
				case DBPROPERTIES:
				case BUCKETS:
				case SKEWED:
				case STORED:
				case DIRECTORIES:
				case LOCATION:
				case EXCHANGE:
				case ARCHIVE:
				case UNARCHIVE:
				case FILEFORMAT:
				case TOUCH:
				case COMPACT:
				case CONCATENATE:
				case CHANGE:
				case CASCADE:
				case RESTRICT:
				case CLUSTERED:
				case SORTED:
				case PURGE:
				case INPUTFORMAT:
				case OUTPUTFORMAT:
				case DATABASE:
				case DATABASES:
				case DFS:
				case TRUNCATE:
				case ANALYZE:
				case COMPUTE:
				case LIST:
				case STATISTICS:
				case PARTITIONED:
				case EXTERNAL:
				case DEFINED:
				case REVOKE:
				case GRANT:
				case LOCK:
				case UNLOCK:
				case MSCK:
				case REPAIR:
				case RECOVER:
				case EXPORT:
				case IMPORT:
				case LOAD:
				case ROLE:
				case ROLES:
				case COMPACTIONS:
				case PRINCIPALS:
				case TRANSACTIONS:
				case INDEX:
				case INDEXES:
				case LOCKS:
				case OPTION:
				case ANTI:
				case LOCAL:
				case INPATH:
				case CURRENT_DATE:
				case CURRENT_TIMESTAMP:
				case IDENTIFIER:
				case BACKQUOTED_IDENTIFIER:
					{
					setState(1735);
					identifier();
					}
					break;
				case T__0:
					{
					setState(1736);
					identifierList();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamedExpressionSeqContext extends ParserRuleContext {
		public List<NamedExpressionContext> namedExpression() {
			return getRuleContexts(NamedExpressionContext.class);
		}
		public NamedExpressionContext namedExpression(int i) {
			return getRuleContext(NamedExpressionContext.class,i);
		}
		public NamedExpressionSeqContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedExpressionSeq; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedExpressionSeq(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedExpressionSeq(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedExpressionSeq(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedExpressionSeqContext namedExpressionSeq() throws RecognitionException {
		NamedExpressionSeqContext _localctx = new NamedExpressionSeqContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_namedExpressionSeq);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1741);
			namedExpression();
			setState(1746);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,242,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1742);
					match(T__2);
					setState(1743);
					namedExpression();
					}
					} 
				}
				setState(1748);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,242,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1749);
			booleanExpression(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BooleanExpressionContext extends ParserRuleContext {
		public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanExpression; }
	 
		public BooleanExpressionContext() { }
		public void copyFrom(BooleanExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class BooleanDefaultContext extends BooleanExpressionContext {
		public PredicatedContext predicated() {
			return getRuleContext(PredicatedContext.class,0);
		}
		public BooleanDefaultContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ExistsContext extends BooleanExpressionContext {
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public ExistsContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterExists(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitExists(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitExists(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LogicalBinaryContext extends BooleanExpressionContext {
		public BooleanExpressionContext left;
		public Token operator;
		public BooleanExpressionContext right;
		public List<BooleanExpressionContext> booleanExpression() {
			return getRuleContexts(BooleanExpressionContext.class);
		}
		public BooleanExpressionContext booleanExpression(int i) {
			return getRuleContext(BooleanExpressionContext.class,i);
		}
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public LogicalBinaryContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLogicalBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLogicalBinary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLogicalBinary(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LogicalNotContext extends BooleanExpressionContext {
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public BooleanExpressionContext booleanExpression() {
			return getRuleContext(BooleanExpressionContext.class,0);
		}
		public LogicalNotContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterLogicalNot(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitLogicalNot(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitLogicalNot(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanExpressionContext booleanExpression() throws RecognitionException {
		return booleanExpression(0);
	}

	private BooleanExpressionContext booleanExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		BooleanExpressionContext _localctx = new BooleanExpressionContext(_ctx, _parentState);
		BooleanExpressionContext _prevctx = _localctx;
		int _startState = 120;
		enterRecursionRule(_localctx, 120, RULE_booleanExpression, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1760);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,243,_ctx) ) {
			case 1:
				{
				_localctx = new LogicalNotContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(1752);
				match(NOT);
				setState(1753);
				booleanExpression(5);
				}
				break;
			case 2:
				{
				_localctx = new BooleanDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1754);
				predicated();
				}
				break;
			case 3:
				{
				_localctx = new ExistsContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1755);
				match(EXISTS);
				setState(1756);
				match(T__0);
				setState(1757);
				query();
				setState(1758);
				match(T__1);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(1770);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,245,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1768);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,244,_ctx) ) {
					case 1:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(1762);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(1763);
						((LogicalBinaryContext)_localctx).operator = match(AND);
						setState(1764);
						((LogicalBinaryContext)_localctx).right = booleanExpression(4);
						}
						break;
					case 2:
						{
						_localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
						((LogicalBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
						setState(1765);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(1766);
						((LogicalBinaryContext)_localctx).operator = match(OR);
						setState(1767);
						((LogicalBinaryContext)_localctx).right = booleanExpression(3);
						}
						break;
					}
					} 
				}
				setState(1772);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,245,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class PredicatedContext extends ParserRuleContext {
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public PredicatedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicated; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPredicated(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPredicated(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPredicated(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicatedContext predicated() throws RecognitionException {
		PredicatedContext _localctx = new PredicatedContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_predicated);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1773);
			valueExpression(0);
			setState(1775);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,246,_ctx) ) {
			case 1:
				{
				setState(1774);
				predicate();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PredicateContext extends ParserRuleContext {
		public Token kind;
		public ValueExpressionContext lower;
		public ValueExpressionContext upper;
		public ValueExpressionContext pattern;
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public TerminalNode RLIKE() { return getToken(SqlBaseParser.RLIKE, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public PredicateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicate; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPredicate(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateContext predicate() throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_predicate);
		int _la;
		try {
			setState(1818);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,253,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1778);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1777);
					match(NOT);
					}
				}

				setState(1780);
				((PredicateContext)_localctx).kind = match(BETWEEN);
				setState(1781);
				((PredicateContext)_localctx).lower = valueExpression(0);
				setState(1782);
				match(AND);
				setState(1783);
				((PredicateContext)_localctx).upper = valueExpression(0);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1786);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1785);
					match(NOT);
					}
				}

				setState(1788);
				((PredicateContext)_localctx).kind = match(IN);
				setState(1789);
				match(T__0);
				setState(1790);
				expression();
				setState(1795);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__2) {
					{
					{
					setState(1791);
					match(T__2);
					setState(1792);
					expression();
					}
					}
					setState(1797);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1798);
				match(T__1);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1801);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1800);
					match(NOT);
					}
				}

				setState(1803);
				((PredicateContext)_localctx).kind = match(IN);
				setState(1804);
				match(T__0);
				setState(1805);
				query();
				setState(1806);
				match(T__1);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1809);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1808);
					match(NOT);
					}
				}

				setState(1811);
				((PredicateContext)_localctx).kind = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==LIKE || _la==RLIKE) ) {
					((PredicateContext)_localctx).kind = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(1812);
				((PredicateContext)_localctx).pattern = valueExpression(0);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1813);
				match(IS);
				setState(1815);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1814);
					match(NOT);
					}
				}

				setState(1817);
				((PredicateContext)_localctx).kind = match(NULL);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ValueExpressionContext extends ParserRuleContext {
		public ValueExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valueExpression; }
	 
		public ValueExpressionContext() { }
		public void copyFrom(ValueExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ValueExpressionDefaultContext extends ValueExpressionContext {
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionDefaultContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterValueExpressionDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitValueExpressionDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitValueExpressionDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ArithmeticBinaryContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public Token operator;
		public ValueExpressionContext right;
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(SqlBaseParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(SqlBaseParser.PERCENT, 0); }
		public TerminalNode DIV() { return getToken(SqlBaseParser.DIV, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode AMPERSAND() { return getToken(SqlBaseParser.AMPERSAND, 0); }
		public TerminalNode HAT() { return getToken(SqlBaseParser.HAT, 0); }
		public TerminalNode PIPE() { return getToken(SqlBaseParser.PIPE, 0); }
		public ArithmeticBinaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticBinary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArithmeticBinary(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ArithmeticUnaryContext extends ValueExpressionContext {
		public Token operator;
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode TILDE() { return getToken(SqlBaseParser.TILDE, 0); }
		public ArithmeticUnaryContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticUnary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticUnary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArithmeticUnary(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ComparisonContext extends ValueExpressionContext {
		public ValueExpressionContext left;
		public ValueExpressionContext right;
		public ComparisonOperatorContext comparisonOperator() {
			return getRuleContext(ComparisonOperatorContext.class,0);
		}
		public List<ValueExpressionContext> valueExpression() {
			return getRuleContexts(ValueExpressionContext.class);
		}
		public ValueExpressionContext valueExpression(int i) {
			return getRuleContext(ValueExpressionContext.class,i);
		}
		public ComparisonContext(ValueExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparison(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparison(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparison(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ValueExpressionContext valueExpression() throws RecognitionException {
		return valueExpression(0);
	}

	private ValueExpressionContext valueExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ValueExpressionContext _localctx = new ValueExpressionContext(_ctx, _parentState);
		ValueExpressionContext _prevctx = _localctx;
		int _startState = 126;
		enterRecursionRule(_localctx, 126, RULE_valueExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1824);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,254,_ctx) ) {
			case 1:
				{
				_localctx = new ValueExpressionDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(1821);
				primaryExpression(0);
				}
				break;
			case 2:
				{
				_localctx = new ArithmeticUnaryContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1822);
				((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
				_la = _input.LA(1);
				if ( !(((((_la - 125)) & ~0x3f) == 0 && ((1L << (_la - 125)) & ((1L << (PLUS - 125)) | (1L << (MINUS - 125)) | (1L << (TILDE - 125)))) != 0)) ) {
					((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(1823);
				valueExpression(7);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(1847);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,256,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1845);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,255,_ctx) ) {
					case 1:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1826);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(1827);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(((((_la - 127)) & ~0x3f) == 0 && ((1L << (_la - 127)) & ((1L << (ASTERISK - 127)) | (1L << (SLASH - 127)) | (1L << (PERCENT - 127)) | (1L << (DIV - 127)))) != 0)) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						} else {
							consume();
						}
						setState(1828);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(7);
						}
						break;
					case 2:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1829);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(1830);
						((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
						_la = _input.LA(1);
						if ( !(_la==PLUS || _la==MINUS) ) {
							((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
						} else {
							consume();
						}
						setState(1831);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(6);
						}
						break;
					case 3:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1832);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(1833);
						((ArithmeticBinaryContext)_localctx).operator = match(AMPERSAND);
						setState(1834);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(5);
						}
						break;
					case 4:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1835);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(1836);
						((ArithmeticBinaryContext)_localctx).operator = match(HAT);
						setState(1837);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(4);
						}
						break;
					case 5:
						{
						_localctx = new ArithmeticBinaryContext(new ValueExpressionContext(_parentctx, _parentState));
						((ArithmeticBinaryContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1838);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(1839);
						((ArithmeticBinaryContext)_localctx).operator = match(PIPE);
						setState(1840);
						((ArithmeticBinaryContext)_localctx).right = valueExpression(3);
						}
						break;
					case 6:
						{
						_localctx = new ComparisonContext(new ValueExpressionContext(_parentctx, _parentState));
						((ComparisonContext)_localctx).left = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
						setState(1841);
						if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
						setState(1842);
						comparisonOperator();
						setState(1843);
						((ComparisonContext)_localctx).right = valueExpression(2);
						}
						break;
					}
					} 
				}
				setState(1849);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,256,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class PrimaryExpressionContext extends ParserRuleContext {
		public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primaryExpression; }
	 
		public PrimaryExpressionContext() { }
		public void copyFrom(PrimaryExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ColumnReferenceContext extends PrimaryExpressionContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColumnReferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColumnReference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColumnReference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColumnReference(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ConstantDefaultContext extends PrimaryExpressionContext {
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public ConstantDefaultContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterConstantDefault(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitConstantDefault(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitConstantDefault(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SubqueryExpressionContext extends PrimaryExpressionContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public SubqueryExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubqueryExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubqueryExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubqueryExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class StarContext extends PrimaryExpressionContext {
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public StarContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStar(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStar(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStar(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SimpleCaseContext extends PrimaryExpressionContext {
		public ExpressionContext value;
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public SimpleCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSimpleCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSimpleCase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSimpleCase(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterParenthesizedExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitParenthesizedExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitParenthesizedExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SearchedCaseContext extends PrimaryExpressionContext {
		public ExpressionContext elseExpression;
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public List<WhenClauseContext> whenClause() {
			return getRuleContexts(WhenClauseContext.class);
		}
		public WhenClauseContext whenClause(int i) {
			return getRuleContext(WhenClauseContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SearchedCaseContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSearchedCase(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSearchedCase(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSearchedCase(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class FunctionCallContext extends PrimaryExpressionContext {
		public QualifiedNameContext qualifiedName() {
			return getRuleContext(QualifiedNameContext.class,0);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public WindowSpecContext windowSpec() {
			return getRuleContext(WindowSpecContext.class,0);
		}
		public SetQuantifierContext setQuantifier() {
			return getRuleContext(SetQuantifierContext.class,0);
		}
		public FunctionCallContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFunctionCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFunctionCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFunctionCall(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TimeFunctionCallContext extends PrimaryExpressionContext {
		public Token name;
		public TerminalNode CURRENT_DATE() { return getToken(SqlBaseParser.CURRENT_DATE, 0); }
		public TerminalNode CURRENT_TIMESTAMP() { return getToken(SqlBaseParser.CURRENT_TIMESTAMP, 0); }
		public TimeFunctionCallContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTimeFunctionCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTimeFunctionCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTimeFunctionCall(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RowConstructorContext extends PrimaryExpressionContext {
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public RowConstructorContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterRowConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitRowConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitRowConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CastContext extends PrimaryExpressionContext {
		public TerminalNode CAST() { return getToken(SqlBaseParser.CAST, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public CastContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterCast(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitCast(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitCast(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SubscriptContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext value;
		public ValueExpressionContext index;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public ValueExpressionContext valueExpression() {
			return getRuleContext(ValueExpressionContext.class,0);
		}
		public SubscriptContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSubscript(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSubscript(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSubscript(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DereferenceContext extends PrimaryExpressionContext {
		public PrimaryExpressionContext base;
		public IdentifierContext fieldName;
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DereferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDereference(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDereference(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDereference(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
		return primaryExpression(0);
	}

	private PrimaryExpressionContext primaryExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, _parentState);
		PrimaryExpressionContext _prevctx = _localctx;
		int _startState = 128;
		enterRecursionRule(_localctx, 128, RULE_primaryExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1929);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,266,_ctx) ) {
			case 1:
				{
				_localctx = new TimeFunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(1851);
				((TimeFunctionCallContext)_localctx).name = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==CURRENT_DATE || _la==CURRENT_TIMESTAMP) ) {
					((TimeFunctionCallContext)_localctx).name = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
				break;
			case 2:
				{
				_localctx = new SearchedCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1852);
				match(CASE);
				setState(1854); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1853);
					whenClause();
					}
					}
					setState(1856); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(1860);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(1858);
					match(ELSE);
					setState(1859);
					((SearchedCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(1862);
				match(END);
				}
				break;
			case 3:
				{
				_localctx = new SimpleCaseContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1864);
				match(CASE);
				setState(1865);
				((SimpleCaseContext)_localctx).value = expression();
				setState(1867); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1866);
					whenClause();
					}
					}
					setState(1869); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==WHEN );
				setState(1873);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(1871);
					match(ELSE);
					setState(1872);
					((SimpleCaseContext)_localctx).elseExpression = expression();
					}
				}

				setState(1875);
				match(END);
				}
				break;
			case 4:
				{
				_localctx = new CastContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1877);
				match(CAST);
				setState(1878);
				match(T__0);
				setState(1879);
				expression();
				setState(1880);
				match(AS);
				setState(1881);
				dataType();
				setState(1882);
				match(T__1);
				}
				break;
			case 5:
				{
				_localctx = new ConstantDefaultContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1884);
				constant();
				}
				break;
			case 6:
				{
				_localctx = new StarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1885);
				match(ASTERISK);
				}
				break;
			case 7:
				{
				_localctx = new StarContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1886);
				qualifiedName();
				setState(1887);
				match(T__3);
				setState(1888);
				match(ASTERISK);
				}
				break;
			case 8:
				{
				_localctx = new RowConstructorContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1890);
				match(T__0);
				setState(1891);
				expression();
				setState(1894); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1892);
					match(T__2);
					setState(1893);
					expression();
					}
					}
					setState(1896); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==T__2 );
				setState(1898);
				match(T__1);
				}
				break;
			case 9:
				{
				_localctx = new SubqueryExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1900);
				match(T__0);
				setState(1901);
				query();
				setState(1902);
				match(T__1);
				}
				break;
			case 10:
				{
				_localctx = new FunctionCallContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1904);
				qualifiedName();
				setState(1905);
				match(T__0);
				setState(1917);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << LATERAL) | (1L << WINDOW) | (1L << OVER) | (1L << PARTITION) | (1L << RANGE) | (1L << ROWS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (FIRST - 64)) | (1L << (AFTER - 64)) | (1L << (LAST - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (SETMINUS - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IF - 64)) | (1L << (PLUS - 64)) | (1L << (MINUS - 64)) | (1L << (ASTERISK - 64)))) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & ((1L << (DIV - 130)) | (1L << (TILDE - 130)) | (1L << (PERCENTLIT - 130)) | (1L << (BUCKET - 130)) | (1L << (OUT - 130)) | (1L << (OF - 130)) | (1L << (SORT - 130)) | (1L << (CLUSTER - 130)) | (1L << (DISTRIBUTE - 130)) | (1L << (OVERWRITE - 130)) | (1L << (TRANSFORM - 130)) | (1L << (REDUCE - 130)) | (1L << (USING - 130)) | (1L << (SERDE - 130)) | (1L << (SERDEPROPERTIES - 130)) | (1L << (RECORDREADER - 130)) | (1L << (RECORDWRITER - 130)) | (1L << (DELIMITED - 130)) | (1L << (FIELDS - 130)) | (1L << (TERMINATED - 130)) | (1L << (COLLECTION - 130)) | (1L << (ITEMS - 130)) | (1L << (KEYS - 130)) | (1L << (ESCAPED - 130)) | (1L << (LINES - 130)) | (1L << (SEPARATED - 130)) | (1L << (FUNCTION - 130)) | (1L << (EXTENDED - 130)) | (1L << (REFRESH - 130)) | (1L << (CLEAR - 130)) | (1L << (CACHE - 130)) | (1L << (UNCACHE - 130)) | (1L << (LAZY - 130)) | (1L << (FORMATTED - 130)) | (1L << (GLOBAL - 130)) | (1L << (TEMPORARY - 130)) | (1L << (OPTIONS - 130)) | (1L << (UNSET - 130)) | (1L << (TBLPROPERTIES - 130)) | (1L << (DBPROPERTIES - 130)) | (1L << (BUCKETS - 130)) | (1L << (SKEWED - 130)) | (1L << (STORED - 130)) | (1L << (DIRECTORIES - 130)) | (1L << (LOCATION - 130)) | (1L << (EXCHANGE - 130)) | (1L << (ARCHIVE - 130)) | (1L << (UNARCHIVE - 130)) | (1L << (FILEFORMAT - 130)) | (1L << (TOUCH - 130)) | (1L << (COMPACT - 130)) | (1L << (CONCATENATE - 130)) | (1L << (CHANGE - 130)) | (1L << (CASCADE - 130)) | (1L << (RESTRICT - 130)) | (1L << (CLUSTERED - 130)) | (1L << (SORTED - 130)) | (1L << (PURGE - 130)) | (1L << (INPUTFORMAT - 130)) | (1L << (OUTPUTFORMAT - 130)) | (1L << (DATABASE - 130)))) != 0) || ((((_la - 194)) & ~0x3f) == 0 && ((1L << (_la - 194)) & ((1L << (DATABASES - 194)) | (1L << (DFS - 194)) | (1L << (TRUNCATE - 194)) | (1L << (ANALYZE - 194)) | (1L << (COMPUTE - 194)) | (1L << (LIST - 194)) | (1L << (STATISTICS - 194)) | (1L << (PARTITIONED - 194)) | (1L << (EXTERNAL - 194)) | (1L << (DEFINED - 194)) | (1L << (REVOKE - 194)) | (1L << (GRANT - 194)) | (1L << (LOCK - 194)) | (1L << (UNLOCK - 194)) | (1L << (MSCK - 194)) | (1L << (REPAIR - 194)) | (1L << (RECOVER - 194)) | (1L << (EXPORT - 194)) | (1L << (IMPORT - 194)) | (1L << (LOAD - 194)) | (1L << (ROLE - 194)) | (1L << (ROLES - 194)) | (1L << (COMPACTIONS - 194)) | (1L << (PRINCIPALS - 194)) | (1L << (TRANSACTIONS - 194)) | (1L << (INDEX - 194)) | (1L << (INDEXES - 194)) | (1L << (LOCKS - 194)) | (1L << (OPTION - 194)) | (1L << (ANTI - 194)) | (1L << (LOCAL - 194)) | (1L << (INPATH - 194)) | (1L << (CURRENT_DATE - 194)) | (1L << (CURRENT_TIMESTAMP - 194)) | (1L << (STRING - 194)) | (1L << (BIGINT_LITERAL - 194)) | (1L << (SMALLINT_LITERAL - 194)) | (1L << (TINYINT_LITERAL - 194)) | (1L << (INTEGER_VALUE - 194)) | (1L << (DECIMAL_VALUE - 194)) | (1L << (DOUBLE_LITERAL - 194)) | (1L << (BIGDECIMAL_LITERAL - 194)) | (1L << (IDENTIFIER - 194)) | (1L << (BACKQUOTED_IDENTIFIER - 194)))) != 0)) {
					{
					setState(1907);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,262,_ctx) ) {
					case 1:
						{
						setState(1906);
						setQuantifier();
						}
						break;
					}
					setState(1909);
					expression();
					setState(1914);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(1910);
						match(T__2);
						setState(1911);
						expression();
						}
						}
						setState(1916);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1919);
				match(T__1);
				setState(1922);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,265,_ctx) ) {
				case 1:
					{
					setState(1920);
					match(OVER);
					setState(1921);
					windowSpec();
					}
					break;
				}
				}
				break;
			case 11:
				{
				_localctx = new ColumnReferenceContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1924);
				identifier();
				}
				break;
			case 12:
				{
				_localctx = new ParenthesizedExpressionContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(1925);
				match(T__0);
				setState(1926);
				expression();
				setState(1927);
				match(T__1);
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(1941);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,268,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(1939);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,267,_ctx) ) {
					case 1:
						{
						_localctx = new SubscriptContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((SubscriptContext)_localctx).value = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(1931);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(1932);
						match(T__4);
						setState(1933);
						((SubscriptContext)_localctx).index = valueExpression(0);
						setState(1934);
						match(T__5);
						}
						break;
					case 2:
						{
						_localctx = new DereferenceContext(new PrimaryExpressionContext(_parentctx, _parentState));
						((DereferenceContext)_localctx).base = _prevctx;
						pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
						setState(1936);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(1937);
						match(T__3);
						setState(1938);
						((DereferenceContext)_localctx).fieldName = identifier();
						}
						break;
					}
					} 
				}
				setState(1943);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,268,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class ConstantContext extends ParserRuleContext {
		public ConstantContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constant; }
	 
		public ConstantContext() { }
		public void copyFrom(ConstantContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class NullLiteralContext extends ConstantContext {
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public NullLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNullLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNullLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNullLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class IntervalLiteralContext extends ConstantContext {
		public IntervalContext interval() {
			return getRuleContext(IntervalContext.class,0);
		}
		public IntervalLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TypeConstructorContext extends ConstantContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public TypeConstructorContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTypeConstructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTypeConstructor(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTypeConstructor(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class StringLiteralContext extends ConstantContext {
		public List<TerminalNode> STRING() { return getTokens(SqlBaseParser.STRING); }
		public TerminalNode STRING(int i) {
			return getToken(SqlBaseParser.STRING, i);
		}
		public StringLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterStringLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitStringLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitStringLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BooleanLiteralContext extends ConstantContext {
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public BooleanLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class NumericLiteralContext extends ConstantContext {
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public NumericLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNumericLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNumericLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNumericLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstantContext constant() throws RecognitionException {
		ConstantContext _localctx = new ConstantContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_constant);
		try {
			int _alt;
			setState(1956);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,270,_ctx) ) {
			case 1:
				_localctx = new NullLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1944);
				match(NULL);
				}
				break;
			case 2:
				_localctx = new IntervalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1945);
				interval();
				}
				break;
			case 3:
				_localctx = new TypeConstructorContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1946);
				identifier();
				setState(1947);
				match(STRING);
				}
				break;
			case 4:
				_localctx = new NumericLiteralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1949);
				number();
				}
				break;
			case 5:
				_localctx = new BooleanLiteralContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1950);
				booleanValue();
				}
				break;
			case 6:
				_localctx = new StringLiteralContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(1952); 
				_errHandler.sync(this);
				_alt = 1;
				do {
					switch (_alt) {
					case 1:
						{
						{
						setState(1951);
						match(STRING);
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					setState(1954); 
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,269,_ctx);
				} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComparisonOperatorContext extends ParserRuleContext {
		public TerminalNode EQ() { return getToken(SqlBaseParser.EQ, 0); }
		public TerminalNode NEQ() { return getToken(SqlBaseParser.NEQ, 0); }
		public TerminalNode NEQJ() { return getToken(SqlBaseParser.NEQJ, 0); }
		public TerminalNode LT() { return getToken(SqlBaseParser.LT, 0); }
		public TerminalNode LTE() { return getToken(SqlBaseParser.LTE, 0); }
		public TerminalNode GT() { return getToken(SqlBaseParser.GT, 0); }
		public TerminalNode GTE() { return getToken(SqlBaseParser.GTE, 0); }
		public TerminalNode NSEQ() { return getToken(SqlBaseParser.NSEQ, 0); }
		public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comparisonOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComparisonOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComparisonOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComparisonOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
		ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_comparisonOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1958);
			_la = _input.LA(1);
			if ( !(((((_la - 117)) & ~0x3f) == 0 && ((1L << (_la - 117)) & ((1L << (EQ - 117)) | (1L << (NSEQ - 117)) | (1L << (NEQ - 117)) | (1L << (NEQJ - 117)) | (1L << (LT - 117)) | (1L << (LTE - 117)) | (1L << (GT - 117)) | (1L << (GTE - 117)))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ArithmeticOperatorContext extends ParserRuleContext {
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode ASTERISK() { return getToken(SqlBaseParser.ASTERISK, 0); }
		public TerminalNode SLASH() { return getToken(SqlBaseParser.SLASH, 0); }
		public TerminalNode PERCENT() { return getToken(SqlBaseParser.PERCENT, 0); }
		public TerminalNode DIV() { return getToken(SqlBaseParser.DIV, 0); }
		public TerminalNode TILDE() { return getToken(SqlBaseParser.TILDE, 0); }
		public TerminalNode AMPERSAND() { return getToken(SqlBaseParser.AMPERSAND, 0); }
		public TerminalNode PIPE() { return getToken(SqlBaseParser.PIPE, 0); }
		public TerminalNode HAT() { return getToken(SqlBaseParser.HAT, 0); }
		public ArithmeticOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_arithmeticOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterArithmeticOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitArithmeticOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitArithmeticOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ArithmeticOperatorContext arithmeticOperator() throws RecognitionException {
		ArithmeticOperatorContext _localctx = new ArithmeticOperatorContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_arithmeticOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1960);
			_la = _input.LA(1);
			if ( !(((((_la - 125)) & ~0x3f) == 0 && ((1L << (_la - 125)) & ((1L << (PLUS - 125)) | (1L << (MINUS - 125)) | (1L << (ASTERISK - 125)) | (1L << (SLASH - 125)) | (1L << (PERCENT - 125)) | (1L << (DIV - 125)) | (1L << (TILDE - 125)) | (1L << (AMPERSAND - 125)) | (1L << (PIPE - 125)) | (1L << (HAT - 125)))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PredicateOperatorContext extends ParserRuleContext {
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public PredicateOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicateOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPredicateOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPredicateOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPredicateOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateOperatorContext predicateOperator() throws RecognitionException {
		PredicateOperatorContext _localctx = new PredicateOperatorContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_predicateOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1962);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BooleanValueContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
		public BooleanValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBooleanValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBooleanValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBooleanValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanValueContext booleanValue() throws RecognitionException {
		BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_booleanValue);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1964);
			_la = _input.LA(1);
			if ( !(_la==TRUE || _la==FALSE) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntervalContext extends ParserRuleContext {
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public List<IntervalFieldContext> intervalField() {
			return getRuleContexts(IntervalFieldContext.class);
		}
		public IntervalFieldContext intervalField(int i) {
			return getRuleContext(IntervalFieldContext.class,i);
		}
		public IntervalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_interval; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterInterval(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitInterval(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitInterval(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalContext interval() throws RecognitionException {
		IntervalContext _localctx = new IntervalContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_interval);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1966);
			match(INTERVAL);
			setState(1970);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,271,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1967);
					intervalField();
					}
					} 
				}
				setState(1972);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,271,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntervalFieldContext extends ParserRuleContext {
		public IntervalValueContext value;
		public IdentifierContext unit;
		public IdentifierContext to;
		public IntervalValueContext intervalValue() {
			return getRuleContext(IntervalValueContext.class,0);
		}
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public TerminalNode TO() { return getToken(SqlBaseParser.TO, 0); }
		public IntervalFieldContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intervalField; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalField(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalField(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalField(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalFieldContext intervalField() throws RecognitionException {
		IntervalFieldContext _localctx = new IntervalFieldContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_intervalField);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1973);
			((IntervalFieldContext)_localctx).value = intervalValue();
			setState(1974);
			((IntervalFieldContext)_localctx).unit = identifier();
			setState(1977);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,272,_ctx) ) {
			case 1:
				{
				setState(1975);
				match(TO);
				setState(1976);
				((IntervalFieldContext)_localctx).to = identifier();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntervalValueContext extends ParserRuleContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode PLUS() { return getToken(SqlBaseParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public IntervalValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_intervalValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntervalValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntervalValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntervalValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IntervalValueContext intervalValue() throws RecognitionException {
		IntervalValueContext _localctx = new IntervalValueContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_intervalValue);
		int _la;
		try {
			setState(1984);
			switch (_input.LA(1)) {
			case PLUS:
			case MINUS:
			case INTEGER_VALUE:
			case DECIMAL_VALUE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1980);
				_la = _input.LA(1);
				if (_la==PLUS || _la==MINUS) {
					{
					setState(1979);
					_la = _input.LA(1);
					if ( !(_la==PLUS || _la==MINUS) ) {
					_errHandler.recoverInline(this);
					} else {
						consume();
					}
					}
				}

				setState(1982);
				_la = _input.LA(1);
				if ( !(_la==INTEGER_VALUE || _la==DECIMAL_VALUE) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
				break;
			case STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1983);
				match(STRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColPositionContext extends ParserRuleContext {
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode AFTER() { return getToken(SqlBaseParser.AFTER, 0); }
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public ColPositionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colPosition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColPosition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColPosition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColPosition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColPositionContext colPosition() throws RecognitionException {
		ColPositionContext _localctx = new ColPositionContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_colPosition);
		try {
			setState(1989);
			switch (_input.LA(1)) {
			case FIRST:
				enterOuterAlt(_localctx, 1);
				{
				setState(1986);
				match(FIRST);
				}
				break;
			case AFTER:
				enterOuterAlt(_localctx, 2);
				{
				setState(1987);
				match(AFTER);
				setState(1988);
				identifier();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DataTypeContext extends ParserRuleContext {
		public DataTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dataType; }
	 
		public DataTypeContext() { }
		public void copyFrom(DataTypeContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class PrimitiveDataTypeContext extends DataTypeContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public List<TerminalNode> INTEGER_VALUE() { return getTokens(SqlBaseParser.INTEGER_VALUE); }
		public TerminalNode INTEGER_VALUE(int i) {
			return getToken(SqlBaseParser.INTEGER_VALUE, i);
		}
		public PrimitiveDataTypeContext(DataTypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterPrimitiveDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitPrimitiveDataType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitPrimitiveDataType(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ComplexDataTypeContext extends DataTypeContext {
		public Token complex;
		public List<DataTypeContext> dataType() {
			return getRuleContexts(DataTypeContext.class);
		}
		public DataTypeContext dataType(int i) {
			return getRuleContext(DataTypeContext.class,i);
		}
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode STRUCT() { return getToken(SqlBaseParser.STRUCT, 0); }
		public TerminalNode NEQ() { return getToken(SqlBaseParser.NEQ, 0); }
		public ComplexColTypeListContext complexColTypeList() {
			return getRuleContext(ComplexColTypeListContext.class,0);
		}
		public ComplexDataTypeContext(DataTypeContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComplexDataType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComplexDataType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComplexDataType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DataTypeContext dataType() throws RecognitionException {
		DataTypeContext _localctx = new DataTypeContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_dataType);
		int _la;
		try {
			setState(2025);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,280,_ctx) ) {
			case 1:
				_localctx = new ComplexDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1991);
				((ComplexDataTypeContext)_localctx).complex = match(ARRAY);
				setState(1992);
				match(LT);
				setState(1993);
				dataType();
				setState(1994);
				match(GT);
				}
				break;
			case 2:
				_localctx = new ComplexDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1996);
				((ComplexDataTypeContext)_localctx).complex = match(MAP);
				setState(1997);
				match(LT);
				setState(1998);
				dataType();
				setState(1999);
				match(T__2);
				setState(2000);
				dataType();
				setState(2001);
				match(GT);
				}
				break;
			case 3:
				_localctx = new ComplexDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2003);
				((ComplexDataTypeContext)_localctx).complex = match(STRUCT);
				setState(2010);
				switch (_input.LA(1)) {
				case LT:
					{
					setState(2004);
					match(LT);
					setState(2006);
					_la = _input.LA(1);
					if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << JOIN) | (1L << CROSS) | (1L << OUTER) | (1L << INNER) | (1L << LEFT) | (1L << SEMI) | (1L << RIGHT) | (1L << FULL) | (1L << NATURAL) | (1L << ON) | (1L << LATERAL) | (1L << WINDOW) | (1L << OVER) | (1L << PARTITION) | (1L << RANGE) | (1L << ROWS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (FIRST - 64)) | (1L << (AFTER - 64)) | (1L << (LAST - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (UNION - 64)) | (1L << (EXCEPT - 64)) | (1L << (SETMINUS - 64)) | (1L << (INTERSECT - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IF - 64)))) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & ((1L << (DIV - 130)) | (1L << (PERCENTLIT - 130)) | (1L << (BUCKET - 130)) | (1L << (OUT - 130)) | (1L << (OF - 130)) | (1L << (SORT - 130)) | (1L << (CLUSTER - 130)) | (1L << (DISTRIBUTE - 130)) | (1L << (OVERWRITE - 130)) | (1L << (TRANSFORM - 130)) | (1L << (REDUCE - 130)) | (1L << (USING - 130)) | (1L << (SERDE - 130)) | (1L << (SERDEPROPERTIES - 130)) | (1L << (RECORDREADER - 130)) | (1L << (RECORDWRITER - 130)) | (1L << (DELIMITED - 130)) | (1L << (FIELDS - 130)) | (1L << (TERMINATED - 130)) | (1L << (COLLECTION - 130)) | (1L << (ITEMS - 130)) | (1L << (KEYS - 130)) | (1L << (ESCAPED - 130)) | (1L << (LINES - 130)) | (1L << (SEPARATED - 130)) | (1L << (FUNCTION - 130)) | (1L << (EXTENDED - 130)) | (1L << (REFRESH - 130)) | (1L << (CLEAR - 130)) | (1L << (CACHE - 130)) | (1L << (UNCACHE - 130)) | (1L << (LAZY - 130)) | (1L << (FORMATTED - 130)) | (1L << (GLOBAL - 130)) | (1L << (TEMPORARY - 130)) | (1L << (OPTIONS - 130)) | (1L << (UNSET - 130)) | (1L << (TBLPROPERTIES - 130)) | (1L << (DBPROPERTIES - 130)) | (1L << (BUCKETS - 130)) | (1L << (SKEWED - 130)) | (1L << (STORED - 130)) | (1L << (DIRECTORIES - 130)) | (1L << (LOCATION - 130)) | (1L << (EXCHANGE - 130)) | (1L << (ARCHIVE - 130)) | (1L << (UNARCHIVE - 130)) | (1L << (FILEFORMAT - 130)) | (1L << (TOUCH - 130)) | (1L << (COMPACT - 130)) | (1L << (CONCATENATE - 130)) | (1L << (CHANGE - 130)) | (1L << (CASCADE - 130)) | (1L << (RESTRICT - 130)) | (1L << (CLUSTERED - 130)) | (1L << (SORTED - 130)) | (1L << (PURGE - 130)) | (1L << (INPUTFORMAT - 130)) | (1L << (OUTPUTFORMAT - 130)) | (1L << (DATABASE - 130)))) != 0) || ((((_la - 194)) & ~0x3f) == 0 && ((1L << (_la - 194)) & ((1L << (DATABASES - 194)) | (1L << (DFS - 194)) | (1L << (TRUNCATE - 194)) | (1L << (ANALYZE - 194)) | (1L << (COMPUTE - 194)) | (1L << (LIST - 194)) | (1L << (STATISTICS - 194)) | (1L << (PARTITIONED - 194)) | (1L << (EXTERNAL - 194)) | (1L << (DEFINED - 194)) | (1L << (REVOKE - 194)) | (1L << (GRANT - 194)) | (1L << (LOCK - 194)) | (1L << (UNLOCK - 194)) | (1L << (MSCK - 194)) | (1L << (REPAIR - 194)) | (1L << (RECOVER - 194)) | (1L << (EXPORT - 194)) | (1L << (IMPORT - 194)) | (1L << (LOAD - 194)) | (1L << (ROLE - 194)) | (1L << (ROLES - 194)) | (1L << (COMPACTIONS - 194)) | (1L << (PRINCIPALS - 194)) | (1L << (TRANSACTIONS - 194)) | (1L << (INDEX - 194)) | (1L << (INDEXES - 194)) | (1L << (LOCKS - 194)) | (1L << (OPTION - 194)) | (1L << (ANTI - 194)) | (1L << (LOCAL - 194)) | (1L << (INPATH - 194)) | (1L << (CURRENT_DATE - 194)) | (1L << (CURRENT_TIMESTAMP - 194)) | (1L << (IDENTIFIER - 194)) | (1L << (BACKQUOTED_IDENTIFIER - 194)))) != 0)) {
						{
						setState(2005);
						complexColTypeList();
						}
					}

					setState(2008);
					match(GT);
					}
					break;
				case NEQ:
					{
					setState(2009);
					match(NEQ);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case 4:
				_localctx = new PrimitiveDataTypeContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2012);
				identifier();
				setState(2023);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,279,_ctx) ) {
				case 1:
					{
					setState(2013);
					match(T__0);
					setState(2014);
					match(INTEGER_VALUE);
					setState(2019);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(2015);
						match(T__2);
						setState(2016);
						match(INTEGER_VALUE);
						}
						}
						setState(2021);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(2022);
					match(T__1);
					}
					break;
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColTypeListContext extends ParserRuleContext {
		public List<ColTypeContext> colType() {
			return getRuleContexts(ColTypeContext.class);
		}
		public ColTypeContext colType(int i) {
			return getRuleContext(ColTypeContext.class,i);
		}
		public ColTypeListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colTypeList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColTypeList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColTypeList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColTypeList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColTypeListContext colTypeList() throws RecognitionException {
		ColTypeListContext _localctx = new ColTypeListContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_colTypeList);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2027);
			colType();
			setState(2032);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,281,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2028);
					match(T__2);
					setState(2029);
					colType();
					}
					} 
				}
				setState(2034);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,281,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColTypeContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ColTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterColType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitColType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitColType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColTypeContext colType() throws RecognitionException {
		ColTypeContext _localctx = new ColTypeContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_colType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2035);
			identifier();
			setState(2036);
			dataType();
			setState(2039);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,282,_ctx) ) {
			case 1:
				{
				setState(2037);
				match(COMMENT);
				setState(2038);
				match(STRING);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComplexColTypeListContext extends ParserRuleContext {
		public List<ComplexColTypeContext> complexColType() {
			return getRuleContexts(ComplexColTypeContext.class);
		}
		public ComplexColTypeContext complexColType(int i) {
			return getRuleContext(ComplexColTypeContext.class,i);
		}
		public ComplexColTypeListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_complexColTypeList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComplexColTypeList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComplexColTypeList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComplexColTypeList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComplexColTypeListContext complexColTypeList() throws RecognitionException {
		ComplexColTypeListContext _localctx = new ComplexColTypeListContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_complexColTypeList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2041);
			complexColType();
			setState(2046);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__2) {
				{
				{
				setState(2042);
				match(T__2);
				setState(2043);
				complexColType();
				}
				}
				setState(2048);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ComplexColTypeContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public DataTypeContext dataType() {
			return getRuleContext(DataTypeContext.class,0);
		}
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode STRING() { return getToken(SqlBaseParser.STRING, 0); }
		public ComplexColTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_complexColType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterComplexColType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitComplexColType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitComplexColType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ComplexColTypeContext complexColType() throws RecognitionException {
		ComplexColTypeContext _localctx = new ComplexColTypeContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_complexColType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2049);
			identifier();
			setState(2050);
			match(T__6);
			setState(2051);
			dataType();
			setState(2054);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(2052);
				match(COMMENT);
				setState(2053);
				match(STRING);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WhenClauseContext extends ParserRuleContext {
		public ExpressionContext condition;
		public ExpressionContext result;
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WhenClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whenClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWhenClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWhenClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWhenClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WhenClauseContext whenClause() throws RecognitionException {
		WhenClauseContext _localctx = new WhenClauseContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_whenClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2056);
			match(WHEN);
			setState(2057);
			((WhenClauseContext)_localctx).condition = expression();
			setState(2058);
			match(THEN);
			setState(2059);
			((WhenClauseContext)_localctx).result = expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowsContext extends ParserRuleContext {
		public TerminalNode WINDOW() { return getToken(SqlBaseParser.WINDOW, 0); }
		public List<NamedWindowContext> namedWindow() {
			return getRuleContexts(NamedWindowContext.class);
		}
		public NamedWindowContext namedWindow(int i) {
			return getRuleContext(NamedWindowContext.class,i);
		}
		public WindowsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windows; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindows(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindows(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWindows(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowsContext windows() throws RecognitionException {
		WindowsContext _localctx = new WindowsContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_windows);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2061);
			match(WINDOW);
			setState(2062);
			namedWindow();
			setState(2067);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,285,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2063);
					match(T__2);
					setState(2064);
					namedWindow();
					}
					} 
				}
				setState(2069);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,285,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NamedWindowContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public WindowSpecContext windowSpec() {
			return getRuleContext(WindowSpecContext.class,0);
		}
		public NamedWindowContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedWindow; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNamedWindow(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNamedWindow(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNamedWindow(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamedWindowContext namedWindow() throws RecognitionException {
		NamedWindowContext _localctx = new NamedWindowContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_namedWindow);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2070);
			identifier();
			setState(2071);
			match(AS);
			setState(2072);
			windowSpec();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowSpecContext extends ParserRuleContext {
		public WindowSpecContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowSpec; }
	 
		public WindowSpecContext() { }
		public void copyFrom(WindowSpecContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class WindowDefContext extends WindowSpecContext {
		public ExpressionContext expression;
		public List<ExpressionContext> partition = new ArrayList<ExpressionContext>();
		public TerminalNode CLUSTER() { return getToken(SqlBaseParser.CLUSTER, 0); }
		public List<TerminalNode> BY() { return getTokens(SqlBaseParser.BY); }
		public TerminalNode BY(int i) {
			return getToken(SqlBaseParser.BY, i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public WindowFrameContext windowFrame() {
			return getRuleContext(WindowFrameContext.class,0);
		}
		public List<SortItemContext> sortItem() {
			return getRuleContexts(SortItemContext.class);
		}
		public SortItemContext sortItem(int i) {
			return getRuleContext(SortItemContext.class,i);
		}
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlBaseParser.DISTRIBUTE, 0); }
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode SORT() { return getToken(SqlBaseParser.SORT, 0); }
		public WindowDefContext(WindowSpecContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindowDef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindowDef(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWindowDef(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class WindowRefContext extends WindowSpecContext {
		public IdentifierContext name;
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public WindowRefContext(WindowSpecContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindowRef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindowRef(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWindowRef(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowSpecContext windowSpec() throws RecognitionException {
		WindowSpecContext _localctx = new WindowSpecContext(_ctx, getState());
		enterRule(_localctx, 164, RULE_windowSpec);
		int _la;
		try {
			setState(2116);
			switch (_input.LA(1)) {
			case SELECT:
			case FROM:
			case ADD:
			case AS:
			case ALL:
			case DISTINCT:
			case WHERE:
			case GROUP:
			case BY:
			case GROUPING:
			case SETS:
			case CUBE:
			case ROLLUP:
			case ORDER:
			case HAVING:
			case LIMIT:
			case AT:
			case OR:
			case AND:
			case IN:
			case NOT:
			case NO:
			case EXISTS:
			case BETWEEN:
			case LIKE:
			case RLIKE:
			case IS:
			case NULL:
			case TRUE:
			case FALSE:
			case NULLS:
			case ASC:
			case DESC:
			case FOR:
			case INTERVAL:
			case CASE:
			case WHEN:
			case THEN:
			case ELSE:
			case END:
			case JOIN:
			case CROSS:
			case OUTER:
			case INNER:
			case LEFT:
			case SEMI:
			case RIGHT:
			case FULL:
			case NATURAL:
			case ON:
			case LATERAL:
			case WINDOW:
			case OVER:
			case PARTITION:
			case RANGE:
			case ROWS:
			case UNBOUNDED:
			case PRECEDING:
			case FOLLOWING:
			case CURRENT:
			case FIRST:
			case AFTER:
			case LAST:
			case ROW:
			case WITH:
			case VALUES:
			case CREATE:
			case TABLE:
			case VIEW:
			case REPLACE:
			case INSERT:
			case DELETE:
			case INTO:
			case DESCRIBE:
			case EXPLAIN:
			case FORMAT:
			case LOGICAL:
			case CODEGEN:
			case CAST:
			case SHOW:
			case TABLES:
			case COLUMNS:
			case COLUMN:
			case USE:
			case PARTITIONS:
			case FUNCTIONS:
			case DROP:
			case UNION:
			case EXCEPT:
			case SETMINUS:
			case INTERSECT:
			case TO:
			case TABLESAMPLE:
			case STRATIFY:
			case ALTER:
			case RENAME:
			case ARRAY:
			case MAP:
			case STRUCT:
			case COMMENT:
			case SET:
			case RESET:
			case DATA:
			case START:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case MACRO:
			case IF:
			case DIV:
			case PERCENTLIT:
			case BUCKET:
			case OUT:
			case OF:
			case SORT:
			case CLUSTER:
			case DISTRIBUTE:
			case OVERWRITE:
			case TRANSFORM:
			case REDUCE:
			case USING:
			case SERDE:
			case SERDEPROPERTIES:
			case RECORDREADER:
			case RECORDWRITER:
			case DELIMITED:
			case FIELDS:
			case TERMINATED:
			case COLLECTION:
			case ITEMS:
			case KEYS:
			case ESCAPED:
			case LINES:
			case SEPARATED:
			case FUNCTION:
			case EXTENDED:
			case REFRESH:
			case CLEAR:
			case CACHE:
			case UNCACHE:
			case LAZY:
			case FORMATTED:
			case GLOBAL:
			case TEMPORARY:
			case OPTIONS:
			case UNSET:
			case TBLPROPERTIES:
			case DBPROPERTIES:
			case BUCKETS:
			case SKEWED:
			case STORED:
			case DIRECTORIES:
			case LOCATION:
			case EXCHANGE:
			case ARCHIVE:
			case UNARCHIVE:
			case FILEFORMAT:
			case TOUCH:
			case COMPACT:
			case CONCATENATE:
			case CHANGE:
			case CASCADE:
			case RESTRICT:
			case CLUSTERED:
			case SORTED:
			case PURGE:
			case INPUTFORMAT:
			case OUTPUTFORMAT:
			case DATABASE:
			case DATABASES:
			case DFS:
			case TRUNCATE:
			case ANALYZE:
			case COMPUTE:
			case LIST:
			case STATISTICS:
			case PARTITIONED:
			case EXTERNAL:
			case DEFINED:
			case REVOKE:
			case GRANT:
			case LOCK:
			case UNLOCK:
			case MSCK:
			case REPAIR:
			case RECOVER:
			case EXPORT:
			case IMPORT:
			case LOAD:
			case ROLE:
			case ROLES:
			case COMPACTIONS:
			case PRINCIPALS:
			case TRANSACTIONS:
			case INDEX:
			case INDEXES:
			case LOCKS:
			case OPTION:
			case ANTI:
			case LOCAL:
			case INPATH:
			case CURRENT_DATE:
			case CURRENT_TIMESTAMP:
			case IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				_localctx = new WindowRefContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2074);
				((WindowRefContext)_localctx).name = identifier();
				}
				break;
			case T__0:
				_localctx = new WindowDefContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2075);
				match(T__0);
				setState(2110);
				switch (_input.LA(1)) {
				case CLUSTER:
					{
					setState(2076);
					match(CLUSTER);
					setState(2077);
					match(BY);
					setState(2078);
					((WindowDefContext)_localctx).expression = expression();
					((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
					setState(2083);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__2) {
						{
						{
						setState(2079);
						match(T__2);
						setState(2080);
						((WindowDefContext)_localctx).expression = expression();
						((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
						}
						}
						setState(2085);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
					break;
				case T__1:
				case ORDER:
				case PARTITION:
				case RANGE:
				case ROWS:
				case SORT:
				case DISTRIBUTE:
					{
					setState(2096);
					_la = _input.LA(1);
					if (_la==PARTITION || _la==DISTRIBUTE) {
						{
						setState(2086);
						_la = _input.LA(1);
						if ( !(_la==PARTITION || _la==DISTRIBUTE) ) {
						_errHandler.recoverInline(this);
						} else {
							consume();
						}
						setState(2087);
						match(BY);
						setState(2088);
						((WindowDefContext)_localctx).expression = expression();
						((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
						setState(2093);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==T__2) {
							{
							{
							setState(2089);
							match(T__2);
							setState(2090);
							((WindowDefContext)_localctx).expression = expression();
							((WindowDefContext)_localctx).partition.add(((WindowDefContext)_localctx).expression);
							}
							}
							setState(2095);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						}
					}

					setState(2108);
					_la = _input.LA(1);
					if (_la==ORDER || _la==SORT) {
						{
						setState(2098);
						_la = _input.LA(1);
						if ( !(_la==ORDER || _la==SORT) ) {
						_errHandler.recoverInline(this);
						} else {
							consume();
						}
						setState(2099);
						match(BY);
						setState(2100);
						sortItem();
						setState(2105);
						_errHandler.sync(this);
						_la = _input.LA(1);
						while (_la==T__2) {
							{
							{
							setState(2101);
							match(T__2);
							setState(2102);
							sortItem();
							}
							}
							setState(2107);
							_errHandler.sync(this);
							_la = _input.LA(1);
						}
						}
					}

					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(2113);
				_la = _input.LA(1);
				if (_la==RANGE || _la==ROWS) {
					{
					setState(2112);
					windowFrame();
					}
				}

				setState(2115);
				match(T__1);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowFrameContext extends ParserRuleContext {
		public Token frameType;
		public FrameBoundContext start;
		public FrameBoundContext end;
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public List<FrameBoundContext> frameBound() {
			return getRuleContexts(FrameBoundContext.class);
		}
		public FrameBoundContext frameBound(int i) {
			return getRuleContext(FrameBoundContext.class,i);
		}
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public WindowFrameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowFrame; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterWindowFrame(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitWindowFrame(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitWindowFrame(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowFrameContext windowFrame() throws RecognitionException {
		WindowFrameContext _localctx = new WindowFrameContext(_ctx, getState());
		enterRule(_localctx, 166, RULE_windowFrame);
		try {
			setState(2134);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,294,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2118);
				((WindowFrameContext)_localctx).frameType = match(RANGE);
				setState(2119);
				((WindowFrameContext)_localctx).start = frameBound();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2120);
				((WindowFrameContext)_localctx).frameType = match(ROWS);
				setState(2121);
				((WindowFrameContext)_localctx).start = frameBound();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2122);
				((WindowFrameContext)_localctx).frameType = match(RANGE);
				setState(2123);
				match(BETWEEN);
				setState(2124);
				((WindowFrameContext)_localctx).start = frameBound();
				setState(2125);
				match(AND);
				setState(2126);
				((WindowFrameContext)_localctx).end = frameBound();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(2128);
				((WindowFrameContext)_localctx).frameType = match(ROWS);
				setState(2129);
				match(BETWEEN);
				setState(2130);
				((WindowFrameContext)_localctx).start = frameBound();
				setState(2131);
				match(AND);
				setState(2132);
				((WindowFrameContext)_localctx).end = frameBound();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FrameBoundContext extends ParserRuleContext {
		public Token boundType;
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public FrameBoundContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_frameBound; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterFrameBound(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitFrameBound(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitFrameBound(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FrameBoundContext frameBound() throws RecognitionException {
		FrameBoundContext _localctx = new FrameBoundContext(_ctx, getState());
		enterRule(_localctx, 168, RULE_frameBound);
		int _la;
		try {
			setState(2143);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,295,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2136);
				match(UNBOUNDED);
				setState(2137);
				((FrameBoundContext)_localctx).boundType = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PRECEDING || _la==FOLLOWING) ) {
					((FrameBoundContext)_localctx).boundType = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2138);
				((FrameBoundContext)_localctx).boundType = match(CURRENT);
				setState(2139);
				match(ROW);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(2140);
				expression();
				setState(2141);
				((FrameBoundContext)_localctx).boundType = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==PRECEDING || _la==FOLLOWING) ) {
					((FrameBoundContext)_localctx).boundType = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QualifiedNameContext extends ParserRuleContext {
		public List<IdentifierContext> identifier() {
			return getRuleContexts(IdentifierContext.class);
		}
		public IdentifierContext identifier(int i) {
			return getRuleContext(IdentifierContext.class,i);
		}
		public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_qualifiedName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQualifiedName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQualifiedName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQualifiedName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QualifiedNameContext qualifiedName() throws RecognitionException {
		QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
		enterRule(_localctx, 170, RULE_qualifiedName);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2145);
			identifier();
			setState(2150);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,296,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2146);
					match(T__3);
					setState(2147);
					identifier();
					}
					} 
				}
				setState(2152);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,296,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierContext extends ParserRuleContext {
		public StrictIdentifierContext strictIdentifier() {
			return getRuleContext(StrictIdentifierContext.class,0);
		}
		public TerminalNode ANTI() { return getToken(SqlBaseParser.ANTI, 0); }
		public TerminalNode FULL() { return getToken(SqlBaseParser.FULL, 0); }
		public TerminalNode INNER() { return getToken(SqlBaseParser.INNER, 0); }
		public TerminalNode LEFT() { return getToken(SqlBaseParser.LEFT, 0); }
		public TerminalNode SEMI() { return getToken(SqlBaseParser.SEMI, 0); }
		public TerminalNode RIGHT() { return getToken(SqlBaseParser.RIGHT, 0); }
		public TerminalNode NATURAL() { return getToken(SqlBaseParser.NATURAL, 0); }
		public TerminalNode JOIN() { return getToken(SqlBaseParser.JOIN, 0); }
		public TerminalNode CROSS() { return getToken(SqlBaseParser.CROSS, 0); }
		public TerminalNode ON() { return getToken(SqlBaseParser.ON, 0); }
		public TerminalNode UNION() { return getToken(SqlBaseParser.UNION, 0); }
		public TerminalNode INTERSECT() { return getToken(SqlBaseParser.INTERSECT, 0); }
		public TerminalNode EXCEPT() { return getToken(SqlBaseParser.EXCEPT, 0); }
		public TerminalNode SETMINUS() { return getToken(SqlBaseParser.SETMINUS, 0); }
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 172, RULE_identifier);
		try {
			setState(2168);
			switch (_input.LA(1)) {
			case SELECT:
			case FROM:
			case ADD:
			case AS:
			case ALL:
			case DISTINCT:
			case WHERE:
			case GROUP:
			case BY:
			case GROUPING:
			case SETS:
			case CUBE:
			case ROLLUP:
			case ORDER:
			case HAVING:
			case LIMIT:
			case AT:
			case OR:
			case AND:
			case IN:
			case NOT:
			case NO:
			case EXISTS:
			case BETWEEN:
			case LIKE:
			case RLIKE:
			case IS:
			case NULL:
			case TRUE:
			case FALSE:
			case NULLS:
			case ASC:
			case DESC:
			case FOR:
			case INTERVAL:
			case CASE:
			case WHEN:
			case THEN:
			case ELSE:
			case END:
			case OUTER:
			case LATERAL:
			case WINDOW:
			case OVER:
			case PARTITION:
			case RANGE:
			case ROWS:
			case UNBOUNDED:
			case PRECEDING:
			case FOLLOWING:
			case CURRENT:
			case FIRST:
			case AFTER:
			case LAST:
			case ROW:
			case WITH:
			case VALUES:
			case CREATE:
			case TABLE:
			case VIEW:
			case REPLACE:
			case INSERT:
			case DELETE:
			case INTO:
			case DESCRIBE:
			case EXPLAIN:
			case FORMAT:
			case LOGICAL:
			case CODEGEN:
			case CAST:
			case SHOW:
			case TABLES:
			case COLUMNS:
			case COLUMN:
			case USE:
			case PARTITIONS:
			case FUNCTIONS:
			case DROP:
			case TO:
			case TABLESAMPLE:
			case STRATIFY:
			case ALTER:
			case RENAME:
			case ARRAY:
			case MAP:
			case STRUCT:
			case COMMENT:
			case SET:
			case RESET:
			case DATA:
			case START:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case MACRO:
			case IF:
			case DIV:
			case PERCENTLIT:
			case BUCKET:
			case OUT:
			case OF:
			case SORT:
			case CLUSTER:
			case DISTRIBUTE:
			case OVERWRITE:
			case TRANSFORM:
			case REDUCE:
			case USING:
			case SERDE:
			case SERDEPROPERTIES:
			case RECORDREADER:
			case RECORDWRITER:
			case DELIMITED:
			case FIELDS:
			case TERMINATED:
			case COLLECTION:
			case ITEMS:
			case KEYS:
			case ESCAPED:
			case LINES:
			case SEPARATED:
			case FUNCTION:
			case EXTENDED:
			case REFRESH:
			case CLEAR:
			case CACHE:
			case UNCACHE:
			case LAZY:
			case FORMATTED:
			case GLOBAL:
			case TEMPORARY:
			case OPTIONS:
			case UNSET:
			case TBLPROPERTIES:
			case DBPROPERTIES:
			case BUCKETS:
			case SKEWED:
			case STORED:
			case DIRECTORIES:
			case LOCATION:
			case EXCHANGE:
			case ARCHIVE:
			case UNARCHIVE:
			case FILEFORMAT:
			case TOUCH:
			case COMPACT:
			case CONCATENATE:
			case CHANGE:
			case CASCADE:
			case RESTRICT:
			case CLUSTERED:
			case SORTED:
			case PURGE:
			case INPUTFORMAT:
			case OUTPUTFORMAT:
			case DATABASE:
			case DATABASES:
			case DFS:
			case TRUNCATE:
			case ANALYZE:
			case COMPUTE:
			case LIST:
			case STATISTICS:
			case PARTITIONED:
			case EXTERNAL:
			case DEFINED:
			case REVOKE:
			case GRANT:
			case LOCK:
			case UNLOCK:
			case MSCK:
			case REPAIR:
			case RECOVER:
			case EXPORT:
			case IMPORT:
			case LOAD:
			case ROLE:
			case ROLES:
			case COMPACTIONS:
			case PRINCIPALS:
			case TRANSACTIONS:
			case INDEX:
			case INDEXES:
			case LOCKS:
			case OPTION:
			case LOCAL:
			case INPATH:
			case CURRENT_DATE:
			case CURRENT_TIMESTAMP:
			case IDENTIFIER:
			case BACKQUOTED_IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(2153);
				strictIdentifier();
				}
				break;
			case ANTI:
				enterOuterAlt(_localctx, 2);
				{
				setState(2154);
				match(ANTI);
				}
				break;
			case FULL:
				enterOuterAlt(_localctx, 3);
				{
				setState(2155);
				match(FULL);
				}
				break;
			case INNER:
				enterOuterAlt(_localctx, 4);
				{
				setState(2156);
				match(INNER);
				}
				break;
			case LEFT:
				enterOuterAlt(_localctx, 5);
				{
				setState(2157);
				match(LEFT);
				}
				break;
			case SEMI:
				enterOuterAlt(_localctx, 6);
				{
				setState(2158);
				match(SEMI);
				}
				break;
			case RIGHT:
				enterOuterAlt(_localctx, 7);
				{
				setState(2159);
				match(RIGHT);
				}
				break;
			case NATURAL:
				enterOuterAlt(_localctx, 8);
				{
				setState(2160);
				match(NATURAL);
				}
				break;
			case JOIN:
				enterOuterAlt(_localctx, 9);
				{
				setState(2161);
				match(JOIN);
				}
				break;
			case CROSS:
				enterOuterAlt(_localctx, 10);
				{
				setState(2162);
				match(CROSS);
				}
				break;
			case ON:
				enterOuterAlt(_localctx, 11);
				{
				setState(2163);
				match(ON);
				}
				break;
			case UNION:
				enterOuterAlt(_localctx, 12);
				{
				setState(2164);
				match(UNION);
				}
				break;
			case INTERSECT:
				enterOuterAlt(_localctx, 13);
				{
				setState(2165);
				match(INTERSECT);
				}
				break;
			case EXCEPT:
				enterOuterAlt(_localctx, 14);
				{
				setState(2166);
				match(EXCEPT);
				}
				break;
			case SETMINUS:
				enterOuterAlt(_localctx, 15);
				{
				setState(2167);
				match(SETMINUS);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StrictIdentifierContext extends ParserRuleContext {
		public StrictIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_strictIdentifier; }
	 
		public StrictIdentifierContext() { }
		public void copyFrom(StrictIdentifierContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class UnquotedIdentifierContext extends StrictIdentifierContext {
		public TerminalNode IDENTIFIER() { return getToken(SqlBaseParser.IDENTIFIER, 0); }
		public NonReservedContext nonReserved() {
			return getRuleContext(NonReservedContext.class,0);
		}
		public UnquotedIdentifierContext(StrictIdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterUnquotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitUnquotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitUnquotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class QuotedIdentifierAlternativeContext extends StrictIdentifierContext {
		public QuotedIdentifierContext quotedIdentifier() {
			return getRuleContext(QuotedIdentifierContext.class,0);
		}
		public QuotedIdentifierAlternativeContext(StrictIdentifierContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuotedIdentifierAlternative(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuotedIdentifierAlternative(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuotedIdentifierAlternative(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StrictIdentifierContext strictIdentifier() throws RecognitionException {
		StrictIdentifierContext _localctx = new StrictIdentifierContext(_ctx, getState());
		enterRule(_localctx, 174, RULE_strictIdentifier);
		try {
			setState(2173);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2170);
				match(IDENTIFIER);
				}
				break;
			case BACKQUOTED_IDENTIFIER:
				_localctx = new QuotedIdentifierAlternativeContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2171);
				quotedIdentifier();
				}
				break;
			case SELECT:
			case FROM:
			case ADD:
			case AS:
			case ALL:
			case DISTINCT:
			case WHERE:
			case GROUP:
			case BY:
			case GROUPING:
			case SETS:
			case CUBE:
			case ROLLUP:
			case ORDER:
			case HAVING:
			case LIMIT:
			case AT:
			case OR:
			case AND:
			case IN:
			case NOT:
			case NO:
			case EXISTS:
			case BETWEEN:
			case LIKE:
			case RLIKE:
			case IS:
			case NULL:
			case TRUE:
			case FALSE:
			case NULLS:
			case ASC:
			case DESC:
			case FOR:
			case INTERVAL:
			case CASE:
			case WHEN:
			case THEN:
			case ELSE:
			case END:
			case OUTER:
			case LATERAL:
			case WINDOW:
			case OVER:
			case PARTITION:
			case RANGE:
			case ROWS:
			case UNBOUNDED:
			case PRECEDING:
			case FOLLOWING:
			case CURRENT:
			case FIRST:
			case AFTER:
			case LAST:
			case ROW:
			case WITH:
			case VALUES:
			case CREATE:
			case TABLE:
			case VIEW:
			case REPLACE:
			case INSERT:
			case DELETE:
			case INTO:
			case DESCRIBE:
			case EXPLAIN:
			case FORMAT:
			case LOGICAL:
			case CODEGEN:
			case CAST:
			case SHOW:
			case TABLES:
			case COLUMNS:
			case COLUMN:
			case USE:
			case PARTITIONS:
			case FUNCTIONS:
			case DROP:
			case TO:
			case TABLESAMPLE:
			case STRATIFY:
			case ALTER:
			case RENAME:
			case ARRAY:
			case MAP:
			case STRUCT:
			case COMMENT:
			case SET:
			case RESET:
			case DATA:
			case START:
			case TRANSACTION:
			case COMMIT:
			case ROLLBACK:
			case MACRO:
			case IF:
			case DIV:
			case PERCENTLIT:
			case BUCKET:
			case OUT:
			case OF:
			case SORT:
			case CLUSTER:
			case DISTRIBUTE:
			case OVERWRITE:
			case TRANSFORM:
			case REDUCE:
			case USING:
			case SERDE:
			case SERDEPROPERTIES:
			case RECORDREADER:
			case RECORDWRITER:
			case DELIMITED:
			case FIELDS:
			case TERMINATED:
			case COLLECTION:
			case ITEMS:
			case KEYS:
			case ESCAPED:
			case LINES:
			case SEPARATED:
			case FUNCTION:
			case EXTENDED:
			case REFRESH:
			case CLEAR:
			case CACHE:
			case UNCACHE:
			case LAZY:
			case FORMATTED:
			case GLOBAL:
			case TEMPORARY:
			case OPTIONS:
			case UNSET:
			case TBLPROPERTIES:
			case DBPROPERTIES:
			case BUCKETS:
			case SKEWED:
			case STORED:
			case DIRECTORIES:
			case LOCATION:
			case EXCHANGE:
			case ARCHIVE:
			case UNARCHIVE:
			case FILEFORMAT:
			case TOUCH:
			case COMPACT:
			case CONCATENATE:
			case CHANGE:
			case CASCADE:
			case RESTRICT:
			case CLUSTERED:
			case SORTED:
			case PURGE:
			case INPUTFORMAT:
			case OUTPUTFORMAT:
			case DATABASE:
			case DATABASES:
			case DFS:
			case TRUNCATE:
			case ANALYZE:
			case COMPUTE:
			case LIST:
			case STATISTICS:
			case PARTITIONED:
			case EXTERNAL:
			case DEFINED:
			case REVOKE:
			case GRANT:
			case LOCK:
			case UNLOCK:
			case MSCK:
			case REPAIR:
			case RECOVER:
			case EXPORT:
			case IMPORT:
			case LOAD:
			case ROLE:
			case ROLES:
			case COMPACTIONS:
			case PRINCIPALS:
			case TRANSACTIONS:
			case INDEX:
			case INDEXES:
			case LOCKS:
			case OPTION:
			case LOCAL:
			case INPATH:
			case CURRENT_DATE:
			case CURRENT_TIMESTAMP:
				_localctx = new UnquotedIdentifierContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2172);
				nonReserved();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QuotedIdentifierContext extends ParserRuleContext {
		public TerminalNode BACKQUOTED_IDENTIFIER() { return getToken(SqlBaseParser.BACKQUOTED_IDENTIFIER, 0); }
		public QuotedIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quotedIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterQuotedIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitQuotedIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitQuotedIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QuotedIdentifierContext quotedIdentifier() throws RecognitionException {
		QuotedIdentifierContext _localctx = new QuotedIdentifierContext(_ctx, getState());
		enterRule(_localctx, 176, RULE_quotedIdentifier);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2175);
			match(BACKQUOTED_IDENTIFIER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NumberContext extends ParserRuleContext {
		public NumberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_number; }
	 
		public NumberContext() { }
		public void copyFrom(NumberContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class BigIntLiteralContext extends NumberContext {
		public TerminalNode BIGINT_LITERAL() { return getToken(SqlBaseParser.BIGINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public BigIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBigIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBigIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBigIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TinyIntLiteralContext extends NumberContext {
		public TerminalNode TINYINT_LITERAL() { return getToken(SqlBaseParser.TINYINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public TinyIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterTinyIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitTinyIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitTinyIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class BigDecimalLiteralContext extends NumberContext {
		public TerminalNode BIGDECIMAL_LITERAL() { return getToken(SqlBaseParser.BIGDECIMAL_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public BigDecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterBigDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitBigDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitBigDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SmallIntLiteralContext extends NumberContext {
		public TerminalNode SMALLINT_LITERAL() { return getToken(SqlBaseParser.SMALLINT_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public SmallIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterSmallIntLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitSmallIntLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitSmallIntLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DecimalLiteralContext extends NumberContext {
		public TerminalNode DECIMAL_VALUE() { return getToken(SqlBaseParser.DECIMAL_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public DecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDecimalLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDecimalLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDecimalLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class DoubleLiteralContext extends NumberContext {
		public TerminalNode DOUBLE_LITERAL() { return getToken(SqlBaseParser.DOUBLE_LITERAL, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public DoubleLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterDoubleLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitDoubleLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitDoubleLiteral(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class IntegerLiteralContext extends NumberContext {
		public TerminalNode INTEGER_VALUE() { return getToken(SqlBaseParser.INTEGER_VALUE, 0); }
		public TerminalNode MINUS() { return getToken(SqlBaseParser.MINUS, 0); }
		public IntegerLiteralContext(NumberContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterIntegerLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitIntegerLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitIntegerLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NumberContext number() throws RecognitionException {
		NumberContext _localctx = new NumberContext(_ctx, getState());
		enterRule(_localctx, 178, RULE_number);
		int _la;
		try {
			setState(2205);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,306,_ctx) ) {
			case 1:
				_localctx = new DecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2178);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2177);
					match(MINUS);
					}
				}

				setState(2180);
				match(DECIMAL_VALUE);
				}
				break;
			case 2:
				_localctx = new IntegerLiteralContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2182);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2181);
					match(MINUS);
					}
				}

				setState(2184);
				match(INTEGER_VALUE);
				}
				break;
			case 3:
				_localctx = new BigIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2186);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2185);
					match(MINUS);
					}
				}

				setState(2188);
				match(BIGINT_LITERAL);
				}
				break;
			case 4:
				_localctx = new SmallIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2190);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2189);
					match(MINUS);
					}
				}

				setState(2192);
				match(SMALLINT_LITERAL);
				}
				break;
			case 5:
				_localctx = new TinyIntLiteralContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(2194);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2193);
					match(MINUS);
					}
				}

				setState(2196);
				match(TINYINT_LITERAL);
				}
				break;
			case 6:
				_localctx = new DoubleLiteralContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(2198);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2197);
					match(MINUS);
					}
				}

				setState(2200);
				match(DOUBLE_LITERAL);
				}
				break;
			case 7:
				_localctx = new BigDecimalLiteralContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(2202);
				_la = _input.LA(1);
				if (_la==MINUS) {
					{
					setState(2201);
					match(MINUS);
					}
				}

				setState(2204);
				match(BIGDECIMAL_LITERAL);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NonReservedContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(SqlBaseParser.SHOW, 0); }
		public TerminalNode TABLES() { return getToken(SqlBaseParser.TABLES, 0); }
		public TerminalNode COLUMNS() { return getToken(SqlBaseParser.COLUMNS, 0); }
		public TerminalNode COLUMN() { return getToken(SqlBaseParser.COLUMN, 0); }
		public TerminalNode PARTITIONS() { return getToken(SqlBaseParser.PARTITIONS, 0); }
		public TerminalNode FUNCTIONS() { return getToken(SqlBaseParser.FUNCTIONS, 0); }
		public TerminalNode DATABASES() { return getToken(SqlBaseParser.DATABASES, 0); }
		public TerminalNode ADD() { return getToken(SqlBaseParser.ADD, 0); }
		public TerminalNode OVER() { return getToken(SqlBaseParser.OVER, 0); }
		public TerminalNode PARTITION() { return getToken(SqlBaseParser.PARTITION, 0); }
		public TerminalNode RANGE() { return getToken(SqlBaseParser.RANGE, 0); }
		public TerminalNode ROWS() { return getToken(SqlBaseParser.ROWS, 0); }
		public TerminalNode PRECEDING() { return getToken(SqlBaseParser.PRECEDING, 0); }
		public TerminalNode FOLLOWING() { return getToken(SqlBaseParser.FOLLOWING, 0); }
		public TerminalNode CURRENT() { return getToken(SqlBaseParser.CURRENT, 0); }
		public TerminalNode ROW() { return getToken(SqlBaseParser.ROW, 0); }
		public TerminalNode LAST() { return getToken(SqlBaseParser.LAST, 0); }
		public TerminalNode FIRST() { return getToken(SqlBaseParser.FIRST, 0); }
		public TerminalNode AFTER() { return getToken(SqlBaseParser.AFTER, 0); }
		public TerminalNode MAP() { return getToken(SqlBaseParser.MAP, 0); }
		public TerminalNode ARRAY() { return getToken(SqlBaseParser.ARRAY, 0); }
		public TerminalNode STRUCT() { return getToken(SqlBaseParser.STRUCT, 0); }
		public TerminalNode LATERAL() { return getToken(SqlBaseParser.LATERAL, 0); }
		public TerminalNode WINDOW() { return getToken(SqlBaseParser.WINDOW, 0); }
		public TerminalNode REDUCE() { return getToken(SqlBaseParser.REDUCE, 0); }
		public TerminalNode TRANSFORM() { return getToken(SqlBaseParser.TRANSFORM, 0); }
		public TerminalNode USING() { return getToken(SqlBaseParser.USING, 0); }
		public TerminalNode SERDE() { return getToken(SqlBaseParser.SERDE, 0); }
		public TerminalNode SERDEPROPERTIES() { return getToken(SqlBaseParser.SERDEPROPERTIES, 0); }
		public TerminalNode RECORDREADER() { return getToken(SqlBaseParser.RECORDREADER, 0); }
		public TerminalNode DELIMITED() { return getToken(SqlBaseParser.DELIMITED, 0); }
		public TerminalNode FIELDS() { return getToken(SqlBaseParser.FIELDS, 0); }
		public TerminalNode TERMINATED() { return getToken(SqlBaseParser.TERMINATED, 0); }
		public TerminalNode COLLECTION() { return getToken(SqlBaseParser.COLLECTION, 0); }
		public TerminalNode ITEMS() { return getToken(SqlBaseParser.ITEMS, 0); }
		public TerminalNode KEYS() { return getToken(SqlBaseParser.KEYS, 0); }
		public TerminalNode ESCAPED() { return getToken(SqlBaseParser.ESCAPED, 0); }
		public TerminalNode LINES() { return getToken(SqlBaseParser.LINES, 0); }
		public TerminalNode SEPARATED() { return getToken(SqlBaseParser.SEPARATED, 0); }
		public TerminalNode EXTENDED() { return getToken(SqlBaseParser.EXTENDED, 0); }
		public TerminalNode REFRESH() { return getToken(SqlBaseParser.REFRESH, 0); }
		public TerminalNode CLEAR() { return getToken(SqlBaseParser.CLEAR, 0); }
		public TerminalNode CACHE() { return getToken(SqlBaseParser.CACHE, 0); }
		public TerminalNode UNCACHE() { return getToken(SqlBaseParser.UNCACHE, 0); }
		public TerminalNode LAZY() { return getToken(SqlBaseParser.LAZY, 0); }
		public TerminalNode GLOBAL() { return getToken(SqlBaseParser.GLOBAL, 0); }
		public TerminalNode TEMPORARY() { return getToken(SqlBaseParser.TEMPORARY, 0); }
		public TerminalNode OPTIONS() { return getToken(SqlBaseParser.OPTIONS, 0); }
		public TerminalNode GROUPING() { return getToken(SqlBaseParser.GROUPING, 0); }
		public TerminalNode CUBE() { return getToken(SqlBaseParser.CUBE, 0); }
		public TerminalNode ROLLUP() { return getToken(SqlBaseParser.ROLLUP, 0); }
		public TerminalNode EXPLAIN() { return getToken(SqlBaseParser.EXPLAIN, 0); }
		public TerminalNode FORMAT() { return getToken(SqlBaseParser.FORMAT, 0); }
		public TerminalNode LOGICAL() { return getToken(SqlBaseParser.LOGICAL, 0); }
		public TerminalNode FORMATTED() { return getToken(SqlBaseParser.FORMATTED, 0); }
		public TerminalNode CODEGEN() { return getToken(SqlBaseParser.CODEGEN, 0); }
		public TerminalNode TABLESAMPLE() { return getToken(SqlBaseParser.TABLESAMPLE, 0); }
		public TerminalNode USE() { return getToken(SqlBaseParser.USE, 0); }
		public List<TerminalNode> TO() { return getTokens(SqlBaseParser.TO); }
		public TerminalNode TO(int i) {
			return getToken(SqlBaseParser.TO, i);
		}
		public TerminalNode BUCKET() { return getToken(SqlBaseParser.BUCKET, 0); }
		public TerminalNode PERCENTLIT() { return getToken(SqlBaseParser.PERCENTLIT, 0); }
		public TerminalNode OUT() { return getToken(SqlBaseParser.OUT, 0); }
		public TerminalNode OF() { return getToken(SqlBaseParser.OF, 0); }
		public TerminalNode SET() { return getToken(SqlBaseParser.SET, 0); }
		public TerminalNode RESET() { return getToken(SqlBaseParser.RESET, 0); }
		public TerminalNode VIEW() { return getToken(SqlBaseParser.VIEW, 0); }
		public TerminalNode REPLACE() { return getToken(SqlBaseParser.REPLACE, 0); }
		public TerminalNode IF() { return getToken(SqlBaseParser.IF, 0); }
		public TerminalNode NO() { return getToken(SqlBaseParser.NO, 0); }
		public TerminalNode DATA() { return getToken(SqlBaseParser.DATA, 0); }
		public TerminalNode START() { return getToken(SqlBaseParser.START, 0); }
		public TerminalNode TRANSACTION() { return getToken(SqlBaseParser.TRANSACTION, 0); }
		public TerminalNode COMMIT() { return getToken(SqlBaseParser.COMMIT, 0); }
		public TerminalNode ROLLBACK() { return getToken(SqlBaseParser.ROLLBACK, 0); }
		public TerminalNode SORT() { return getToken(SqlBaseParser.SORT, 0); }
		public TerminalNode CLUSTER() { return getToken(SqlBaseParser.CLUSTER, 0); }
		public TerminalNode DISTRIBUTE() { return getToken(SqlBaseParser.DISTRIBUTE, 0); }
		public TerminalNode UNSET() { return getToken(SqlBaseParser.UNSET, 0); }
		public TerminalNode TBLPROPERTIES() { return getToken(SqlBaseParser.TBLPROPERTIES, 0); }
		public TerminalNode SKEWED() { return getToken(SqlBaseParser.SKEWED, 0); }
		public TerminalNode STORED() { return getToken(SqlBaseParser.STORED, 0); }
		public TerminalNode DIRECTORIES() { return getToken(SqlBaseParser.DIRECTORIES, 0); }
		public TerminalNode LOCATION() { return getToken(SqlBaseParser.LOCATION, 0); }
		public TerminalNode EXCHANGE() { return getToken(SqlBaseParser.EXCHANGE, 0); }
		public TerminalNode ARCHIVE() { return getToken(SqlBaseParser.ARCHIVE, 0); }
		public TerminalNode UNARCHIVE() { return getToken(SqlBaseParser.UNARCHIVE, 0); }
		public TerminalNode FILEFORMAT() { return getToken(SqlBaseParser.FILEFORMAT, 0); }
		public TerminalNode TOUCH() { return getToken(SqlBaseParser.TOUCH, 0); }
		public TerminalNode COMPACT() { return getToken(SqlBaseParser.COMPACT, 0); }
		public TerminalNode CONCATENATE() { return getToken(SqlBaseParser.CONCATENATE, 0); }
		public TerminalNode CHANGE() { return getToken(SqlBaseParser.CHANGE, 0); }
		public TerminalNode CASCADE() { return getToken(SqlBaseParser.CASCADE, 0); }
		public TerminalNode RESTRICT() { return getToken(SqlBaseParser.RESTRICT, 0); }
		public TerminalNode BUCKETS() { return getToken(SqlBaseParser.BUCKETS, 0); }
		public TerminalNode CLUSTERED() { return getToken(SqlBaseParser.CLUSTERED, 0); }
		public TerminalNode SORTED() { return getToken(SqlBaseParser.SORTED, 0); }
		public TerminalNode PURGE() { return getToken(SqlBaseParser.PURGE, 0); }
		public TerminalNode INPUTFORMAT() { return getToken(SqlBaseParser.INPUTFORMAT, 0); }
		public TerminalNode OUTPUTFORMAT() { return getToken(SqlBaseParser.OUTPUTFORMAT, 0); }
		public TerminalNode DBPROPERTIES() { return getToken(SqlBaseParser.DBPROPERTIES, 0); }
		public TerminalNode DFS() { return getToken(SqlBaseParser.DFS, 0); }
		public TerminalNode TRUNCATE() { return getToken(SqlBaseParser.TRUNCATE, 0); }
		public TerminalNode COMPUTE() { return getToken(SqlBaseParser.COMPUTE, 0); }
		public TerminalNode LIST() { return getToken(SqlBaseParser.LIST, 0); }
		public TerminalNode STATISTICS() { return getToken(SqlBaseParser.STATISTICS, 0); }
		public TerminalNode ANALYZE() { return getToken(SqlBaseParser.ANALYZE, 0); }
		public TerminalNode PARTITIONED() { return getToken(SqlBaseParser.PARTITIONED, 0); }
		public TerminalNode EXTERNAL() { return getToken(SqlBaseParser.EXTERNAL, 0); }
		public TerminalNode DEFINED() { return getToken(SqlBaseParser.DEFINED, 0); }
		public TerminalNode RECORDWRITER() { return getToken(SqlBaseParser.RECORDWRITER, 0); }
		public TerminalNode REVOKE() { return getToken(SqlBaseParser.REVOKE, 0); }
		public TerminalNode GRANT() { return getToken(SqlBaseParser.GRANT, 0); }
		public TerminalNode LOCK() { return getToken(SqlBaseParser.LOCK, 0); }
		public TerminalNode UNLOCK() { return getToken(SqlBaseParser.UNLOCK, 0); }
		public TerminalNode MSCK() { return getToken(SqlBaseParser.MSCK, 0); }
		public TerminalNode REPAIR() { return getToken(SqlBaseParser.REPAIR, 0); }
		public TerminalNode RECOVER() { return getToken(SqlBaseParser.RECOVER, 0); }
		public TerminalNode EXPORT() { return getToken(SqlBaseParser.EXPORT, 0); }
		public TerminalNode IMPORT() { return getToken(SqlBaseParser.IMPORT, 0); }
		public TerminalNode LOAD() { return getToken(SqlBaseParser.LOAD, 0); }
		public TerminalNode VALUES() { return getToken(SqlBaseParser.VALUES, 0); }
		public TerminalNode COMMENT() { return getToken(SqlBaseParser.COMMENT, 0); }
		public TerminalNode ROLE() { return getToken(SqlBaseParser.ROLE, 0); }
		public TerminalNode ROLES() { return getToken(SqlBaseParser.ROLES, 0); }
		public TerminalNode COMPACTIONS() { return getToken(SqlBaseParser.COMPACTIONS, 0); }
		public TerminalNode PRINCIPALS() { return getToken(SqlBaseParser.PRINCIPALS, 0); }
		public TerminalNode TRANSACTIONS() { return getToken(SqlBaseParser.TRANSACTIONS, 0); }
		public TerminalNode INDEX() { return getToken(SqlBaseParser.INDEX, 0); }
		public TerminalNode INDEXES() { return getToken(SqlBaseParser.INDEXES, 0); }
		public TerminalNode LOCKS() { return getToken(SqlBaseParser.LOCKS, 0); }
		public TerminalNode OPTION() { return getToken(SqlBaseParser.OPTION, 0); }
		public TerminalNode LOCAL() { return getToken(SqlBaseParser.LOCAL, 0); }
		public TerminalNode INPATH() { return getToken(SqlBaseParser.INPATH, 0); }
		public TerminalNode ASC() { return getToken(SqlBaseParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SqlBaseParser.DESC, 0); }
		public TerminalNode LIMIT() { return getToken(SqlBaseParser.LIMIT, 0); }
		public TerminalNode RENAME() { return getToken(SqlBaseParser.RENAME, 0); }
		public TerminalNode SETS() { return getToken(SqlBaseParser.SETS, 0); }
		public TerminalNode AT() { return getToken(SqlBaseParser.AT, 0); }
		public TerminalNode NULLS() { return getToken(SqlBaseParser.NULLS, 0); }
		public TerminalNode OVERWRITE() { return getToken(SqlBaseParser.OVERWRITE, 0); }
		public TerminalNode ALL() { return getToken(SqlBaseParser.ALL, 0); }
		public TerminalNode ALTER() { return getToken(SqlBaseParser.ALTER, 0); }
		public TerminalNode AS() { return getToken(SqlBaseParser.AS, 0); }
		public TerminalNode BETWEEN() { return getToken(SqlBaseParser.BETWEEN, 0); }
		public TerminalNode BY() { return getToken(SqlBaseParser.BY, 0); }
		public TerminalNode CREATE() { return getToken(SqlBaseParser.CREATE, 0); }
		public TerminalNode DELETE() { return getToken(SqlBaseParser.DELETE, 0); }
		public TerminalNode DESCRIBE() { return getToken(SqlBaseParser.DESCRIBE, 0); }
		public TerminalNode DROP() { return getToken(SqlBaseParser.DROP, 0); }
		public TerminalNode EXISTS() { return getToken(SqlBaseParser.EXISTS, 0); }
		public TerminalNode FALSE() { return getToken(SqlBaseParser.FALSE, 0); }
		public TerminalNode FOR() { return getToken(SqlBaseParser.FOR, 0); }
		public TerminalNode GROUP() { return getToken(SqlBaseParser.GROUP, 0); }
		public TerminalNode IN() { return getToken(SqlBaseParser.IN, 0); }
		public TerminalNode INSERT() { return getToken(SqlBaseParser.INSERT, 0); }
		public TerminalNode INTO() { return getToken(SqlBaseParser.INTO, 0); }
		public TerminalNode IS() { return getToken(SqlBaseParser.IS, 0); }
		public TerminalNode LIKE() { return getToken(SqlBaseParser.LIKE, 0); }
		public TerminalNode NULL() { return getToken(SqlBaseParser.NULL, 0); }
		public TerminalNode ORDER() { return getToken(SqlBaseParser.ORDER, 0); }
		public TerminalNode OUTER() { return getToken(SqlBaseParser.OUTER, 0); }
		public List<TerminalNode> TABLE() { return getTokens(SqlBaseParser.TABLE); }
		public TerminalNode TABLE(int i) {
			return getToken(SqlBaseParser.TABLE, i);
		}
		public TerminalNode TRUE() { return getToken(SqlBaseParser.TRUE, 0); }
		public List<TerminalNode> WITH() { return getTokens(SqlBaseParser.WITH); }
		public TerminalNode WITH(int i) {
			return getToken(SqlBaseParser.WITH, i);
		}
		public TerminalNode RLIKE() { return getToken(SqlBaseParser.RLIKE, 0); }
		public TerminalNode AND() { return getToken(SqlBaseParser.AND, 0); }
		public TerminalNode CASE() { return getToken(SqlBaseParser.CASE, 0); }
		public TerminalNode CAST() { return getToken(SqlBaseParser.CAST, 0); }
		public TerminalNode DISTINCT() { return getToken(SqlBaseParser.DISTINCT, 0); }
		public TerminalNode DIV() { return getToken(SqlBaseParser.DIV, 0); }
		public TerminalNode ELSE() { return getToken(SqlBaseParser.ELSE, 0); }
		public TerminalNode END() { return getToken(SqlBaseParser.END, 0); }
		public TerminalNode FUNCTION() { return getToken(SqlBaseParser.FUNCTION, 0); }
		public TerminalNode INTERVAL() { return getToken(SqlBaseParser.INTERVAL, 0); }
		public TerminalNode MACRO() { return getToken(SqlBaseParser.MACRO, 0); }
		public TerminalNode OR() { return getToken(SqlBaseParser.OR, 0); }
		public TerminalNode STRATIFY() { return getToken(SqlBaseParser.STRATIFY, 0); }
		public TerminalNode THEN() { return getToken(SqlBaseParser.THEN, 0); }
		public TerminalNode UNBOUNDED() { return getToken(SqlBaseParser.UNBOUNDED, 0); }
		public TerminalNode WHEN() { return getToken(SqlBaseParser.WHEN, 0); }
		public TerminalNode DATABASE() { return getToken(SqlBaseParser.DATABASE, 0); }
		public TerminalNode SELECT() { return getToken(SqlBaseParser.SELECT, 0); }
		public TerminalNode FROM() { return getToken(SqlBaseParser.FROM, 0); }
		public TerminalNode WHERE() { return getToken(SqlBaseParser.WHERE, 0); }
		public TerminalNode HAVING() { return getToken(SqlBaseParser.HAVING, 0); }
		public TerminalNode NOT() { return getToken(SqlBaseParser.NOT, 0); }
		public TerminalNode CURRENT_DATE() { return getToken(SqlBaseParser.CURRENT_DATE, 0); }
		public TerminalNode CURRENT_TIMESTAMP() { return getToken(SqlBaseParser.CURRENT_TIMESTAMP, 0); }
		public NonReservedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nonReserved; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).enterNonReserved(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof SqlBaseListener ) ((SqlBaseListener)listener).exitNonReserved(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof SqlBaseVisitor ) return ((SqlBaseVisitor<? extends T>)visitor).visitNonReserved(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NonReservedContext nonReserved() throws RecognitionException {
		NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
		enterRule(_localctx, 180, RULE_nonReserved);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2207);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << SELECT) | (1L << FROM) | (1L << ADD) | (1L << AS) | (1L << ALL) | (1L << DISTINCT) | (1L << WHERE) | (1L << GROUP) | (1L << BY) | (1L << GROUPING) | (1L << SETS) | (1L << CUBE) | (1L << ROLLUP) | (1L << ORDER) | (1L << HAVING) | (1L << LIMIT) | (1L << AT) | (1L << OR) | (1L << AND) | (1L << IN) | (1L << NOT) | (1L << NO) | (1L << EXISTS) | (1L << BETWEEN) | (1L << LIKE) | (1L << RLIKE) | (1L << IS) | (1L << NULL) | (1L << TRUE) | (1L << FALSE) | (1L << NULLS) | (1L << ASC) | (1L << DESC) | (1L << FOR) | (1L << INTERVAL) | (1L << CASE) | (1L << WHEN) | (1L << THEN) | (1L << ELSE) | (1L << END) | (1L << OUTER) | (1L << LATERAL) | (1L << WINDOW) | (1L << OVER) | (1L << PARTITION) | (1L << RANGE) | (1L << ROWS))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (UNBOUNDED - 64)) | (1L << (PRECEDING - 64)) | (1L << (FOLLOWING - 64)) | (1L << (CURRENT - 64)) | (1L << (FIRST - 64)) | (1L << (AFTER - 64)) | (1L << (LAST - 64)) | (1L << (ROW - 64)) | (1L << (WITH - 64)) | (1L << (VALUES - 64)) | (1L << (CREATE - 64)) | (1L << (TABLE - 64)) | (1L << (VIEW - 64)) | (1L << (REPLACE - 64)) | (1L << (INSERT - 64)) | (1L << (DELETE - 64)) | (1L << (INTO - 64)) | (1L << (DESCRIBE - 64)) | (1L << (EXPLAIN - 64)) | (1L << (FORMAT - 64)) | (1L << (LOGICAL - 64)) | (1L << (CODEGEN - 64)) | (1L << (CAST - 64)) | (1L << (SHOW - 64)) | (1L << (TABLES - 64)) | (1L << (COLUMNS - 64)) | (1L << (COLUMN - 64)) | (1L << (USE - 64)) | (1L << (PARTITIONS - 64)) | (1L << (FUNCTIONS - 64)) | (1L << (DROP - 64)) | (1L << (TO - 64)) | (1L << (TABLESAMPLE - 64)) | (1L << (STRATIFY - 64)) | (1L << (ALTER - 64)) | (1L << (RENAME - 64)) | (1L << (ARRAY - 64)) | (1L << (MAP - 64)) | (1L << (STRUCT - 64)) | (1L << (COMMENT - 64)) | (1L << (SET - 64)) | (1L << (RESET - 64)) | (1L << (DATA - 64)) | (1L << (START - 64)) | (1L << (TRANSACTION - 64)) | (1L << (COMMIT - 64)) | (1L << (ROLLBACK - 64)) | (1L << (MACRO - 64)) | (1L << (IF - 64)))) != 0) || ((((_la - 130)) & ~0x3f) == 0 && ((1L << (_la - 130)) & ((1L << (DIV - 130)) | (1L << (PERCENTLIT - 130)) | (1L << (BUCKET - 130)) | (1L << (OUT - 130)) | (1L << (OF - 130)) | (1L << (SORT - 130)) | (1L << (CLUSTER - 130)) | (1L << (DISTRIBUTE - 130)) | (1L << (OVERWRITE - 130)) | (1L << (TRANSFORM - 130)) | (1L << (REDUCE - 130)) | (1L << (USING - 130)) | (1L << (SERDE - 130)) | (1L << (SERDEPROPERTIES - 130)) | (1L << (RECORDREADER - 130)) | (1L << (RECORDWRITER - 130)) | (1L << (DELIMITED - 130)) | (1L << (FIELDS - 130)) | (1L << (TERMINATED - 130)) | (1L << (COLLECTION - 130)) | (1L << (ITEMS - 130)) | (1L << (KEYS - 130)) | (1L << (ESCAPED - 130)) | (1L << (LINES - 130)) | (1L << (SEPARATED - 130)) | (1L << (FUNCTION - 130)) | (1L << (EXTENDED - 130)) | (1L << (REFRESH - 130)) | (1L << (CLEAR - 130)) | (1L << (CACHE - 130)) | (1L << (UNCACHE - 130)) | (1L << (LAZY - 130)) | (1L << (FORMATTED - 130)) | (1L << (GLOBAL - 130)) | (1L << (TEMPORARY - 130)) | (1L << (OPTIONS - 130)) | (1L << (UNSET - 130)) | (1L << (TBLPROPERTIES - 130)) | (1L << (DBPROPERTIES - 130)) | (1L << (BUCKETS - 130)) | (1L << (SKEWED - 130)) | (1L << (STORED - 130)) | (1L << (DIRECTORIES - 130)) | (1L << (LOCATION - 130)) | (1L << (EXCHANGE - 130)) | (1L << (ARCHIVE - 130)) | (1L << (UNARCHIVE - 130)) | (1L << (FILEFORMAT - 130)) | (1L << (TOUCH - 130)) | (1L << (COMPACT - 130)) | (1L << (CONCATENATE - 130)) | (1L << (CHANGE - 130)) | (1L << (CASCADE - 130)) | (1L << (RESTRICT - 130)) | (1L << (CLUSTERED - 130)) | (1L << (SORTED - 130)) | (1L << (PURGE - 130)) | (1L << (INPUTFORMAT - 130)) | (1L << (OUTPUTFORMAT - 130)) | (1L << (DATABASE - 130)))) != 0) || ((((_la - 194)) & ~0x3f) == 0 && ((1L << (_la - 194)) & ((1L << (DATABASES - 194)) | (1L << (DFS - 194)) | (1L << (TRUNCATE - 194)) | (1L << (ANALYZE - 194)) | (1L << (COMPUTE - 194)) | (1L << (LIST - 194)) | (1L << (STATISTICS - 194)) | (1L << (PARTITIONED - 194)) | (1L << (EXTERNAL - 194)) | (1L << (DEFINED - 194)) | (1L << (REVOKE - 194)) | (1L << (GRANT - 194)) | (1L << (LOCK - 194)) | (1L << (UNLOCK - 194)) | (1L << (MSCK - 194)) | (1L << (REPAIR - 194)) | (1L << (RECOVER - 194)) | (1L << (EXPORT - 194)) | (1L << (IMPORT - 194)) | (1L << (LOAD - 194)) | (1L << (ROLE - 194)) | (1L << (ROLES - 194)) | (1L << (COMPACTIONS - 194)) | (1L << (PRINCIPALS - 194)) | (1L << (TRANSACTIONS - 194)) | (1L << (INDEX - 194)) | (1L << (INDEXES - 194)) | (1L << (LOCKS - 194)) | (1L << (OPTION - 194)) | (1L << (LOCAL - 194)) | (1L << (INPATH - 194)) | (1L << (CURRENT_DATE - 194)) | (1L << (CURRENT_TIMESTAMP - 194)))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 33:
			return queryTerm_sempred((QueryTermContext)_localctx, predIndex);
		case 60:
			return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
		case 63:
			return valueExpression_sempred((ValueExpressionContext)_localctx, predIndex);
		case 64:
			return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean queryTerm_sempred(QueryTermContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 1:
			return precpred(_ctx, 3);
		case 2:
			return precpred(_ctx, 2);
		}
		return true;
	}
	private boolean valueExpression_sempred(ValueExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 3:
			return precpred(_ctx, 6);
		case 4:
			return precpred(_ctx, 5);
		case 5:
			return precpred(_ctx, 4);
		case 6:
			return precpred(_ctx, 3);
		case 7:
			return precpred(_ctx, 2);
		case 8:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 9:
			return precpred(_ctx, 4);
		case 10:
			return precpred(_ctx, 2);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3\u00f5\u08a4\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4I"+
		"\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\4S\tS\4T\tT"+
		"\4U\tU\4V\tV\4W\tW\4X\tX\4Y\tY\4Z\tZ\4[\t[\4\\\t\\\3\2\3\2\3\2\3\3\3\3"+
		"\3\3\3\4\3\4\3\4\3\5\3\5\3\5\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u00cd"+
		"\n\6\3\6\3\6\3\6\5\6\u00d2\n\6\3\6\5\6\u00d5\n\6\3\6\3\6\3\6\5\6\u00da"+
		"\n\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u00e7\n\6\3\6\3\6"+
		"\5\6\u00eb\n\6\3\6\3\6\3\6\3\6\3\6\5\6\u00f2\n\6\3\6\3\6\3\6\5\6\u00f7"+
		"\n\6\3\6\3\6\3\6\5\6\u00fc\n\6\3\6\5\6\u00ff\n\6\3\6\5\6\u0102\n\6\3\6"+
		"\3\6\5\6\u0106\n\6\3\6\5\6\u0109\n\6\3\6\5\6\u010c\n\6\3\6\3\6\3\6\3\6"+
		"\3\6\5\6\u0113\n\6\3\6\3\6\5\6\u0117\n\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u011f"+
		"\n\6\3\6\5\6\u0122\n\6\3\6\5\6\u0125\n\6\3\6\5\6\u0128\n\6\3\6\5\6\u012b"+
		"\n\6\3\6\5\6\u012e\n\6\3\6\3\6\5\6\u0132\n\6\3\6\5\6\u0135\n\6\3\6\5\6"+
		"\u0138\n\6\3\6\3\6\3\6\3\6\3\6\5\6\u013f\n\6\3\6\3\6\3\6\3\6\5\6\u0145"+
		"\n\6\3\6\3\6\3\6\3\6\5\6\u014b\n\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u0153\n"+
		"\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6"+
		"\3\6\3\6\3\6\3\6\5\6\u016a\n\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u0172\n\6\3"+
		"\6\3\6\5\6\u0176\n\6\3\6\3\6\3\6\5\6\u017b\n\6\3\6\3\6\3\6\3\6\5\6\u0181"+
		"\n\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u0189\n\6\3\6\3\6\3\6\3\6\5\6\u018f\n"+
		"\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u019c\n\6\3\6\6\6\u019f"+
		"\n\6\r\6\16\6\u01a0\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u01aa\n\6\3\6\6\6"+
		"\u01ad\n\6\r\6\16\6\u01ae\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6"+
		"\3\6\3\6\3\6\5\6\u01bf\n\6\3\6\3\6\3\6\7\6\u01c4\n\6\f\6\16\6\u01c7\13"+
		"\6\3\6\5\6\u01ca\n\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u01d2\n\6\3\6\3\6\3\6"+
		"\7\6\u01d7\n\6\f\6\16\6\u01da\13\6\3\6\3\6\3\6\3\6\5\6\u01e0\n\6\3\6\3"+
		"\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u01ef\n\6\3\6\3\6\5"+
		"\6\u01f3\n\6\3\6\3\6\3\6\3\6\5\6\u01f9\n\6\3\6\3\6\3\6\3\6\5\6\u01ff\n"+
		"\6\3\6\5\6\u0202\n\6\3\6\5\6\u0205\n\6\3\6\3\6\3\6\3\6\5\6\u020b\n\6\3"+
		"\6\3\6\5\6\u020f\n\6\3\6\3\6\5\6\u0213\n\6\3\6\3\6\3\6\5\6\u0218\n\6\3"+
		"\6\3\6\5\6\u021c\n\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u0224\n\6\3\6\5\6\u0227"+
		"\n\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u0230\n\6\3\6\3\6\3\6\5\6\u0235\n"+
		"\6\3\6\3\6\3\6\3\6\5\6\u023b\n\6\3\6\3\6\3\6\3\6\5\6\u0241\n\6\3\6\3\6"+
		"\3\6\3\6\3\6\3\6\3\6\3\6\7\6\u024b\n\6\f\6\16\6\u024e\13\6\5\6\u0250\n"+
		"\6\3\6\3\6\5\6\u0254\n\6\3\6\3\6\3\6\5\6\u0259\n\6\3\6\3\6\3\6\5\6\u025e"+
		"\n\6\3\6\3\6\3\6\3\6\3\6\5\6\u0265\n\6\3\6\5\6\u0268\n\6\3\6\5\6\u026b"+
		"\n\6\3\6\3\6\3\6\3\6\3\6\5\6\u0272\n\6\3\6\3\6\3\6\5\6\u0277\n\6\3\6\3"+
		"\6\3\6\3\6\5\6\u027d\n\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u0286\n\6\3\6"+
		"\3\6\3\6\3\6\3\6\3\6\5\6\u028e\n\6\3\6\3\6\3\6\3\6\5\6\u0294\n\6\3\6\3"+
		"\6\5\6\u0298\n\6\3\6\3\6\5\6\u029c\n\6\3\6\3\6\5\6\u02a0\n\6\5\6\u02a2"+
		"\n\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u02ab\n\6\3\6\3\6\3\6\3\6\5\6\u02b1"+
		"\n\6\3\6\3\6\3\6\5\6\u02b6\n\6\3\6\5\6\u02b9\n\6\3\6\3\6\5\6\u02bd\n\6"+
		"\3\6\5\6\u02c0\n\6\3\6\3\6\3\6\3\6\3\6\7\6\u02c7\n\6\f\6\16\6\u02ca\13"+
		"\6\3\6\3\6\5\6\u02ce\n\6\3\6\3\6\3\6\5\6\u02d3\n\6\3\6\5\6\u02d6\n\6\3"+
		"\6\3\6\3\6\3\6\5\6\u02dc\n\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u02e4\n\6\3\6"+
		"\3\6\3\6\5\6\u02e9\n\6\3\6\3\6\3\6\3\6\5\6\u02ef\n\6\3\6\3\6\3\6\3\6\5"+
		"\6\u02f5\n\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\7\6\u02fe\n\6\f\6\16\6\u0301"+
		"\13\6\3\6\3\6\3\6\7\6\u0306\n\6\f\6\16\6\u0309\13\6\3\6\3\6\7\6\u030d"+
		"\n\6\f\6\16\6\u0310\13\6\3\6\3\6\3\6\7\6\u0315\n\6\f\6\16\6\u0318\13\6"+
		"\5\6\u031a\n\6\3\7\3\7\3\7\3\7\3\7\3\7\5\7\u0322\n\7\3\7\3\7\5\7\u0326"+
		"\n\7\3\7\3\7\3\7\3\7\3\7\5\7\u032d\n\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7"+
		"\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3"+
		"\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7"+
		"\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3"+
		"\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7"+
		"\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3"+
		"\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7"+
		"\3\7\5\7\u03a1\n\7\3\7\3\7\3\7\3\7\3\7\3\7\5\7\u03a9\n\7\3\7\3\7\3\7\3"+
		"\7\3\7\3\7\5\7\u03b1\n\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\5\7\u03ba\n\7\3\7"+
		"\3\7\3\7\3\7\3\7\3\7\3\7\5\7\u03c3\n\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7"+
		"\3\7\3\7\5\7\u03cf\n\7\3\b\3\b\5\b\u03d3\n\b\3\b\5\b\u03d6\n\b\3\b\3\b"+
		"\3\b\3\b\5\b\u03dc\n\b\3\b\3\b\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u03e6\n\t\3"+
		"\t\3\t\3\t\3\t\3\n\3\n\3\n\3\n\3\n\3\n\5\n\u03f2\n\n\3\n\3\n\3\n\5\n\u03f7"+
		"\n\n\3\13\3\13\3\13\3\f\5\f\u03fd\n\f\3\f\3\f\3\r\3\r\3\r\3\r\3\r\3\r"+
		"\3\r\3\r\5\r\u0409\n\r\5\r\u040b\n\r\3\r\3\r\3\r\5\r\u0410\n\r\3\r\3\r"+
		"\5\r\u0414\n\r\5\r\u0416\n\r\3\16\3\16\5\16\u041a\n\16\3\17\3\17\3\17"+
		"\3\17\3\17\7\17\u0421\n\17\f\17\16\17\u0424\13\17\3\17\3\17\3\20\3\20"+
		"\3\20\5\20\u042b\n\20\3\21\3\21\3\21\3\21\3\21\5\21\u0432\n\21\3\22\3"+
		"\22\3\22\3\22\5\22\u0438\n\22\7\22\u043a\n\22\f\22\16\22\u043d\13\22\3"+
		"\23\3\23\3\23\3\23\7\23\u0443\n\23\f\23\16\23\u0446\13\23\3\24\3\24\5"+
		"\24\u044a\n\24\3\24\3\24\3\24\3\24\3\25\3\25\3\25\3\26\3\26\3\26\3\26"+
		"\7\26\u0457\n\26\f\26\16\26\u045a\13\26\3\26\3\26\3\27\3\27\5\27\u0460"+
		"\n\27\3\27\5\27\u0463\n\27\3\30\3\30\3\30\7\30\u0468\n\30\f\30\16\30\u046b"+
		"\13\30\3\30\5\30\u046e\n\30\3\31\3\31\3\31\3\31\5\31\u0474\n\31\3\32\3"+
		"\32\3\32\3\32\7\32\u047a\n\32\f\32\16\32\u047d\13\32\3\32\3\32\3\33\3"+
		"\33\3\33\3\33\7\33\u0485\n\33\f\33\16\33\u0488\13\33\3\33\3\33\3\34\3"+
		"\34\3\34\3\34\3\34\3\34\5\34\u0492\n\34\3\35\3\35\3\35\3\35\3\35\5\35"+
		"\u0499\n\35\3\36\3\36\3\36\3\36\5\36\u049f\n\36\3\37\3\37\3\37\3 \5 \u04a5"+
		"\n \3 \3 \3 \3 \3 \6 \u04ac\n \r \16 \u04ad\5 \u04b0\n \3!\3!\3!\3!\3"+
		"!\7!\u04b7\n!\f!\16!\u04ba\13!\5!\u04bc\n!\3!\3!\3!\3!\3!\7!\u04c3\n!"+
		"\f!\16!\u04c6\13!\5!\u04c8\n!\3!\3!\3!\3!\3!\7!\u04cf\n!\f!\16!\u04d2"+
		"\13!\5!\u04d4\n!\3!\3!\3!\3!\3!\7!\u04db\n!\f!\16!\u04de\13!\5!\u04e0"+
		"\n!\3!\5!\u04e3\n!\3!\3!\5!\u04e7\n!\3\"\5\"\u04ea\n\"\3\"\3\"\3\"\3#"+
		"\3#\3#\3#\3#\3#\5#\u04f5\n#\3#\7#\u04f8\n#\f#\16#\u04fb\13#\3$\3$\3$\3"+
		"$\3$\3$\3$\3$\5$\u0505\n$\3%\3%\5%\u0509\n%\3%\3%\5%\u050d\n%\3&\3&\3"+
		"&\3&\3&\3&\3&\3&\3&\3&\5&\u0519\n&\3&\5&\u051c\n&\3&\3&\5&\u0520\n&\3"+
		"&\3&\3&\3&\3&\3&\3&\3&\5&\u052a\n&\3&\3&\5&\u052e\n&\5&\u0530\n&\3&\5"+
		"&\u0533\n&\3&\3&\5&\u0537\n&\3&\5&\u053a\n&\3&\3&\5&\u053e\n&\3&\3&\5"+
		"&\u0542\n&\3&\3&\5&\u0546\n&\3&\3&\3&\5&\u054b\n&\3&\5&\u054e\n&\5&\u0550"+
		"\n&\3&\7&\u0553\n&\f&\16&\u0556\13&\3&\3&\5&\u055a\n&\3&\5&\u055d\n&\3"+
		"&\3&\5&\u0561\n&\3&\5&\u0564\n&\5&\u0566\n&\3\'\3\'\3\'\3\'\7\'\u056c"+
		"\n\'\f\'\16\'\u056f\13\'\3\'\7\'\u0572\n\'\f\'\16\'\u0575\13\'\3(\3(\3"+
		"(\3(\3(\7(\u057c\n(\f(\16(\u057f\13(\3(\3(\3(\3(\3(\3(\3(\3(\3(\3(\7("+
		"\u058b\n(\f(\16(\u058e\13(\3(\3(\5(\u0592\n(\3)\3)\3)\3)\7)\u0598\n)\f"+
		")\16)\u059b\13)\5)\u059d\n)\3)\3)\5)\u05a1\n)\3*\3*\3*\5*\u05a6\n*\3*"+
		"\3*\3*\3*\3*\7*\u05ad\n*\f*\16*\u05b0\13*\5*\u05b2\n*\3*\3*\3*\5*\u05b7"+
		"\n*\3*\3*\3*\7*\u05bc\n*\f*\16*\u05bf\13*\5*\u05c1\n*\3+\3+\3,\3,\7,\u05c7"+
		"\n,\f,\16,\u05ca\13,\3-\3-\3-\3-\5-\u05d0\n-\3-\3-\3-\3-\3-\5-\u05d7\n"+
		"-\3.\5.\u05da\n.\3.\3.\3.\5.\u05df\n.\3.\3.\3.\3.\5.\u05e5\n.\3.\3.\5"+
		".\u05e9\n.\3.\5.\u05ec\n.\3.\5.\u05ef\n.\3/\3/\3/\3/\3/\3/\3/\7/\u05f8"+
		"\n/\f/\16/\u05fb\13/\3/\3/\5/\u05ff\n/\3\60\3\60\3\60\3\60\3\60\3\60\3"+
		"\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60\3\60\5\60\u0614"+
		"\n\60\5\60\u0616\n\60\5\60\u0618\n\60\3\60\3\60\3\61\3\61\3\61\3\61\3"+
		"\62\3\62\3\62\7\62\u0623\n\62\f\62\16\62\u0626\13\62\3\63\3\63\3\63\3"+
		"\63\7\63\u062c\n\63\f\63\16\63\u062f\13\63\3\63\3\63\3\64\3\64\5\64\u0635"+
		"\n\64\3\65\3\65\3\65\3\65\7\65\u063b\n\65\f\65\16\65\u063e\13\65\3\65"+
		"\3\65\3\66\3\66\3\66\5\66\u0645\n\66\3\67\3\67\5\67\u0649\n\67\3\67\5"+
		"\67\u064c\n\67\3\67\5\67\u064f\n\67\3\67\3\67\3\67\3\67\5\67\u0655\n\67"+
		"\3\67\5\67\u0658\n\67\3\67\5\67\u065b\n\67\3\67\3\67\3\67\3\67\5\67\u0661"+
		"\n\67\3\67\5\67\u0664\n\67\3\67\5\67\u0667\n\67\3\67\3\67\3\67\3\67\3"+
		"\67\3\67\7\67\u066f\n\67\f\67\16\67\u0672\13\67\5\67\u0674\n\67\3\67\3"+
		"\67\5\67\u0678\n\67\38\38\38\38\78\u067e\n8\f8\168\u0681\138\38\58\u0684"+
		"\n8\38\38\58\u0688\n8\58\u068a\n8\39\39\39\39\39\39\39\59\u0693\n9\39"+
		"\39\39\39\39\39\39\39\39\39\59\u069f\n9\59\u06a1\n9\39\39\39\39\39\59"+
		"\u06a8\n9\39\39\39\39\39\59\u06af\n9\39\39\39\39\59\u06b5\n9\39\39\39"+
		"\39\59\u06bb\n9\59\u06bd\n9\3:\3:\3:\5:\u06c2\n:\3:\3:\3;\3;\5;\u06c8"+
		"\n;\3;\3;\5;\u06cc\n;\5;\u06ce\n;\3<\3<\3<\7<\u06d3\n<\f<\16<\u06d6\13"+
		"<\3=\3=\3>\3>\3>\3>\3>\3>\3>\3>\3>\5>\u06e3\n>\3>\3>\3>\3>\3>\3>\7>\u06eb"+
		"\n>\f>\16>\u06ee\13>\3?\3?\5?\u06f2\n?\3@\5@\u06f5\n@\3@\3@\3@\3@\3@\3"+
		"@\5@\u06fd\n@\3@\3@\3@\3@\3@\7@\u0704\n@\f@\16@\u0707\13@\3@\3@\3@\5@"+
		"\u070c\n@\3@\3@\3@\3@\3@\3@\5@\u0714\n@\3@\3@\3@\3@\5@\u071a\n@\3@\5@"+
		"\u071d\n@\3A\3A\3A\3A\5A\u0723\nA\3A\3A\3A\3A\3A\3A\3A\3A\3A\3A\3A\3A"+
		"\3A\3A\3A\3A\3A\3A\3A\7A\u0738\nA\fA\16A\u073b\13A\3B\3B\3B\3B\6B\u0741"+
		"\nB\rB\16B\u0742\3B\3B\5B\u0747\nB\3B\3B\3B\3B\3B\6B\u074e\nB\rB\16B\u074f"+
		"\3B\3B\5B\u0754\nB\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B"+
		"\3B\3B\6B\u0769\nB\rB\16B\u076a\3B\3B\3B\3B\3B\3B\3B\3B\3B\5B\u0776\n"+
		"B\3B\3B\3B\7B\u077b\nB\fB\16B\u077e\13B\5B\u0780\nB\3B\3B\3B\5B\u0785"+
		"\nB\3B\3B\3B\3B\3B\5B\u078c\nB\3B\3B\3B\3B\3B\3B\3B\3B\7B\u0796\nB\fB"+
		"\16B\u0799\13B\3C\3C\3C\3C\3C\3C\3C\3C\6C\u07a3\nC\rC\16C\u07a4\5C\u07a7"+
		"\nC\3D\3D\3E\3E\3F\3F\3G\3G\3H\3H\7H\u07b3\nH\fH\16H\u07b6\13H\3I\3I\3"+
		"I\3I\5I\u07bc\nI\3J\5J\u07bf\nJ\3J\3J\5J\u07c3\nJ\3K\3K\3K\5K\u07c8\n"+
		"K\3L\3L\3L\3L\3L\3L\3L\3L\3L\3L\3L\3L\3L\3L\3L\5L\u07d9\nL\3L\3L\5L\u07dd"+
		"\nL\3L\3L\3L\3L\3L\7L\u07e4\nL\fL\16L\u07e7\13L\3L\5L\u07ea\nL\5L\u07ec"+
		"\nL\3M\3M\3M\7M\u07f1\nM\fM\16M\u07f4\13M\3N\3N\3N\3N\5N\u07fa\nN\3O\3"+
		"O\3O\7O\u07ff\nO\fO\16O\u0802\13O\3P\3P\3P\3P\3P\5P\u0809\nP\3Q\3Q\3Q"+
		"\3Q\3Q\3R\3R\3R\3R\7R\u0814\nR\fR\16R\u0817\13R\3S\3S\3S\3S\3T\3T\3T\3"+
		"T\3T\3T\3T\7T\u0824\nT\fT\16T\u0827\13T\3T\3T\3T\3T\3T\7T\u082e\nT\fT"+
		"\16T\u0831\13T\5T\u0833\nT\3T\3T\3T\3T\3T\7T\u083a\nT\fT\16T\u083d\13"+
		"T\5T\u083f\nT\5T\u0841\nT\3T\5T\u0844\nT\3T\5T\u0847\nT\3U\3U\3U\3U\3"+
		"U\3U\3U\3U\3U\3U\3U\3U\3U\3U\3U\3U\5U\u0859\nU\3V\3V\3V\3V\3V\3V\3V\5"+
		"V\u0862\nV\3W\3W\3W\7W\u0867\nW\fW\16W\u086a\13W\3X\3X\3X\3X\3X\3X\3X"+
		"\3X\3X\3X\3X\3X\3X\3X\3X\5X\u087b\nX\3Y\3Y\3Y\5Y\u0880\nY\3Z\3Z\3[\5["+
		"\u0885\n[\3[\3[\5[\u0889\n[\3[\3[\5[\u088d\n[\3[\3[\5[\u0891\n[\3[\3["+
		"\5[\u0895\n[\3[\3[\5[\u0899\n[\3[\3[\5[\u089d\n[\3[\5[\u08a0\n[\3\\\3"+
		"\\\3\\\7\u02c8\u02ff\u0307\u030e\u0316\6Dz\u0080\u0082]\2\4\6\b\n\f\16"+
		"\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJLNPRTVXZ\\^`bd"+
		"fhjlnprtvxz|~\u0080\u0082\u0084\u0086\u0088\u008a\u008c\u008e\u0090\u0092"+
		"\u0094\u0096\u0098\u009a\u009c\u009e\u00a0\u00a2\u00a4\u00a6\u00a8\u00aa"+
		"\u00ac\u00ae\u00b0\u00b2\u00b4\u00b6\2\33\3\2\u00bc\u00bd\3\2MN\5\2VW"+
		"\u00a2\u00a2\u00a8\u00a8\4\2\13\13\35\35\4\2**SS\4\2\u00a2\u00a2\u00a8"+
		"\u00a8\4\2\f\f\u00c9\u00c9\3\2ad\3\2)*\4\2FFHH\3\2\16\17\3\2\u00eb\u00ec"+
		"\3\2\"#\4\2\177\u0080\u0085\u0085\3\2\u0081\u0084\3\2\177\u0080\3\2\u00e4"+
		"\u00e5\3\2w~\3\2\177\u0088\3\2\33\36\3\2&\'\4\2??\u008f\u008f\4\2\27\27"+
		"\u008d\u008d\3\2CD\t\2\n\61\64\64<`ev\u0084\u0084\u0089\u00e0\u00e2\u00e5"+
		"\u0a1f\2\u00b8\3\2\2\2\4\u00bb\3\2\2\2\6\u00be\3\2\2\2\b\u00c1\3\2\2\2"+
		"\n\u0319\3\2\2\2\f\u03ce\3\2\2\2\16\u03d0\3\2\2\2\20\u03df\3\2\2\2\22"+
		"\u03eb\3\2\2\2\24\u03f8\3\2\2\2\26\u03fc\3\2\2\2\30\u0415\3\2\2\2\32\u0417"+
		"\3\2\2\2\34\u041b\3\2\2\2\36\u0427\3\2\2\2 \u0431\3\2\2\2\"\u0433\3\2"+
		"\2\2$\u043e\3\2\2\2&\u0447\3\2\2\2(\u044f\3\2\2\2*\u0452\3\2\2\2,\u045d"+
		"\3\2\2\2.\u046d\3\2\2\2\60\u0473\3\2\2\2\62\u0475\3\2\2\2\64\u0480\3\2"+
		"\2\2\66\u0491\3\2\2\28\u0498\3\2\2\2:\u049a\3\2\2\2<\u04a0\3\2\2\2>\u04af"+
		"\3\2\2\2@\u04bb\3\2\2\2B\u04e9\3\2\2\2D\u04ee\3\2\2\2F\u0504\3\2\2\2H"+
		"\u0506\3\2\2\2J\u0565\3\2\2\2L\u0567\3\2\2\2N\u0576\3\2\2\2P\u05a0\3\2"+
		"\2\2R\u05a2\3\2\2\2T\u05c2\3\2\2\2V\u05c4\3\2\2\2X\u05d6\3\2\2\2Z\u05ee"+
		"\3\2\2\2\\\u05fe\3\2\2\2^\u0600\3\2\2\2`\u061b\3\2\2\2b\u061f\3\2\2\2"+
		"d\u0627\3\2\2\2f\u0632\3\2\2\2h\u0636\3\2\2\2j\u0641\3\2\2\2l\u0677\3"+
		"\2\2\2n\u0679\3\2\2\2p\u06bc\3\2\2\2r\u06c1\3\2\2\2t\u06c5\3\2\2\2v\u06cf"+
		"\3\2\2\2x\u06d7\3\2\2\2z\u06e2\3\2\2\2|\u06ef\3\2\2\2~\u071c\3\2\2\2\u0080"+
		"\u0722\3\2\2\2\u0082\u078b\3\2\2\2\u0084\u07a6\3\2\2\2\u0086\u07a8\3\2"+
		"\2\2\u0088\u07aa\3\2\2\2\u008a\u07ac\3\2\2\2\u008c\u07ae\3\2\2\2\u008e"+
		"\u07b0\3\2\2\2\u0090\u07b7\3\2\2\2\u0092\u07c2\3\2\2\2\u0094\u07c7\3\2"+
		"\2\2\u0096\u07eb\3\2\2\2\u0098\u07ed\3\2\2\2\u009a\u07f5\3\2\2\2\u009c"+
		"\u07fb\3\2\2\2\u009e\u0803\3\2\2\2\u00a0\u080a\3\2\2\2\u00a2\u080f\3\2"+
		"\2\2\u00a4\u0818\3\2\2\2\u00a6\u0846\3\2\2\2\u00a8\u0858\3\2\2\2\u00aa"+
		"\u0861\3\2\2\2\u00ac\u0863\3\2\2\2\u00ae\u087a\3\2\2\2\u00b0\u087f\3\2"+
		"\2\2\u00b2\u0881\3\2\2\2\u00b4\u089f\3\2\2\2\u00b6\u08a1\3\2\2\2\u00b8"+
		"\u00b9\5\n\6\2\u00b9\u00ba\7\2\2\3\u00ba\3\3\2\2\2\u00bb\u00bc\5t;\2\u00bc"+
		"\u00bd\7\2\2\3\u00bd\5\3\2\2\2\u00be\u00bf\5r:\2\u00bf\u00c0\7\2\2\3\u00c0"+
		"\7\3\2\2\2\u00c1\u00c2\5\u0096L\2\u00c2\u00c3\7\2\2\3\u00c3\t\3\2\2\2"+
		"\u00c4\u031a\5\26\f\2\u00c5\u00c6\7]\2\2\u00c6\u031a\5\u00aeX\2\u00c7"+
		"\u00c8\7L\2\2\u00c8\u00cc\7\u00c3\2\2\u00c9\u00ca\7v\2\2\u00ca\u00cb\7"+
		"\36\2\2\u00cb\u00cd\7 \2\2\u00cc\u00c9\3\2\2\2\u00cc\u00cd\3\2\2\2\u00cd"+
		"\u00ce\3\2\2\2\u00ce\u00d1\5\u00aeX\2\u00cf\u00d0\7m\2\2\u00d0\u00d2\7"+
		"\u00e6\2\2\u00d1\u00cf\3\2\2\2\u00d1\u00d2\3\2\2\2\u00d2\u00d4\3\2\2\2"+
		"\u00d3\u00d5\5\24\13\2\u00d4\u00d3\3\2\2\2\u00d4\u00d5\3\2\2\2\u00d5\u00d9"+
		"\3\2\2\2\u00d6\u00d7\7J\2\2\u00d7\u00d8\7\u00ae\2\2\u00d8\u00da\5*\26"+
		"\2\u00d9\u00d6\3\2\2\2\u00d9\u00da\3\2\2\2\u00da\u031a\3\2\2\2\u00db\u00dc"+
		"\7h\2\2\u00dc\u00dd\7\u00c3\2\2\u00dd\u00de\5\u00aeX\2\u00de\u00df\7n"+
		"\2\2\u00df\u00e0\7\u00ae\2\2\u00e0\u00e1\5*\26\2\u00e1\u031a\3\2\2\2\u00e2"+
		"\u00e3\7`\2\2\u00e3\u00e6\7\u00c3\2\2\u00e4\u00e5\7v\2\2\u00e5\u00e7\7"+
		" \2\2\u00e6\u00e4\3\2\2\2\u00e6\u00e7\3\2\2\2\u00e7\u00e8\3\2\2\2\u00e8"+
		"\u00ea\5\u00aeX\2\u00e9\u00eb\t\2\2\2\u00ea\u00e9\3\2\2\2\u00ea\u00eb"+
		"\3\2\2\2\u00eb\u031a\3\2\2\2\u00ec\u00f1\5\16\b\2\u00ed\u00ee\7\3\2\2"+
		"\u00ee\u00ef\5\u0098M\2\u00ef\u00f0\7\4\2\2\u00f0\u00f2\3\2\2\2\u00f1"+
		"\u00ed\3\2\2\2\u00f1\u00f2\3\2\2\2\u00f2\u00f3\3\2\2\2\u00f3\u00f6\5("+
		"\25\2\u00f4\u00f5\7\u00ab\2\2\u00f5\u00f7\5*\26\2\u00f6\u00f4\3\2\2\2"+
		"\u00f6\u00f7\3\2\2\2\u00f7\u00fb\3\2\2\2\u00f8\u00f9\7\u00cb\2\2\u00f9"+
		"\u00fa\7\22\2\2\u00fa\u00fc\5`\61\2\u00fb\u00f8\3\2\2\2\u00fb\u00fc\3"+
		"\2\2\2\u00fc\u00fe\3\2\2\2\u00fd\u00ff\5\20\t\2\u00fe\u00fd\3\2\2\2\u00fe"+
		"\u00ff\3\2\2\2\u00ff\u0101\3\2\2\2\u0100\u0102\5\24\13\2\u0101\u0100\3"+
		"\2\2\2\u0101\u0102\3\2\2\2\u0102\u0105\3\2\2\2\u0103\u0104\7m\2\2\u0104"+
		"\u0106\7\u00e6\2\2\u0105\u0103\3\2\2\2\u0105\u0106\3\2\2\2\u0106\u010b"+
		"\3\2\2\2\u0107\u0109\7\r\2\2\u0108\u0107\3\2\2\2\u0108\u0109\3\2\2\2\u0109"+
		"\u010a\3\2\2\2\u010a\u010c\5\26\f\2\u010b\u0108\3\2\2\2\u010b\u010c\3"+
		"\2\2\2\u010c\u031a\3\2\2\2\u010d\u0112\5\16\b\2\u010e\u010f\7\3\2\2\u010f"+
		"\u0110\5\u0098M\2\u0110\u0111\7\4\2\2\u0111\u0113\3\2\2\2\u0112\u010e"+
		"\3\2\2\2\u0112\u0113\3\2\2\2\u0113\u0116\3\2\2\2\u0114\u0115\7m\2\2\u0115"+
		"\u0117\7\u00e6\2\2\u0116\u0114\3\2\2\2\u0116\u0117\3\2\2\2\u0117\u011e"+
		"\3\2\2\2\u0118\u0119\7\u00cb\2\2\u0119\u011a\7\22\2\2\u011a\u011b\7\3"+
		"\2\2\u011b\u011c\5\u0098M\2\u011c\u011d\7\4\2\2\u011d\u011f\3\2\2\2\u011e"+
		"\u0118\3\2\2\2\u011e\u011f\3\2\2\2\u011f\u0121\3\2\2\2\u0120\u0122\5\20"+
		"\t\2\u0121\u0120\3\2\2\2\u0121\u0122\3\2\2\2\u0122\u0124\3\2\2\2\u0123"+
		"\u0125\5\22\n\2\u0124\u0123\3\2\2\2\u0124\u0125\3\2\2\2\u0125\u0127\3"+
		"\2\2\2\u0126\u0128\5p9\2\u0127\u0126\3\2\2\2\u0127\u0128\3\2\2\2\u0128"+
		"\u012a\3\2\2\2\u0129\u012b\5\66\34\2\u012a\u0129\3\2\2\2\u012a\u012b\3"+
		"\2\2\2\u012b\u012d\3\2\2\2\u012c\u012e\5\24\13\2\u012d\u012c\3\2\2\2\u012d"+
		"\u012e\3\2\2\2\u012e\u0131\3\2\2\2\u012f\u0130\7\u00ad\2\2\u0130\u0132"+
		"\5*\26\2\u0131\u012f\3\2\2\2\u0131\u0132\3\2\2\2\u0132\u0137\3\2\2\2\u0133"+
		"\u0135\7\r\2\2\u0134\u0133\3\2\2\2\u0134\u0135\3\2\2\2\u0135\u0136\3\2"+
		"\2\2\u0136\u0138\5\26\f\2\u0137\u0134\3\2\2\2\u0137\u0138\3\2\2\2\u0138"+
		"\u031a\3\2\2\2\u0139\u013a\7L\2\2\u013a\u013e\7M\2\2\u013b\u013c\7v\2"+
		"\2\u013c\u013d\7\36\2\2\u013d\u013f\7 \2\2\u013e\u013b\3\2\2\2\u013e\u013f"+
		"\3\2\2\2\u013f\u0140\3\2\2\2\u0140\u0141\5r:\2\u0141\u0142\7\"\2\2\u0142"+
		"\u0144\5r:\2\u0143\u0145\5\24\13\2\u0144\u0143\3\2\2\2\u0144\u0145\3\2"+
		"\2\2\u0145\u031a\3\2\2\2\u0146\u0147\7\u00c7\2\2\u0147\u0148\7M\2\2\u0148"+
		"\u014a\5r:\2\u0149\u014b\5\34\17\2\u014a\u0149\3\2\2\2\u014a\u014b\3\2"+
		"\2\2\u014b\u014c\3\2\2\2\u014c\u014d\7\u00c8\2\2\u014d\u0152\7\u00ca\2"+
		"\2\u014e\u0153\5\u00aeX\2\u014f\u0150\7+\2\2\u0150\u0151\7[\2\2\u0151"+
		"\u0153\5b\62\2\u0152\u014e\3\2\2\2\u0152\u014f\3\2\2\2\u0152\u0153\3\2"+
		"\2\2\u0153\u031a\3\2\2\2\u0154\u0155\7h\2\2\u0155\u0156\t\3\2\2\u0156"+
		"\u0157\5r:\2\u0157\u0158\7i\2\2\u0158\u0159\7e\2\2\u0159\u015a\5r:\2\u015a"+
		"\u031a\3\2\2\2\u015b\u015c\7h\2\2\u015c\u015d\t\3\2\2\u015d\u015e\5r:"+
		"\2\u015e\u015f\7n\2\2\u015f\u0160\7\u00ad\2\2\u0160\u0161\5*\26\2\u0161"+
		"\u031a\3\2\2\2\u0162\u0163\7h\2\2\u0163\u0164\t\3\2\2\u0164\u0165\5r:"+
		"\2\u0165\u0166\7\u00ac\2\2\u0166\u0169\7\u00ad\2\2\u0167\u0168\7v\2\2"+
		"\u0168\u016a\7 \2\2\u0169\u0167\3\2\2\2\u0169\u016a\3\2\2\2\u016a\u016b"+
		"\3\2\2\2\u016b\u016c\5*\26\2\u016c\u031a\3\2\2\2\u016d\u016e\7h\2\2\u016e"+
		"\u016f\7M\2\2\u016f\u0171\5r:\2\u0170\u0172\5\34\17\2\u0171\u0170\3\2"+
		"\2\2\u0171\u0172\3\2\2\2\u0172\u0173\3\2\2\2\u0173\u0175\7\u00bb\2\2\u0174"+
		"\u0176\7\\\2\2\u0175\u0174\3\2\2\2\u0175\u0176\3\2\2\2\u0176\u0177\3\2"+
		"\2\2\u0177\u0178\5\u00aeX\2\u0178\u017a\5\u009aN\2\u0179\u017b\5\u0094"+
		"K\2\u017a\u0179\3\2\2\2\u017a\u017b\3\2\2\2\u017b\u031a\3\2\2\2\u017c"+
		"\u017d\7h\2\2\u017d\u017e\7M\2\2\u017e\u0180\5r:\2\u017f\u0181\5\34\17"+
		"\2\u0180\u017f\3\2\2\2\u0180\u0181\3\2\2\2\u0181\u0182\3\2\2\2\u0182\u0183"+
		"\7n\2\2\u0183\u0184\7\u0094\2\2\u0184\u0188\7\u00e6\2\2\u0185\u0186\7"+
		"J\2\2\u0186\u0187\7\u0095\2\2\u0187\u0189\5*\26\2\u0188\u0185\3\2\2\2"+
		"\u0188\u0189\3\2\2\2\u0189\u031a\3\2\2\2\u018a\u018b\7h\2\2\u018b\u018c"+
		"\7M\2\2\u018c\u018e\5r:\2\u018d\u018f\5\34\17\2\u018e\u018d\3\2\2\2\u018e"+
		"\u018f\3\2\2\2\u018f\u0190\3\2\2\2\u0190\u0191\7n\2\2\u0191\u0192\7\u0095"+
		"\2\2\u0192\u0193\5*\26\2\u0193\u031a\3\2\2\2\u0194\u0195\7h\2\2\u0195"+
		"\u0196\7M\2\2\u0196\u0197\5r:\2\u0197\u019b\7\f\2\2\u0198\u0199\7v\2\2"+
		"\u0199\u019a\7\36\2\2\u019a\u019c\7 \2\2\u019b\u0198\3\2\2\2\u019b\u019c"+
		"\3\2\2\2\u019c\u019e\3\2\2\2\u019d\u019f\5\32\16\2\u019e\u019d\3\2\2\2"+
		"\u019f\u01a0\3\2\2\2\u01a0\u019e\3\2\2\2\u01a0\u01a1\3\2\2\2\u01a1\u031a"+
		"\3\2\2\2\u01a2\u01a3\7h\2\2\u01a3\u01a4\7N\2\2\u01a4\u01a5\5r:\2\u01a5"+
		"\u01a9\7\f\2\2\u01a6\u01a7\7v\2\2\u01a7\u01a8\7\36\2\2\u01a8\u01aa\7 "+
		"\2\2\u01a9\u01a6\3\2\2\2\u01a9\u01aa\3\2\2\2\u01aa\u01ac\3\2\2\2\u01ab"+
		"\u01ad\5\34\17\2\u01ac\u01ab\3\2\2\2\u01ad\u01ae\3\2\2\2\u01ae\u01ac\3"+
		"\2\2\2\u01ae\u01af\3\2\2\2\u01af\u031a\3\2\2\2\u01b0\u01b1\7h\2\2\u01b1"+
		"\u01b2\7M\2\2\u01b2\u01b3\5r:\2\u01b3\u01b4\5\34\17\2\u01b4\u01b5\7i\2"+
		"\2\u01b5\u01b6\7e\2\2\u01b6\u01b7\5\34\17\2\u01b7\u031a\3\2\2\2\u01b8"+
		"\u01b9\7h\2\2\u01b9\u01ba\7M\2\2\u01ba\u01bb\5r:\2\u01bb\u01be\7`\2\2"+
		"\u01bc\u01bd\7v\2\2\u01bd\u01bf\7 \2\2\u01be\u01bc\3\2\2\2\u01be\u01bf"+
		"\3\2\2\2\u01bf\u01c0\3\2\2\2\u01c0\u01c5\5\34\17\2\u01c1\u01c2\7\5\2\2"+
		"\u01c2\u01c4\5\34\17\2\u01c3\u01c1\3\2\2\2\u01c4\u01c7\3\2\2\2\u01c5\u01c3"+
		"\3\2\2\2\u01c5\u01c6\3\2\2\2\u01c6\u01c9\3\2\2\2\u01c7\u01c5\3\2\2\2\u01c8"+
		"\u01ca\7\u00c0\2\2\u01c9\u01c8\3\2\2\2\u01c9\u01ca\3\2\2\2\u01ca\u031a"+
		"\3\2\2\2\u01cb\u01cc\7h\2\2\u01cc\u01cd\7N\2\2\u01cd\u01ce\5r:\2\u01ce"+
		"\u01d1\7`\2\2\u01cf\u01d0\7v\2\2\u01d0\u01d2\7 \2\2\u01d1\u01cf\3\2\2"+
		"\2\u01d1\u01d2\3\2\2\2\u01d2\u01d3\3\2\2\2\u01d3\u01d8\5\34\17\2\u01d4"+
		"\u01d5\7\5\2\2\u01d5\u01d7\5\34\17\2\u01d6\u01d4\3\2\2\2\u01d7\u01da\3"+
		"\2\2\2\u01d8\u01d6\3\2\2\2\u01d8\u01d9\3\2\2\2\u01d9\u031a\3\2\2\2\u01da"+
		"\u01d8\3\2\2\2\u01db\u01dc\7h\2\2\u01dc\u01dd\7M\2\2\u01dd\u01df\5r:\2"+
		"\u01de\u01e0\5\34\17\2\u01df\u01de\3\2\2\2\u01df\u01e0\3\2\2\2\u01e0\u01e1"+
		"\3\2\2\2\u01e1\u01e2\7n\2\2\u01e2\u01e3\5\24\13\2\u01e3\u031a\3\2\2\2"+
		"\u01e4\u01e5\7h\2\2\u01e5\u01e6\7M\2\2\u01e6\u01e7\5r:\2\u01e7\u01e8\7"+
		"\u00d4\2\2\u01e8\u01e9\7^\2\2\u01e9\u031a\3\2\2\2\u01ea\u01eb\7`\2\2\u01eb"+
		"\u01ee\7M\2\2\u01ec\u01ed\7v\2\2\u01ed\u01ef\7 \2\2\u01ee\u01ec\3\2\2"+
		"\2\u01ee\u01ef\3\2\2\2\u01ef\u01f0\3\2\2\2\u01f0\u01f2\5r:\2\u01f1\u01f3"+
		"\7\u00c0\2\2\u01f2\u01f1\3\2\2\2\u01f2\u01f3\3\2\2\2\u01f3\u031a\3\2\2"+
		"\2\u01f4\u01f5\7`\2\2\u01f5\u01f8\7N\2\2\u01f6\u01f7\7v\2\2\u01f7\u01f9"+
		"\7 \2\2\u01f8\u01f6\3\2\2\2\u01f8\u01f9\3\2\2\2\u01f9\u01fa\3\2\2\2\u01fa"+
		"\u031a\5r:\2\u01fb\u01fe\7L\2\2\u01fc\u01fd\7\33\2\2\u01fd\u01ff\7O\2"+
		"\2\u01fe\u01fc\3\2\2\2\u01fe\u01ff\3\2\2\2\u01ff\u0204\3\2\2\2\u0200\u0202"+
		"\7\u00a9\2\2\u0201\u0200\3\2\2\2\u0201\u0202\3\2\2\2\u0202\u0203\3\2\2"+
		"\2\u0203\u0205\7\u00aa\2\2\u0204\u0201\3\2\2\2\u0204\u0205\3\2\2\2\u0205"+
		"\u0206\3\2\2\2\u0206\u020a\7N\2\2\u0207\u0208\7v\2\2\u0208\u0209\7\36"+
		"\2\2\u0209\u020b\7 \2\2\u020a\u0207\3\2\2\2\u020a\u020b\3\2\2\2\u020b"+
		"\u020c\3\2\2\2\u020c\u020e\5r:\2\u020d\u020f\5h\65\2\u020e\u020d\3\2\2"+
		"\2\u020e\u020f\3\2\2\2\u020f\u0212\3\2\2\2\u0210\u0211\7m\2\2\u0211\u0213"+
		"\7\u00e6\2\2\u0212\u0210\3\2\2\2\u0212\u0213\3\2\2\2\u0213\u0217\3\2\2"+
		"\2\u0214\u0215\7\u00cb\2\2\u0215\u0216\7;\2\2\u0216\u0218\5`\61\2\u0217"+
		"\u0214\3\2\2\2\u0217\u0218\3\2\2\2\u0218\u021b\3\2\2\2\u0219\u021a\7\u00ad"+
		"\2\2\u021a\u021c\5*\26\2\u021b\u0219\3\2\2\2\u021b\u021c\3\2\2\2\u021c"+
		"\u021d\3\2\2\2\u021d\u021e\7\r\2\2\u021e\u021f\5\26\f\2\u021f\u031a\3"+
		"\2\2\2\u0220\u0223\7L\2\2\u0221\u0222\7\33\2\2\u0222\u0224\7O\2\2\u0223"+
		"\u0221\3\2\2\2\u0223\u0224\3\2\2\2\u0224\u0226\3\2\2\2\u0225\u0227\7\u00a9"+
		"\2\2\u0226\u0225\3\2\2\2\u0226\u0227\3\2\2\2\u0227\u0228\3\2\2\2\u0228"+
		"\u0229\7\u00aa\2\2\u0229\u022a\7N\2\2\u022a\u022f\5r:\2\u022b\u022c\7"+
		"\3\2\2\u022c\u022d\5\u0098M\2\u022d\u022e\7\4\2\2\u022e\u0230\3\2\2\2"+
		"\u022f\u022b\3\2\2\2\u022f\u0230\3\2\2\2\u0230\u0231\3\2\2\2\u0231\u0234"+
		"\5(\25\2\u0232\u0233\7\u00ab\2\2\u0233\u0235\5*\26\2\u0234\u0232\3\2\2"+
		"\2\u0234\u0235\3\2\2\2\u0235\u031a\3\2\2\2\u0236\u0237\7h\2\2\u0237\u0238"+
		"\7N\2\2\u0238\u023a\5r:\2\u0239\u023b\7\r\2\2\u023a\u0239\3\2\2\2\u023a"+
		"\u023b\3\2\2\2\u023b\u023c\3\2\2\2\u023c\u023d\5\26\f\2\u023d\u031a\3"+
		"\2\2\2\u023e\u0240\7L\2\2\u023f\u0241\7\u00aa\2\2\u0240\u023f\3\2\2\2"+
		"\u0240\u0241\3\2\2\2\u0241\u0242\3\2\2\2\u0242\u0243\7\u00a1\2\2\u0243"+
		"\u0244\5\u00acW\2\u0244\u0245\7\r\2\2\u0245\u024f\7\u00e6\2\2\u0246\u0247"+
		"\7\u0093\2\2\u0247\u024c\5<\37\2\u0248\u0249\7\5\2\2\u0249\u024b\5<\37"+
		"\2\u024a\u0248\3\2\2\2\u024b\u024e\3\2\2\2\u024c\u024a\3\2\2\2\u024c\u024d"+
		"\3\2\2\2\u024d\u0250\3\2\2\2\u024e\u024c\3\2\2\2\u024f\u0246\3\2\2\2\u024f"+
		"\u0250\3\2\2\2\u0250\u031a\3\2\2\2\u0251\u0253\7`\2\2\u0252\u0254\7\u00aa"+
		"\2\2\u0253\u0252\3\2\2\2\u0253\u0254\3\2\2\2\u0254\u0255\3\2\2\2\u0255"+
		"\u0258\7\u00a1\2\2\u0256\u0257\7v\2\2\u0257\u0259\7 \2\2\u0258\u0256\3"+
		"\2\2\2\u0258\u0259\3\2\2\2\u0259\u025a\3\2\2\2\u025a\u031a\5\u00acW\2"+
		"\u025b\u025d\7T\2\2\u025c\u025e\t\4\2\2\u025d\u025c\3\2\2\2\u025d\u025e"+
		"\3\2\2\2\u025e\u025f\3\2\2\2\u025f\u031a\5\n\6\2\u0260\u0261\7Y\2\2\u0261"+
		"\u0264\7Z\2\2\u0262\u0263\t\5\2\2\u0263\u0265\5\u00aeX\2\u0264\u0262\3"+
		"\2\2\2\u0264\u0265\3\2\2\2\u0265\u026a\3\2\2\2\u0266\u0268\7\"\2\2\u0267"+
		"\u0266\3\2\2\2\u0267\u0268\3\2\2\2\u0268\u0269\3\2\2\2\u0269\u026b\7\u00e6"+
		"\2\2\u026a\u0267\3\2\2\2\u026a\u026b\3\2\2\2\u026b\u031a\3\2\2\2\u026c"+
		"\u026d\7Y\2\2\u026d\u026e\7M\2\2\u026e\u0271\7\u00a2\2\2\u026f\u0270\t"+
		"\5\2\2\u0270\u0272\5\u00aeX\2\u0271\u026f\3\2\2\2\u0271\u0272\3\2\2\2"+
		"\u0272\u0273\3\2\2\2\u0273\u0274\7\"\2\2\u0274\u0276\7\u00e6\2\2\u0275"+
		"\u0277\5\34\17\2\u0276\u0275\3\2\2\2\u0276\u0277\3\2\2\2\u0277\u031a\3"+
		"\2\2\2\u0278\u0279\7Y\2\2\u0279\u027c\7\u00c4\2\2\u027a\u027b\7\"\2\2"+
		"\u027b\u027d\7\u00e6\2\2\u027c\u027a\3\2\2\2\u027c\u027d\3\2\2\2\u027d"+
		"\u031a\3\2\2\2\u027e\u027f\7Y\2\2\u027f\u0280\7\u00ad\2\2\u0280\u0285"+
		"\5r:\2\u0281\u0282\7\3\2\2\u0282\u0283\5.\30\2\u0283\u0284\7\4\2\2\u0284"+
		"\u0286\3\2\2\2\u0285\u0281\3\2\2\2\u0285\u0286\3\2\2\2\u0286\u031a\3\2"+
		"\2\2\u0287\u0288\7Y\2\2\u0288\u0289\7[\2\2\u0289\u028a\t\5\2\2\u028a\u028d"+
		"\5r:\2\u028b\u028c\t\5\2\2\u028c\u028e\5\u00aeX\2\u028d\u028b\3\2\2\2"+
		"\u028d\u028e\3\2\2\2\u028e\u031a\3\2\2\2\u028f\u0290\7Y\2\2\u0290\u0291"+
		"\7^\2\2\u0291\u0293\5r:\2\u0292\u0294\5\34\17\2\u0293\u0292\3\2\2\2\u0293"+
		"\u0294\3\2\2\2\u0294\u031a\3\2\2\2\u0295\u0297\7Y\2\2\u0296\u0298\5\u00ae"+
		"X\2\u0297\u0296\3\2\2\2\u0297\u0298\3\2\2\2\u0298\u0299\3\2\2\2\u0299"+
		"\u02a1\7_\2\2\u029a\u029c\7\"\2\2\u029b\u029a\3\2\2\2\u029b\u029c\3\2"+
		"\2\2\u029c\u029f\3\2\2\2\u029d\u02a0\5\u00acW\2\u029e\u02a0\7\u00e6\2"+
		"\2\u029f\u029d\3\2\2\2\u029f\u029e\3\2\2\2\u02a0\u02a2\3\2\2\2\u02a1\u029b"+
		"\3\2\2\2\u02a1\u02a2\3\2\2\2\u02a2\u031a\3\2\2\2\u02a3\u02a4\7Y\2\2\u02a4"+
		"\u02a5\7L\2\2\u02a5\u02a6\7M\2\2\u02a6\u031a\5r:\2\u02a7\u02a8\t\6\2\2"+
		"\u02a8\u02aa\7\u00a1\2\2\u02a9\u02ab\7\u00a2\2\2\u02aa\u02a9\3\2\2\2\u02aa"+
		"\u02ab\3\2\2\2\u02ab\u02ac\3\2\2\2\u02ac\u031a\5 \21\2\u02ad\u02ae\t\6"+
		"\2\2\u02ae\u02b0\7\u00c3\2\2\u02af\u02b1\7\u00a2\2\2\u02b0\u02af\3\2\2"+
		"\2\u02b0\u02b1\3\2\2\2\u02b1\u02b2\3\2\2\2\u02b2\u031a\5\u00aeX\2\u02b3"+
		"\u02b5\t\6\2\2\u02b4\u02b6\7M\2\2\u02b5\u02b4\3\2\2\2\u02b5\u02b6\3\2"+
		"\2\2\u02b6\u02b8\3\2\2\2\u02b7\u02b9\t\7\2\2\u02b8\u02b7\3\2\2\2\u02b8"+
		"\u02b9\3\2\2\2\u02b9\u02ba\3\2\2\2\u02ba\u02bc\5r:\2\u02bb\u02bd\5\34"+
		"\17\2\u02bc\u02bb\3\2\2\2\u02bc\u02bd\3\2\2\2\u02bd\u02bf\3\2\2\2\u02be"+
		"\u02c0\5\"\22\2\u02bf\u02be\3\2\2\2\u02bf\u02c0\3\2\2\2\u02c0\u031a\3"+
		"\2\2\2\u02c1\u02c2\7\u00a3\2\2\u02c2\u02c3\7M\2\2\u02c3\u031a\5r:\2\u02c4"+
		"\u02c8\7\u00a3\2\2\u02c5\u02c7\13\2\2\2\u02c6\u02c5\3\2\2\2\u02c7\u02ca"+
		"\3\2\2\2\u02c8\u02c9\3\2\2\2\u02c8\u02c6\3\2\2\2\u02c9\u031a\3\2\2\2\u02ca"+
		"\u02c8\3\2\2\2\u02cb\u02cd\7\u00a5\2\2\u02cc\u02ce\7\u00a7\2\2\u02cd\u02cc"+
		"\3\2\2\2\u02cd\u02ce\3\2\2\2\u02ce\u02cf\3\2\2\2\u02cf\u02d0\7M\2\2\u02d0"+
		"\u02d5\5r:\2\u02d1\u02d3\7\r\2\2\u02d2\u02d1\3\2\2\2\u02d2\u02d3\3\2\2"+
		"\2\u02d3\u02d4\3\2\2\2\u02d4\u02d6\5\26\f\2\u02d5\u02d2\3\2\2\2\u02d5"+
		"\u02d6\3\2\2\2\u02d6\u031a\3\2\2\2\u02d7\u02d8\7\u00a6\2\2\u02d8\u02db"+
		"\7M\2\2\u02d9\u02da\7v\2\2\u02da\u02dc\7 \2\2\u02db\u02d9\3\2\2\2\u02db"+
		"\u02dc\3\2\2\2\u02dc\u02dd\3\2\2\2\u02dd\u031a\5r:\2\u02de\u02df\7\u00a4"+
		"\2\2\u02df\u031a\7\u00a5\2\2\u02e0\u02e1\7\u00d7\2\2\u02e1\u02e3\7p\2"+
		"\2\u02e2\u02e4\7\u00e2\2\2\u02e3\u02e2\3\2\2\2\u02e3\u02e4\3\2\2\2\u02e4"+
		"\u02e5\3\2\2\2\u02e5\u02e6\7\u00e3\2\2\u02e6\u02e8\7\u00e6\2\2\u02e7\u02e9"+
		"\7\u0090\2\2\u02e8\u02e7\3\2\2\2\u02e8\u02e9\3\2\2\2\u02e9\u02ea\3\2\2"+
		"\2\u02ea\u02eb\7R\2\2\u02eb\u02ec\7M\2\2\u02ec\u02ee\5r:\2\u02ed\u02ef"+
		"\5\34\17\2\u02ee\u02ed\3\2\2\2\u02ee\u02ef\3\2\2\2\u02ef\u031a\3\2\2\2"+
		"\u02f0\u02f1\7\u00c6\2\2\u02f1\u02f2\7M\2\2\u02f2\u02f4\5r:\2\u02f3\u02f5"+
		"\5\34\17\2\u02f4\u02f3\3\2\2\2\u02f4\u02f5\3\2\2\2\u02f5\u031a\3\2\2\2"+
		"\u02f6\u02f7\7\u00d2\2\2\u02f7\u02f8\7\u00d3\2\2\u02f8\u02f9\7M\2\2\u02f9"+
		"\u031a\5r:\2\u02fa\u02fb\t\b\2\2\u02fb\u02ff\5\u00aeX\2\u02fc\u02fe\13"+
		"\2\2\2\u02fd\u02fc\3\2\2\2\u02fe\u0301\3\2\2\2\u02ff\u0300\3\2\2\2\u02ff"+
		"\u02fd\3\2\2\2\u0300\u031a\3\2\2\2\u0301\u02ff\3\2\2\2\u0302\u0303\7n"+
		"\2\2\u0303\u0307\7\u00d8\2\2\u0304\u0306\13\2\2\2\u0305\u0304\3\2\2\2"+
		"\u0306\u0309\3\2\2\2\u0307\u0308\3\2\2\2\u0307\u0305\3\2\2\2\u0308\u031a"+
		"\3\2\2\2\u0309\u0307\3\2\2\2\u030a\u030e\7n\2\2\u030b\u030d\13\2\2\2\u030c"+
		"\u030b\3\2\2\2\u030d\u0310\3\2\2\2\u030e\u030f\3\2\2\2\u030e\u030c\3\2"+
		"\2\2\u030f\u031a\3\2\2\2\u0310\u030e\3\2\2\2\u0311\u031a\7o\2\2\u0312"+
		"\u0316\5\f\7\2\u0313\u0315\13\2\2\2\u0314\u0313\3\2\2\2\u0315\u0318\3"+
		"\2\2\2\u0316\u0317\3\2\2\2\u0316\u0314\3\2\2\2\u0317\u031a\3\2\2\2\u0318"+
		"\u0316\3\2\2\2\u0319\u00c4\3\2\2\2\u0319\u00c5\3\2\2\2\u0319\u00c7\3\2"+
		"\2\2\u0319\u00db\3\2\2\2\u0319\u00e2\3\2\2\2\u0319\u00ec\3\2\2\2\u0319"+
		"\u010d\3\2\2\2\u0319\u0139\3\2\2\2\u0319\u0146\3\2\2\2\u0319\u0154\3\2"+
		"\2\2\u0319\u015b\3\2\2\2\u0319\u0162\3\2\2\2\u0319\u016d\3\2\2\2\u0319"+
		"\u017c\3\2\2\2\u0319\u018a\3\2\2\2\u0319\u0194\3\2\2\2\u0319\u01a2\3\2"+
		"\2\2\u0319\u01b0\3\2\2\2\u0319\u01b8\3\2\2\2\u0319\u01cb\3\2\2\2\u0319"+
		"\u01db\3\2\2\2\u0319\u01e4\3\2\2\2\u0319\u01ea\3\2\2\2\u0319\u01f4\3\2"+
		"\2\2\u0319\u01fb\3\2\2\2\u0319\u0220\3\2\2\2\u0319\u0236\3\2\2\2\u0319"+
		"\u023e\3\2\2\2\u0319\u0251\3\2\2\2\u0319\u025b\3\2\2\2\u0319\u0260\3\2"+
		"\2\2\u0319\u026c\3\2\2\2\u0319\u0278\3\2\2\2\u0319\u027e\3\2\2\2\u0319"+
		"\u0287\3\2\2\2\u0319\u028f\3\2\2\2\u0319\u0295\3\2\2\2\u0319\u02a3\3\2"+
		"\2\2\u0319\u02a7\3\2\2\2\u0319\u02ad\3\2\2\2\u0319\u02b3\3\2\2\2\u0319"+
		"\u02c1\3\2\2\2\u0319\u02c4\3\2\2\2\u0319\u02cb\3\2\2\2\u0319\u02d7\3\2"+
		"\2\2\u0319\u02de\3\2\2\2\u0319\u02e0\3\2\2\2\u0319\u02f0\3\2\2\2\u0319"+
		"\u02f6\3\2\2\2\u0319\u02fa\3\2\2\2\u0319\u0302\3\2\2\2\u0319\u030a\3\2"+
		"\2\2\u0319\u0311\3\2\2\2\u0319\u0312\3\2\2\2\u031a\13\3\2\2\2\u031b\u031c"+
		"\7L\2\2\u031c\u03cf\7\u00d8\2\2\u031d\u031e\7`\2\2\u031e\u03cf\7\u00d8"+
		"\2\2\u031f\u0321\7\u00cf\2\2\u0320\u0322\7\u00d8\2\2\u0321\u0320\3\2\2"+
		"\2\u0321\u0322\3\2\2\2\u0322\u03cf\3\2\2\2\u0323\u0325\7\u00ce\2\2\u0324"+
		"\u0326\7\u00d8\2\2\u0325\u0324\3\2\2\2\u0325\u0326\3\2\2\2\u0326\u03cf"+
		"\3\2\2\2\u0327\u0328\7Y\2\2\u0328\u03cf\7\u00cf\2\2\u0329\u032a\7Y\2\2"+
		"\u032a\u032c\7\u00d8\2\2\u032b\u032d\7\u00cf\2\2\u032c\u032b\3\2\2\2\u032c"+
		"\u032d\3\2\2\2\u032d\u03cf\3\2\2\2\u032e\u032f\7Y\2\2\u032f\u03cf\7\u00db"+
		"\2\2\u0330\u0331\7Y\2\2\u0331\u03cf\7\u00d9\2\2\u0332\u0333\7Y\2\2\u0333"+
		"\u0334\7E\2\2\u0334\u03cf\7\u00d9\2\2\u0335\u0336\7\u00d5\2\2\u0336\u03cf"+
		"\7M\2\2\u0337\u0338\7\u00d6\2\2\u0338\u03cf\7M\2\2\u0339\u033a\7Y\2\2"+
		"\u033a\u03cf\7\u00da\2\2\u033b\u033c\7Y\2\2\u033c\u033d\7L\2\2\u033d\u03cf"+
		"\7M\2\2\u033e\u033f\7Y\2\2\u033f\u03cf\7\u00dc\2\2\u0340\u0341\7Y\2\2"+
		"\u0341\u03cf\7\u00de\2\2\u0342\u0343\7Y\2\2\u0343\u03cf\7\u00df\2\2\u0344"+
		"\u0345\7L\2\2\u0345\u03cf\7\u00dd\2\2\u0346\u0347\7`\2\2\u0347\u03cf\7"+
		"\u00dd\2\2\u0348\u0349\7h\2\2\u0349\u03cf\7\u00dd\2\2\u034a\u034b\7\u00d0"+
		"\2\2\u034b\u03cf\7M\2\2\u034c\u034d\7\u00d0\2\2\u034d\u03cf\7\u00c3\2"+
		"\2\u034e\u034f\7\u00d1\2\2\u034f\u03cf\7M\2\2\u0350\u0351\7\u00d1\2\2"+
		"\u0351\u03cf\7\u00c3\2\2\u0352\u0353\7L\2\2\u0353\u0354\7\u00aa\2\2\u0354"+
		"\u03cf\7u\2\2\u0355\u0356\7`\2\2\u0356\u0357\7\u00aa\2\2\u0357\u03cf\7"+
		"u\2\2\u0358\u0359\7h\2\2\u0359\u035a\7M\2\2\u035a\u035b\5r:\2\u035b\u035c"+
		"\7\36\2\2\u035c\u035d\7\u00be\2\2\u035d\u03cf\3\2\2\2\u035e\u035f\7h\2"+
		"\2\u035f\u0360\7M\2\2\u0360\u0361\5r:\2\u0361\u0362\7\u00be\2\2\u0362"+
		"\u0363\7\22\2\2\u0363\u03cf\3\2\2\2\u0364\u0365\7h\2\2\u0365\u0366\7M"+
		"\2\2\u0366\u0367\5r:\2\u0367\u0368\7\36\2\2\u0368\u0369\7\u00bf\2\2\u0369"+
		"\u03cf\3\2\2\2\u036a\u036b\7h\2\2\u036b\u036c\7M\2\2\u036c\u036d\5r:\2"+
		"\u036d\u036e\7\u00b0\2\2\u036e\u036f\7\22\2\2\u036f\u03cf\3\2\2\2\u0370"+
		"\u0371\7h\2\2\u0371\u0372\7M\2\2\u0372\u0373\5r:\2\u0373\u0374\7\36\2"+
		"\2\u0374\u0375\7\u00b0\2\2\u0375\u03cf\3\2\2\2\u0376\u0377\7h\2\2\u0377"+
		"\u0378\7M\2\2\u0378\u0379\5r:\2\u0379\u037a\7\36\2\2\u037a\u037b\7\u00b1"+
		"\2\2\u037b\u037c\7\r\2\2\u037c\u037d\7\u00b2\2\2\u037d\u03cf\3\2\2\2\u037e"+
		"\u037f\7h\2\2\u037f\u0380\7M\2\2\u0380\u0381\5r:\2\u0381\u0382\7n\2\2"+
		"\u0382\u0383\7\u00b0\2\2\u0383\u0384\7\u00b3\2\2\u0384\u03cf\3\2\2\2\u0385"+
		"\u0386\7h\2\2\u0386\u0387\7M\2\2\u0387\u0388\5r:\2\u0388\u0389\7\u00b4"+
		"\2\2\u0389\u038a\7?\2\2\u038a\u03cf\3\2\2\2\u038b\u038c\7h\2\2\u038c\u038d"+
		"\7M\2\2\u038d\u038e\5r:\2\u038e\u038f\7\u00b5\2\2\u038f\u0390\7?\2\2\u0390"+
		"\u03cf\3\2\2\2\u0391\u0392\7h\2\2\u0392\u0393\7M\2\2\u0393\u0394\5r:\2"+
		"\u0394\u0395\7\u00b6\2\2\u0395\u0396\7?\2\2\u0396\u03cf\3\2\2\2\u0397"+
		"\u0398\7h\2\2\u0398\u0399\7M\2\2\u0399\u039a\5r:\2\u039a\u039b\7\u00b8"+
		"\2\2\u039b\u03cf\3\2\2\2\u039c\u039d\7h\2\2\u039d\u039e\7M\2\2\u039e\u03a0"+
		"\5r:\2\u039f\u03a1\5\34\17\2\u03a0\u039f\3\2\2\2\u03a0\u03a1\3\2\2\2\u03a1"+
		"\u03a2\3\2\2\2\u03a2\u03a3\7\u00b9\2\2\u03a3\u03cf\3\2\2\2\u03a4\u03a5"+
		"\7h\2\2\u03a5\u03a6\7M\2\2\u03a6\u03a8\5r:\2\u03a7\u03a9\5\34\17\2\u03a8"+
		"\u03a7\3\2\2\2\u03a8\u03a9\3\2\2\2\u03a9\u03aa\3\2\2\2\u03aa\u03ab\7\u00ba"+
		"\2\2\u03ab\u03cf\3\2\2\2\u03ac\u03ad\7h\2\2\u03ad\u03ae\7M\2\2\u03ae\u03b0"+
		"\5r:\2\u03af\u03b1\5\34\17\2\u03b0\u03af\3\2\2\2\u03b0\u03b1\3\2\2\2\u03b1"+
		"\u03b2\3\2\2\2\u03b2\u03b3\7n\2\2\u03b3\u03b4\7\u00b7\2\2\u03b4\u03cf"+
		"\3\2\2\2\u03b5\u03b6\7h\2\2\u03b6\u03b7\7M\2\2\u03b7\u03b9\5r:\2\u03b8"+
		"\u03ba\5\34\17\2\u03b9\u03b8\3\2\2\2\u03b9\u03ba\3\2\2\2\u03ba\u03bb\3"+
		"\2\2\2\u03bb\u03bc\7\f\2\2\u03bc\u03bd\7[\2\2\u03bd\u03cf\3\2\2\2\u03be"+
		"\u03bf\7h\2\2\u03bf\u03c0\7M\2\2\u03c0\u03c2\5r:\2\u03c1\u03c3\5\34\17"+
		"\2\u03c2\u03c1\3\2\2\2\u03c2\u03c3\3\2\2\2\u03c3\u03c4\3\2\2\2\u03c4\u03c5"+
		"\7O\2\2\u03c5\u03c6\7[\2\2\u03c6\u03cf\3\2\2\2\u03c7\u03c8\7q\2\2\u03c8"+
		"\u03cf\7r\2\2\u03c9\u03cf\7s\2\2\u03ca\u03cf\7t\2\2\u03cb\u03cf\7\u00c5"+
		"\2\2\u03cc\u03cd\7Q\2\2\u03cd\u03cf\7\13\2\2\u03ce\u031b\3\2\2\2\u03ce"+
		"\u031d\3\2\2\2\u03ce\u031f\3\2\2\2\u03ce\u0323\3\2\2\2\u03ce\u0327\3\2"+
		"\2\2\u03ce\u0329\3\2\2\2\u03ce\u032e\3\2\2\2\u03ce\u0330\3\2\2\2\u03ce"+
		"\u0332\3\2\2\2\u03ce\u0335\3\2\2\2\u03ce\u0337\3\2\2\2\u03ce\u0339\3\2"+
		"\2\2\u03ce\u033b\3\2\2\2\u03ce\u033e\3\2\2\2\u03ce\u0340\3\2\2\2\u03ce"+
		"\u0342\3\2\2\2\u03ce\u0344\3\2\2\2\u03ce\u0346\3\2\2\2\u03ce\u0348\3\2"+
		"\2\2\u03ce\u034a\3\2\2\2\u03ce\u034c\3\2\2\2\u03ce\u034e\3\2\2\2\u03ce"+
		"\u0350\3\2\2\2\u03ce\u0352\3\2\2\2\u03ce\u0355\3\2\2\2\u03ce\u0358\3\2"+
		"\2\2\u03ce\u035e\3\2\2\2\u03ce\u0364\3\2\2\2\u03ce\u036a\3\2\2\2\u03ce"+
		"\u0370\3\2\2\2\u03ce\u0376\3\2\2\2\u03ce\u037e\3\2\2\2\u03ce\u0385\3\2"+
		"\2\2\u03ce\u038b\3\2\2\2\u03ce\u0391\3\2\2\2\u03ce\u0397\3\2\2\2\u03ce"+
		"\u039c\3\2\2\2\u03ce\u03a4\3\2\2\2\u03ce\u03ac\3\2\2\2\u03ce\u03b5\3\2"+
		"\2\2\u03ce\u03be\3\2\2\2\u03ce\u03c7\3\2\2\2\u03ce\u03c9\3\2\2\2\u03ce"+
		"\u03ca\3\2\2\2\u03ce\u03cb\3\2\2\2\u03ce\u03cc\3\2\2\2\u03cf\r\3\2\2\2"+
		"\u03d0\u03d2\7L\2\2\u03d1\u03d3\7\u00aa\2\2\u03d2\u03d1\3\2\2\2\u03d2"+
		"\u03d3\3\2\2\2\u03d3\u03d5\3\2\2\2\u03d4\u03d6\7\u00cc\2\2\u03d5\u03d4"+
		"\3\2\2\2\u03d5\u03d6\3\2\2\2\u03d6\u03d7\3\2\2\2\u03d7\u03db\7M\2\2\u03d8"+
		"\u03d9\7v\2\2\u03d9\u03da\7\36\2\2\u03da\u03dc\7 \2\2\u03db\u03d8\3\2"+
		"\2\2\u03db\u03dc\3\2\2\2\u03dc\u03dd\3\2\2\2\u03dd\u03de\5r:\2\u03de\17"+
		"\3\2\2\2\u03df\u03e0\7\u00be\2\2\u03e0\u03e1\7\22\2\2\u03e1\u03e5\5`\61"+
		"\2\u03e2\u03e3\7\u00bf\2\2\u03e3\u03e4\7\22\2\2\u03e4\u03e6\5d\63\2\u03e5"+
		"\u03e2\3\2\2\2\u03e5\u03e6\3\2\2\2\u03e6\u03e7\3\2\2\2\u03e7\u03e8\7R"+
		"\2\2\u03e8\u03e9\7\u00eb\2\2\u03e9\u03ea\7\u00af\2\2\u03ea\21\3\2\2\2"+
		"\u03eb\u03ec\7\u00b0\2\2\u03ec\u03ed\7\22\2\2\u03ed\u03ee\5`\61\2\u03ee"+
		"\u03f1\7;\2\2\u03ef\u03f2\5\62\32\2\u03f0\u03f2\5\64\33\2\u03f1\u03ef"+
		"\3\2\2\2\u03f1\u03f0\3\2\2\2\u03f2\u03f6\3\2\2\2\u03f3\u03f4\7\u00b1\2"+
		"\2\u03f4\u03f5\7\r\2\2\u03f5\u03f7\7\u00b2\2\2\u03f6\u03f3\3\2\2\2\u03f6"+
		"\u03f7\3\2\2\2\u03f7\23\3\2\2\2\u03f8\u03f9\7\u00b3\2\2\u03f9\u03fa\7"+
		"\u00e6\2\2\u03fa\25\3\2\2\2\u03fb\u03fd\5$\23\2\u03fc\u03fb\3\2\2\2\u03fc"+
		"\u03fd\3\2\2\2\u03fd\u03fe\3\2\2\2\u03fe\u03ff\5> \2\u03ff\27\3\2\2\2"+
		"\u0400\u0401\7P\2\2\u0401\u0402\7\u0090\2\2\u0402\u0403\7M\2\2\u0403\u040a"+
		"\5r:\2\u0404\u0408\5\34\17\2\u0405\u0406\7v\2\2\u0406\u0407\7\36\2\2\u0407"+
		"\u0409\7 \2\2\u0408\u0405\3\2\2\2\u0408\u0409\3\2\2\2\u0409\u040b\3\2"+
		"\2\2\u040a\u0404\3\2\2\2\u040a\u040b\3\2\2\2\u040b\u0416\3\2\2\2\u040c"+
		"\u040d\7P\2\2\u040d\u040f\7R\2\2\u040e\u0410\7M\2\2\u040f\u040e\3\2\2"+
		"\2\u040f\u0410\3\2\2\2\u0410\u0411\3\2\2\2\u0411\u0413\5r:\2\u0412\u0414"+
		"\5\34\17\2\u0413\u0412\3\2\2\2\u0413\u0414\3\2\2\2\u0414\u0416\3\2\2\2"+
		"\u0415\u0400\3\2\2\2\u0415\u040c\3\2\2\2\u0416\31\3\2\2\2\u0417\u0419"+
		"\5\34\17\2\u0418\u041a\5\24\13\2\u0419\u0418\3\2\2\2\u0419\u041a\3\2\2"+
		"\2\u041a\33\3\2\2\2\u041b\u041c\7?\2\2\u041c\u041d\7\3\2\2\u041d\u0422"+
		"\5\36\20\2\u041e\u041f\7\5\2\2\u041f\u0421\5\36\20\2\u0420\u041e\3\2\2"+
		"\2\u0421\u0424\3\2\2\2\u0422\u0420\3\2\2\2\u0422\u0423\3\2\2\2\u0423\u0425"+
		"\3\2\2\2\u0424\u0422\3\2\2\2\u0425\u0426\7\4\2\2\u0426\35\3\2\2\2\u0427"+
		"\u042a\5\u00aeX\2\u0428\u0429\7w\2\2\u0429\u042b\5\u0084C\2\u042a\u0428"+
		"\3\2\2\2\u042a\u042b\3\2\2\2\u042b\37\3\2\2\2\u042c\u0432\5\u00acW\2\u042d"+
		"\u0432\7\u00e6\2\2\u042e\u0432\5\u0086D\2\u042f\u0432\5\u0088E\2\u0430"+
		"\u0432\5\u008aF\2\u0431\u042c\3\2\2\2\u0431\u042d\3\2\2\2\u0431\u042e"+
		"\3\2\2\2\u0431\u042f\3\2\2\2\u0431\u0430\3\2\2\2\u0432!\3\2\2\2\u0433"+
		"\u043b\5\u00aeX\2\u0434\u0437\7\6\2\2\u0435\u0438\5\u00aeX\2\u0436\u0438"+
		"\7\u00e6\2\2\u0437\u0435\3\2\2\2\u0437\u0436\3\2\2\2\u0438\u043a\3\2\2"+
		"\2\u0439\u0434\3\2\2\2\u043a\u043d\3\2\2\2\u043b\u0439\3\2\2\2\u043b\u043c"+
		"\3\2\2\2\u043c#\3\2\2\2\u043d\u043b\3\2\2\2\u043e\u043f\7J\2\2\u043f\u0444"+
		"\5&\24\2\u0440\u0441\7\5\2\2\u0441\u0443\5&\24\2\u0442\u0440\3\2\2\2\u0443"+
		"\u0446\3\2\2\2\u0444\u0442\3\2\2\2\u0444\u0445\3\2\2\2\u0445%\3\2\2\2"+
		"\u0446\u0444\3\2\2\2\u0447\u0449\5\u00aeX\2\u0448\u044a\7\r\2\2\u0449"+
		"\u0448\3\2\2\2\u0449\u044a\3\2\2\2\u044a\u044b\3\2\2\2\u044b\u044c\7\3"+
		"\2\2\u044c\u044d\5\26\f\2\u044d\u044e\7\4\2\2\u044e\'\3\2\2\2\u044f\u0450"+
		"\7\u0093\2\2\u0450\u0451\5\u00acW\2\u0451)\3\2\2\2\u0452\u0453\7\3\2\2"+
		"\u0453\u0458\5,\27\2\u0454\u0455\7\5\2\2\u0455\u0457\5,\27\2\u0456\u0454"+
		"\3\2\2\2\u0457\u045a\3\2\2\2\u0458\u0456\3\2\2\2\u0458\u0459\3\2\2\2\u0459"+
		"\u045b\3\2\2\2\u045a\u0458\3\2\2\2\u045b\u045c\7\4\2\2\u045c+\3\2\2\2"+
		"\u045d\u0462\5.\30\2\u045e\u0460\7w\2\2\u045f\u045e\3\2\2\2\u045f\u0460"+
		"\3\2\2\2\u0460\u0461\3\2\2\2\u0461\u0463\5\60\31\2\u0462\u045f\3\2\2\2"+
		"\u0462\u0463\3\2\2\2\u0463-\3\2\2\2\u0464\u0469\5\u00aeX\2\u0465\u0466"+
		"\7\6\2\2\u0466\u0468\5\u00aeX\2\u0467\u0465\3\2\2\2\u0468\u046b\3\2\2"+
		"\2\u0469\u0467\3\2\2\2\u0469\u046a\3\2\2\2\u046a\u046e\3\2\2\2\u046b\u0469"+
		"\3\2\2\2\u046c\u046e\7\u00e6\2\2\u046d\u0464\3\2\2\2\u046d\u046c\3\2\2"+
		"\2\u046e/\3\2\2\2\u046f\u0474\7\u00eb\2\2\u0470\u0474\7\u00ec\2\2\u0471"+
		"\u0474\5\u008cG\2\u0472\u0474\7\u00e6\2\2\u0473\u046f\3\2\2\2\u0473\u0470"+
		"\3\2\2\2\u0473\u0471\3\2\2\2\u0473\u0472\3\2\2\2\u0474\61\3\2\2\2\u0475"+
		"\u0476\7\3\2\2\u0476\u047b\5\u0084C\2\u0477\u0478\7\5\2\2\u0478\u047a"+
		"\5\u0084C\2\u0479\u0477\3\2\2\2\u047a\u047d\3\2\2\2\u047b\u0479\3\2\2"+
		"\2\u047b\u047c\3\2\2\2\u047c\u047e\3\2\2\2\u047d\u047b\3\2\2\2\u047e\u047f"+
		"\7\4\2\2\u047f\63\3\2\2\2\u0480\u0481\7\3\2\2\u0481\u0486\5\62\32\2\u0482"+
		"\u0483\7\5\2\2\u0483\u0485\5\62\32\2\u0484\u0482\3\2\2\2\u0485\u0488\3"+
		"\2\2\2\u0486\u0484\3\2\2\2\u0486\u0487\3\2\2\2\u0487\u0489\3\2\2\2\u0488"+
		"\u0486\3\2\2\2\u0489\u048a\7\4\2\2\u048a\65\3\2\2\2\u048b\u048c\7\u00b1"+
		"\2\2\u048c\u048d\7\r\2\2\u048d\u0492\58\35\2\u048e\u048f\7\u00b1\2\2\u048f"+
		"\u0490\7\22\2\2\u0490\u0492\5:\36\2\u0491\u048b\3\2\2\2\u0491\u048e\3"+
		"\2\2\2\u0492\67\3\2\2\2\u0493\u0494\7\u00c1\2\2\u0494\u0495\7\u00e6\2"+
		"\2\u0495\u0496\7\u00c2\2\2\u0496\u0499\7\u00e6\2\2\u0497\u0499\5\u00ae"+
		"X\2\u0498\u0493\3\2\2\2\u0498\u0497\3\2\2\2\u04999\3\2\2\2\u049a\u049e"+
		"\7\u00e6\2\2\u049b\u049c\7J\2\2\u049c\u049d\7\u0095\2\2\u049d\u049f\5"+
		"*\26\2\u049e\u049b\3\2\2\2\u049e\u049f\3\2\2\2\u049f;\3\2\2\2\u04a0\u04a1"+
		"\5\u00aeX\2\u04a1\u04a2\7\u00e6\2\2\u04a2=\3\2\2\2\u04a3\u04a5\5\30\r"+
		"\2\u04a4\u04a3\3\2\2\2\u04a4\u04a5\3\2\2\2\u04a5\u04a6\3\2\2\2\u04a6\u04a7"+
		"\5D#\2\u04a7\u04a8\5@!\2\u04a8\u04b0\3\2\2\2\u04a9\u04ab\5L\'\2\u04aa"+
		"\u04ac\5B\"\2\u04ab\u04aa\3\2\2\2\u04ac\u04ad\3\2\2\2\u04ad\u04ab\3\2"+
		"\2\2\u04ad\u04ae\3\2\2\2\u04ae\u04b0\3\2\2\2\u04af\u04a4\3\2\2\2\u04af"+
		"\u04a9\3\2\2\2\u04b0?\3\2\2\2\u04b1\u04b2\7\27\2\2\u04b2\u04b3\7\22\2"+
		"\2\u04b3\u04b8\5H%\2\u04b4\u04b5\7\5\2\2\u04b5\u04b7\5H%\2\u04b6\u04b4"+
		"\3\2\2\2\u04b7\u04ba\3\2\2\2\u04b8\u04b6\3\2\2\2\u04b8\u04b9\3\2\2\2\u04b9"+
		"\u04bc\3\2\2\2\u04ba\u04b8\3\2\2\2\u04bb\u04b1\3\2\2\2\u04bb\u04bc\3\2"+
		"\2\2\u04bc\u04c7\3\2\2\2\u04bd\u04be\7\u008e\2\2\u04be\u04bf\7\22\2\2"+
		"\u04bf\u04c4\5x=\2\u04c0\u04c1\7\5\2\2\u04c1\u04c3\5x=\2\u04c2\u04c0\3"+
		"\2\2\2\u04c3\u04c6\3\2\2\2\u04c4\u04c2\3\2\2\2\u04c4\u04c5\3\2\2\2\u04c5"+
		"\u04c8\3\2\2\2\u04c6\u04c4\3\2\2\2\u04c7\u04bd\3\2\2\2\u04c7\u04c8\3\2"+
		"\2\2\u04c8\u04d3\3\2\2\2\u04c9\u04ca\7\u008f\2\2\u04ca\u04cb\7\22\2\2"+
		"\u04cb\u04d0\5x=\2\u04cc\u04cd\7\5\2\2\u04cd\u04cf\5x=\2\u04ce\u04cc\3"+
		"\2\2\2\u04cf\u04d2\3\2\2\2\u04d0\u04ce\3\2\2\2\u04d0\u04d1\3\2\2\2\u04d1"+
		"\u04d4\3\2\2\2\u04d2\u04d0\3\2\2\2\u04d3\u04c9\3\2\2\2\u04d3\u04d4\3\2"+
		"\2\2\u04d4\u04df\3\2\2\2\u04d5\u04d6\7\u008d\2\2\u04d6\u04d7\7\22\2\2"+
		"\u04d7\u04dc\5H%\2\u04d8\u04d9\7\5\2\2\u04d9\u04db\5H%\2\u04da\u04d8\3"+
		"\2\2\2\u04db\u04de\3\2\2\2\u04dc\u04da\3\2\2\2\u04dc\u04dd\3\2\2\2\u04dd"+
		"\u04e0\3\2\2\2\u04de\u04dc\3\2\2\2\u04df\u04d5\3\2\2\2\u04df\u04e0\3\2"+
		"\2\2\u04e0\u04e2\3\2\2\2\u04e1\u04e3\5\u00a2R\2\u04e2\u04e1\3\2\2\2\u04e2"+
		"\u04e3\3\2\2\2\u04e3\u04e6\3\2\2\2\u04e4\u04e5\7\31\2\2\u04e5\u04e7\5"+
		"x=\2\u04e6\u04e4\3\2\2\2\u04e6\u04e7\3\2\2\2\u04e7A\3\2\2\2\u04e8\u04ea"+
		"\5\30\r\2\u04e9\u04e8\3\2\2\2\u04e9\u04ea\3\2\2\2\u04ea\u04eb\3\2\2\2"+
		"\u04eb\u04ec\5J&\2\u04ec\u04ed\5@!\2\u04edC\3\2\2\2\u04ee\u04ef\b#\1\2"+
		"\u04ef\u04f0\5F$\2\u04f0\u04f9\3\2\2\2\u04f1\u04f2\f\3\2\2\u04f2\u04f4"+
		"\t\t\2\2\u04f3\u04f5\5T+\2\u04f4\u04f3\3\2\2\2\u04f4\u04f5\3\2\2\2\u04f5"+
		"\u04f6\3\2\2\2\u04f6\u04f8\5D#\4\u04f7\u04f1\3\2\2\2\u04f8\u04fb\3\2\2"+
		"\2\u04f9\u04f7\3\2\2\2\u04f9\u04fa\3\2\2\2\u04faE\3\2\2\2\u04fb\u04f9"+
		"\3\2\2\2\u04fc\u0505\5J&\2\u04fd\u04fe\7M\2\2\u04fe\u0505\5r:\2\u04ff"+
		"\u0505\5n8\2\u0500\u0501\7\3\2\2\u0501\u0502\5> \2\u0502\u0503\7\4\2\2"+
		"\u0503\u0505\3\2\2\2\u0504\u04fc\3\2\2\2\u0504\u04fd\3\2\2\2\u0504\u04ff"+
		"\3\2\2\2\u0504\u0500\3\2\2\2\u0505G\3\2\2\2\u0506\u0508\5x=\2\u0507\u0509"+
		"\t\n\2\2\u0508\u0507\3\2\2\2\u0508\u0509\3\2\2\2\u0509\u050c\3\2\2\2\u050a"+
		"\u050b\7(\2\2\u050b\u050d\t\13\2\2\u050c\u050a\3\2\2\2\u050c\u050d\3\2"+
		"\2\2\u050dI\3\2\2\2\u050e\u050f\7\n\2\2\u050f\u0510\7\u0091\2\2\u0510"+
		"\u0511\7\3\2\2\u0511\u0512\5v<\2\u0512\u0513\7\4\2\2\u0513\u0519\3\2\2"+
		"\2\u0514\u0515\7k\2\2\u0515\u0519\5v<\2\u0516\u0517\7\u0092\2\2\u0517"+
		"\u0519\5v<\2\u0518\u050e\3\2\2\2\u0518\u0514\3\2\2\2\u0518\u0516\3\2\2"+
		"\2\u0519\u051b\3\2\2\2\u051a\u051c\5p9\2\u051b\u051a\3\2\2\2\u051b\u051c"+
		"\3\2\2\2\u051c\u051f\3\2\2\2\u051d\u051e\7\u0097\2\2\u051e\u0520\7\u00e6"+
		"\2\2\u051f\u051d\3\2\2\2\u051f\u0520\3\2\2\2\u0520\u0521\3\2\2\2\u0521"+
		"\u0522\7\u0093\2\2\u0522\u052f\7\u00e6\2\2\u0523\u052d\7\r\2\2\u0524\u052e"+
		"\5b\62\2\u0525\u052e\5\u0098M\2\u0526\u0529\7\3\2\2\u0527\u052a\5b\62"+
		"\2\u0528\u052a\5\u0098M\2\u0529\u0527\3\2\2\2\u0529\u0528\3\2\2\2\u052a"+
		"\u052b\3\2\2\2\u052b\u052c\7\4\2\2\u052c\u052e\3\2\2\2\u052d\u0524\3\2"+
		"\2\2\u052d\u0525\3\2\2\2\u052d\u0526\3\2\2\2\u052e\u0530\3\2\2\2\u052f"+
		"\u0523\3\2\2\2\u052f\u0530\3\2\2\2\u0530\u0532\3\2\2\2\u0531\u0533\5p"+
		"9\2\u0532\u0531\3\2\2\2\u0532\u0533\3\2\2\2\u0533\u0536\3\2\2\2\u0534"+
		"\u0535\7\u0096\2\2\u0535\u0537\7\u00e6\2\2\u0536\u0534\3\2\2\2\u0536\u0537"+
		"\3\2\2\2\u0537\u0539\3\2\2\2\u0538\u053a\5L\'\2\u0539\u0538\3\2\2\2\u0539"+
		"\u053a\3\2\2\2\u053a\u053d\3\2\2\2\u053b\u053c\7\20\2\2\u053c\u053e\5"+
		"z>\2\u053d\u053b\3\2\2\2\u053d\u053e\3\2\2\2\u053e\u0566\3\2\2\2\u053f"+
		"\u0541\7\n\2\2\u0540\u0542\5T+\2\u0541\u0540\3\2\2\2\u0541\u0542\3\2\2"+
		"\2\u0542\u0543\3\2\2\2\u0543\u0545\5v<\2\u0544\u0546\5L\'\2\u0545\u0544"+
		"\3\2\2\2\u0545\u0546\3\2\2\2\u0546\u0550\3\2\2\2\u0547\u054d\5L\'\2\u0548"+
		"\u054a\7\n\2\2\u0549\u054b\5T+\2\u054a\u0549\3\2\2\2\u054a\u054b\3\2\2"+
		"\2\u054b\u054c\3\2\2\2\u054c\u054e\5v<\2\u054d\u0548\3\2\2\2\u054d\u054e"+
		"\3\2\2\2\u054e\u0550\3\2\2\2\u054f\u053f\3\2\2\2\u054f\u0547\3\2\2\2\u0550"+
		"\u0554\3\2\2\2\u0551\u0553\5R*\2\u0552\u0551\3\2\2\2\u0553\u0556\3\2\2"+
		"\2\u0554\u0552\3\2\2\2\u0554\u0555\3\2\2\2\u0555\u0559\3\2\2\2\u0556\u0554"+
		"\3\2\2\2\u0557\u0558\7\20\2\2\u0558\u055a\5z>\2\u0559\u0557\3\2\2\2\u0559"+
		"\u055a\3\2\2\2\u055a\u055c\3\2\2\2\u055b\u055d\5N(\2\u055c\u055b\3\2\2"+
		"\2\u055c\u055d\3\2\2\2\u055d\u0560\3\2\2\2\u055e\u055f\7\30\2\2\u055f"+
		"\u0561\5z>\2\u0560\u055e\3\2\2\2\u0560\u0561\3\2\2\2\u0561\u0563\3\2\2"+
		"\2\u0562\u0564\5\u00a2R\2\u0563\u0562\3\2\2\2\u0563\u0564\3\2\2\2\u0564"+
		"\u0566\3\2\2\2\u0565\u0518\3\2\2\2\u0565\u054f\3\2\2\2\u0566K\3\2\2\2"+
		"\u0567\u0568\7\13\2\2\u0568\u056d\5V,\2\u0569\u056a\7\5\2\2\u056a\u056c"+
		"\5V,\2\u056b\u0569\3\2\2\2\u056c\u056f\3\2\2\2\u056d\u056b\3\2\2\2\u056d"+
		"\u056e\3\2\2\2\u056e\u0573\3\2\2\2\u056f\u056d\3\2\2\2\u0570\u0572\5R"+
		"*\2\u0571\u0570\3\2\2\2\u0572\u0575\3\2\2\2\u0573\u0571\3\2\2\2\u0573"+
		"\u0574\3\2\2\2\u0574M\3\2\2\2\u0575\u0573\3\2\2\2\u0576\u0577\7\21\2\2"+
		"\u0577\u0578\7\22\2\2\u0578\u057d\5x=\2\u0579\u057a\7\5\2\2\u057a\u057c"+
		"\5x=\2\u057b\u0579\3\2\2\2\u057c\u057f\3\2\2\2\u057d\u057b\3\2\2\2\u057d"+
		"\u057e\3\2\2\2\u057e\u0591\3\2\2\2\u057f\u057d\3\2\2\2\u0580\u0581\7J"+
		"\2\2\u0581\u0592\7\26\2\2\u0582\u0583\7J\2\2\u0583\u0592\7\25\2\2\u0584"+
		"\u0585\7\23\2\2\u0585\u0586\7\24\2\2\u0586\u0587\7\3\2\2\u0587\u058c\5"+
		"P)\2\u0588\u0589\7\5\2\2\u0589\u058b\5P)\2\u058a\u0588\3\2\2\2\u058b\u058e"+
		"\3\2\2\2\u058c\u058a\3\2\2\2\u058c\u058d\3\2\2\2\u058d\u058f\3\2\2\2\u058e"+
		"\u058c\3\2\2\2\u058f\u0590\7\4\2\2\u0590\u0592\3\2\2\2\u0591\u0580\3\2"+
		"\2\2\u0591\u0582\3\2\2\2\u0591\u0584\3\2\2\2\u0591\u0592\3\2\2\2\u0592"+
		"O\3\2\2\2\u0593\u059c\7\3\2\2\u0594\u0599\5x=\2\u0595\u0596\7\5\2\2\u0596"+
		"\u0598\5x=\2\u0597\u0595\3\2\2\2\u0598\u059b\3\2\2\2\u0599\u0597\3\2\2"+
		"\2\u0599\u059a\3\2\2\2\u059a\u059d\3\2\2\2\u059b\u0599\3\2\2\2\u059c\u0594"+
		"\3\2\2\2\u059c\u059d\3\2\2\2\u059d\u059e\3\2\2\2\u059e\u05a1\7\4\2\2\u059f"+
		"\u05a1\5x=\2\u05a0\u0593\3\2\2\2\u05a0\u059f\3\2\2\2\u05a1Q\3\2\2\2\u05a2"+
		"\u05a3\7<\2\2\u05a3\u05a5\7N\2\2\u05a4\u05a6\7\64\2\2\u05a5\u05a4\3\2"+
		"\2\2\u05a5\u05a6\3\2\2\2\u05a6\u05a7\3\2\2\2\u05a7\u05a8\5\u00acW\2\u05a8"+
		"\u05b1\7\3\2\2\u05a9\u05ae\5x=\2\u05aa\u05ab\7\5\2\2\u05ab\u05ad\5x=\2"+
		"\u05ac\u05aa\3\2\2\2\u05ad\u05b0\3\2\2\2\u05ae\u05ac\3\2\2\2\u05ae\u05af"+
		"\3\2\2\2\u05af\u05b2\3\2\2\2\u05b0\u05ae\3\2\2\2\u05b1\u05a9\3\2\2\2\u05b1"+
		"\u05b2\3\2\2\2\u05b2\u05b3\3\2\2\2\u05b3\u05b4\7\4\2\2\u05b4\u05c0\5\u00ae"+
		"X\2\u05b5\u05b7\7\r\2\2\u05b6\u05b5\3\2\2\2\u05b6\u05b7\3\2\2\2\u05b7"+
		"\u05b8\3\2\2\2\u05b8\u05bd\5\u00aeX\2\u05b9\u05ba\7\5\2\2\u05ba\u05bc"+
		"\5\u00aeX\2\u05bb\u05b9\3\2\2\2\u05bc\u05bf\3\2\2\2\u05bd\u05bb\3\2\2"+
		"\2\u05bd\u05be\3\2\2\2\u05be\u05c1\3\2\2\2\u05bf\u05bd\3\2\2\2\u05c0\u05b6"+
		"\3\2\2\2\u05c0\u05c1\3\2\2\2\u05c1S\3\2\2\2\u05c2\u05c3\t\f\2\2\u05c3"+
		"U\3\2\2\2\u05c4\u05c8\5l\67\2\u05c5\u05c7\5X-\2\u05c6\u05c5\3\2\2\2\u05c7"+
		"\u05ca\3\2\2\2\u05c8\u05c6\3\2\2\2\u05c8\u05c9\3\2\2\2\u05c9W\3\2\2\2"+
		"\u05ca\u05c8\3\2\2\2\u05cb\u05cc\5Z.\2\u05cc\u05cd\7\62\2\2\u05cd\u05cf"+
		"\5l\67\2\u05ce\u05d0\5\\/\2\u05cf\u05ce\3\2\2\2\u05cf\u05d0\3\2\2\2\u05d0"+
		"\u05d7\3\2\2\2\u05d1\u05d2\7:\2\2\u05d2\u05d3\5Z.\2\u05d3\u05d4\7\62\2"+
		"\2\u05d4\u05d5\5l\67\2\u05d5\u05d7\3\2\2\2\u05d6\u05cb\3\2\2\2\u05d6\u05d1"+
		"\3\2\2\2\u05d7Y\3\2\2\2\u05d8\u05da\7\65\2\2\u05d9\u05d8\3\2\2\2\u05d9"+
		"\u05da\3\2\2\2\u05da\u05ef\3\2\2\2\u05db\u05ef\7\63\2\2\u05dc\u05de\7"+
		"\66\2\2\u05dd\u05df\7\64\2\2\u05de\u05dd\3\2\2\2\u05de\u05df\3\2\2\2\u05df"+
		"\u05ef\3\2\2\2\u05e0\u05e1\7\66\2\2\u05e1\u05ef\7\67\2\2\u05e2\u05e4\7"+
		"8\2\2\u05e3\u05e5\7\64\2\2\u05e4\u05e3\3\2\2\2\u05e4\u05e5\3\2\2\2\u05e5"+
		"\u05ef\3\2\2\2\u05e6\u05e8\79\2\2\u05e7\u05e9\7\64\2\2\u05e8\u05e7\3\2"+
		"\2\2\u05e8\u05e9\3\2\2\2\u05e9\u05ef\3\2\2\2\u05ea\u05ec\7\66\2\2\u05eb"+
		"\u05ea\3\2\2\2\u05eb\u05ec\3\2\2\2\u05ec\u05ed\3\2\2\2\u05ed\u05ef\7\u00e1"+
		"\2\2\u05ee\u05d9\3\2\2\2\u05ee\u05db\3\2\2\2\u05ee\u05dc\3\2\2\2\u05ee"+
		"\u05e0\3\2\2\2\u05ee\u05e2\3\2\2\2\u05ee\u05e6\3\2\2\2\u05ee\u05eb\3\2"+
		"\2\2\u05ef[\3\2\2\2\u05f0\u05f1\7;\2\2\u05f1\u05ff\5z>\2\u05f2\u05f3\7"+
		"\u0093\2\2\u05f3\u05f4\7\3\2\2\u05f4\u05f9\5\u00aeX\2\u05f5\u05f6\7\5"+
		"\2\2\u05f6\u05f8\5\u00aeX\2\u05f7\u05f5\3\2\2\2\u05f8\u05fb\3\2\2\2\u05f9"+
		"\u05f7\3\2\2\2\u05f9\u05fa\3\2\2\2\u05fa\u05fc\3\2\2\2\u05fb\u05f9\3\2"+
		"\2\2\u05fc\u05fd\7\4\2\2\u05fd\u05ff\3\2\2\2\u05fe\u05f0\3\2\2\2\u05fe"+
		"\u05f2\3\2\2\2\u05ff]\3\2\2\2\u0600\u0601\7f\2\2\u0601\u0617\7\3\2\2\u0602"+
		"\u0603\t\r\2\2\u0603\u0618\7\u0089\2\2\u0604\u0605\5x=\2\u0605\u0606\7"+
		"A\2\2\u0606\u0618\3\2\2\2\u0607\u0618\7\u00ea\2\2\u0608\u0609\7\u008a"+
		"\2\2\u0609\u060a\7\u00eb\2\2\u060a\u060b\7\u008b\2\2\u060b\u060c\7\u008c"+
		"\2\2\u060c\u0615\7\u00eb\2\2\u060d\u0613\7;\2\2\u060e\u0614\5\u00aeX\2"+
		"\u060f\u0610\5\u00acW\2\u0610\u0611\7\3\2\2\u0611\u0612\7\4\2\2\u0612"+
		"\u0614\3\2\2\2\u0613\u060e\3\2\2\2\u0613\u060f\3\2\2\2\u0614\u0616\3\2"+
		"\2\2\u0615\u060d\3\2\2\2\u0615\u0616\3\2\2\2\u0616\u0618\3\2\2\2\u0617"+
		"\u0602\3\2\2\2\u0617\u0604\3\2\2\2\u0617\u0607\3\2\2\2\u0617\u0608\3\2"+
		"\2\2\u0618\u0619\3\2\2\2\u0619\u061a\7\4\2\2\u061a_\3\2\2\2\u061b\u061c"+
		"\7\3\2\2\u061c\u061d\5b\62\2\u061d\u061e\7\4\2\2\u061ea\3\2\2\2\u061f"+
		"\u0624\5\u00aeX\2\u0620\u0621\7\5\2\2\u0621\u0623\5\u00aeX\2\u0622\u0620"+
		"\3\2\2\2\u0623\u0626\3\2\2\2\u0624\u0622\3\2\2\2\u0624\u0625\3\2\2\2\u0625"+
		"c\3\2\2\2\u0626\u0624\3\2\2\2\u0627\u0628\7\3\2\2\u0628\u062d\5f\64\2"+
		"\u0629\u062a\7\5\2\2\u062a\u062c\5f\64\2\u062b\u0629\3\2\2\2\u062c\u062f"+
		"\3\2\2\2\u062d\u062b\3\2\2\2\u062d\u062e\3\2\2\2\u062e\u0630\3\2\2\2\u062f"+
		"\u062d\3\2\2\2\u0630\u0631\7\4\2\2\u0631e\3\2\2\2\u0632\u0634\5\u00ae"+
		"X\2\u0633\u0635\t\n\2\2\u0634\u0633\3\2\2\2\u0634\u0635\3\2\2\2\u0635"+
		"g\3\2\2\2\u0636\u0637\7\3\2\2\u0637\u063c\5j\66\2\u0638\u0639\7\5\2\2"+
		"\u0639\u063b\5j\66\2\u063a\u0638\3\2\2\2\u063b\u063e\3\2\2\2\u063c\u063a"+
		"\3\2\2\2\u063c\u063d\3\2\2\2\u063d\u063f\3\2\2\2\u063e\u063c\3\2\2\2\u063f"+
		"\u0640\7\4\2\2\u0640i\3\2\2\2\u0641\u0644\5\u00aeX\2\u0642\u0643\7m\2"+
		"\2\u0643\u0645\7\u00e6\2\2\u0644\u0642\3\2\2\2\u0644\u0645\3\2\2\2\u0645"+
		"k\3\2\2\2\u0646\u0648\5r:\2\u0647\u0649\5^\60\2\u0648\u0647\3\2\2\2\u0648"+
		"\u0649\3\2\2\2\u0649\u064e\3\2\2\2\u064a\u064c\7\r\2\2\u064b\u064a\3\2"+
		"\2\2\u064b\u064c\3\2\2\2\u064c\u064d\3\2\2\2\u064d\u064f\5\u00b0Y\2\u064e"+
		"\u064b\3\2\2\2\u064e\u064f\3\2\2\2\u064f\u0678\3\2\2\2\u0650\u0651\7\3"+
		"\2\2\u0651\u0652\5> \2\u0652\u0654\7\4\2\2\u0653\u0655\5^\60\2\u0654\u0653"+
		"\3\2\2\2\u0654\u0655\3\2\2\2\u0655\u065a\3\2\2\2\u0656\u0658\7\r\2\2\u0657"+
		"\u0656\3\2\2\2\u0657\u0658\3\2\2\2\u0658\u0659\3\2\2\2\u0659\u065b\5\u00b0"+
		"Y\2\u065a\u0657\3\2\2\2\u065a\u065b\3\2\2\2\u065b\u0678\3\2\2\2\u065c"+
		"\u065d\7\3\2\2\u065d\u065e\5V,\2\u065e\u0660\7\4\2\2\u065f\u0661\5^\60"+
		"\2\u0660\u065f\3\2\2\2\u0660\u0661\3\2\2\2\u0661\u0666\3\2\2\2\u0662\u0664"+
		"\7\r\2\2\u0663\u0662\3\2\2\2\u0663\u0664\3\2\2\2\u0664\u0665\3\2\2\2\u0665"+
		"\u0667\5\u00b0Y\2\u0666\u0663\3\2\2\2\u0666\u0667\3\2\2\2\u0667\u0678"+
		"\3\2\2\2\u0668\u0678\5n8\2\u0669\u066a\5\u00aeX\2\u066a\u0673\7\3\2\2"+
		"\u066b\u0670\5x=\2\u066c\u066d\7\5\2\2\u066d\u066f\5x=\2\u066e\u066c\3"+
		"\2\2\2\u066f\u0672\3\2\2\2\u0670\u066e\3\2\2\2\u0670\u0671\3\2\2\2\u0671"+
		"\u0674\3\2\2\2\u0672\u0670\3\2\2\2\u0673\u066b\3\2\2\2\u0673\u0674\3\2"+
		"\2\2\u0674\u0675\3\2\2\2\u0675\u0676\7\4\2\2\u0676\u0678\3\2\2\2\u0677"+
		"\u0646\3\2\2\2\u0677\u0650\3\2\2\2\u0677\u065c\3\2\2\2\u0677\u0668\3\2"+
		"\2\2\u0677\u0669\3\2\2\2\u0678m\3\2\2\2\u0679\u067a\7K\2\2\u067a\u067f"+
		"\5x=\2\u067b\u067c\7\5\2\2\u067c\u067e\5x=\2\u067d\u067b\3\2\2\2\u067e"+
		"\u0681\3\2\2\2\u067f\u067d\3\2\2\2\u067f\u0680\3\2\2\2\u0680\u0689\3\2"+
		"\2\2\u0681\u067f\3\2\2\2\u0682\u0684\7\r\2\2\u0683\u0682\3\2\2\2\u0683"+
		"\u0684\3\2\2\2\u0684\u0685\3\2\2\2\u0685\u0687\5\u00aeX\2\u0686\u0688"+
		"\5`\61\2\u0687\u0686\3\2\2\2\u0687\u0688\3\2\2\2\u0688\u068a\3\2\2\2\u0689"+
		"\u0683\3\2\2\2\u0689\u068a\3\2\2\2\u068ao\3\2\2\2\u068b\u068c\7I\2\2\u068c"+
		"\u068d\7U\2\2\u068d\u068e\7\u0094\2\2\u068e\u0692\7\u00e6\2\2\u068f\u0690"+
		"\7J\2\2\u0690\u0691\7\u0095\2\2\u0691\u0693\5*\26\2\u0692\u068f\3\2\2"+
		"\2\u0692\u0693\3\2\2\2\u0693\u06bd\3\2\2\2\u0694\u0695\7I\2\2\u0695\u0696"+
		"\7U\2\2\u0696\u06a0\7\u0098\2\2\u0697\u0698\7\u0099\2\2\u0698\u0699\7"+
		"\u009a\2\2\u0699\u069a\7\22\2\2\u069a\u069e\7\u00e6\2\2\u069b\u069c\7"+
		"\u009e\2\2\u069c\u069d\7\22\2\2\u069d\u069f\7\u00e6\2\2\u069e\u069b\3"+
		"\2\2\2\u069e\u069f\3\2\2\2\u069f\u06a1\3\2\2\2\u06a0\u0697\3\2\2\2\u06a0"+
		"\u06a1\3\2\2\2\u06a1\u06a7\3\2\2\2\u06a2\u06a3\7\u009b\2\2\u06a3\u06a4"+
		"\7\u009c\2\2\u06a4\u06a5\7\u009a\2\2\u06a5\u06a6\7\22\2\2\u06a6\u06a8"+
		"\7\u00e6\2\2\u06a7\u06a2\3\2\2\2\u06a7\u06a8\3\2\2\2\u06a8\u06ae\3\2\2"+
		"\2\u06a9\u06aa\7k\2\2\u06aa\u06ab\7\u009d\2\2\u06ab\u06ac\7\u009a\2\2"+
		"\u06ac\u06ad\7\22\2\2\u06ad\u06af\7\u00e6\2\2\u06ae\u06a9\3\2\2\2\u06ae"+
		"\u06af\3\2\2\2\u06af\u06b4\3\2\2\2\u06b0\u06b1\7\u009f\2\2\u06b1\u06b2"+
		"\7\u009a\2\2\u06b2\u06b3\7\22\2\2\u06b3\u06b5\7\u00e6\2\2\u06b4\u06b0"+
		"\3\2\2\2\u06b4\u06b5\3\2\2\2\u06b5\u06ba\3\2\2\2\u06b6\u06b7\7%\2\2\u06b7"+
		"\u06b8\7\u00cd\2\2\u06b8\u06b9\7\r\2\2\u06b9\u06bb\7\u00e6\2\2\u06ba\u06b6"+
		"\3\2\2\2\u06ba\u06bb\3\2\2\2\u06bb\u06bd\3\2\2\2\u06bc\u068b\3\2\2\2\u06bc"+
		"\u0694\3\2\2\2\u06bdq\3\2\2\2\u06be\u06bf\5\u00aeX\2\u06bf\u06c0\7\6\2"+
		"\2\u06c0\u06c2\3\2\2\2\u06c1\u06be\3\2\2\2\u06c1\u06c2\3\2\2\2\u06c2\u06c3"+
		"\3\2\2\2\u06c3\u06c4\5\u00aeX\2\u06c4s\3\2\2\2\u06c5\u06cd\5x=\2\u06c6"+
		"\u06c8\7\r\2\2\u06c7\u06c6\3\2\2\2\u06c7\u06c8\3\2\2\2\u06c8\u06cb\3\2"+
		"\2\2\u06c9\u06cc\5\u00aeX\2\u06ca\u06cc\5`\61\2\u06cb\u06c9\3\2\2\2\u06cb"+
		"\u06ca\3\2\2\2\u06cc\u06ce\3\2\2\2\u06cd\u06c7\3\2\2\2\u06cd\u06ce\3\2"+
		"\2\2\u06ceu\3\2\2\2\u06cf\u06d4\5t;\2\u06d0\u06d1\7\5\2\2\u06d1\u06d3"+
		"\5t;\2\u06d2\u06d0\3\2\2\2\u06d3\u06d6\3\2\2\2\u06d4\u06d2\3\2\2\2\u06d4"+
		"\u06d5\3\2\2\2\u06d5w\3\2\2\2\u06d6\u06d4\3\2\2\2\u06d7\u06d8\5z>\2\u06d8"+
		"y\3\2\2\2\u06d9\u06da\b>\1\2\u06da\u06db\7\36\2\2\u06db\u06e3\5z>\7\u06dc"+
		"\u06e3\5|?\2\u06dd\u06de\7 \2\2\u06de\u06df\7\3\2\2\u06df\u06e0\5\26\f"+
		"\2\u06e0\u06e1\7\4\2\2\u06e1\u06e3\3\2\2\2\u06e2\u06d9\3\2\2\2\u06e2\u06dc"+
		"\3\2\2\2\u06e2\u06dd\3\2\2\2\u06e3\u06ec\3\2\2\2\u06e4\u06e5\f\5\2\2\u06e5"+
		"\u06e6\7\34\2\2\u06e6\u06eb\5z>\6\u06e7\u06e8\f\4\2\2\u06e8\u06e9\7\33"+
		"\2\2\u06e9\u06eb\5z>\5\u06ea\u06e4\3\2\2\2\u06ea\u06e7\3\2\2\2\u06eb\u06ee"+
		"\3\2\2\2\u06ec\u06ea\3\2\2\2\u06ec\u06ed\3\2\2\2\u06ed{\3\2\2\2\u06ee"+
		"\u06ec\3\2\2\2\u06ef\u06f1\5\u0080A\2\u06f0\u06f2\5~@\2\u06f1\u06f0\3"+
		"\2\2\2\u06f1\u06f2\3\2\2\2\u06f2}\3\2\2\2\u06f3\u06f5\7\36\2\2\u06f4\u06f3"+
		"\3\2\2\2\u06f4\u06f5\3\2\2\2\u06f5\u06f6\3\2\2\2\u06f6\u06f7\7!\2\2\u06f7"+
		"\u06f8\5\u0080A\2\u06f8\u06f9\7\34\2\2\u06f9\u06fa\5\u0080A\2\u06fa\u071d"+
		"\3\2\2\2\u06fb\u06fd\7\36\2\2\u06fc\u06fb\3\2\2\2\u06fc\u06fd\3\2\2\2"+
		"\u06fd\u06fe\3\2\2\2\u06fe\u06ff\7\35\2\2\u06ff\u0700\7\3\2\2\u0700\u0705"+
		"\5x=\2\u0701\u0702\7\5\2\2\u0702\u0704\5x=\2\u0703\u0701\3\2\2\2\u0704"+
		"\u0707\3\2\2\2\u0705\u0703\3\2\2\2\u0705\u0706\3\2\2\2\u0706\u0708\3\2"+
		"\2\2\u0707\u0705\3\2\2\2\u0708\u0709\7\4\2\2\u0709\u071d\3\2\2\2\u070a"+
		"\u070c\7\36\2\2\u070b\u070a\3\2\2\2\u070b\u070c\3\2\2\2\u070c\u070d\3"+
		"\2\2\2\u070d\u070e\7\35\2\2\u070e\u070f\7\3\2\2\u070f\u0710\5\26\f\2\u0710"+
		"\u0711\7\4\2\2\u0711\u071d\3\2\2\2\u0712\u0714\7\36\2\2\u0713\u0712\3"+
		"\2\2\2\u0713\u0714\3\2\2\2\u0714\u0715\3\2\2\2\u0715\u0716\t\16\2\2\u0716"+
		"\u071d\5\u0080A\2\u0717\u0719\7$\2\2\u0718\u071a\7\36\2\2\u0719\u0718"+
		"\3\2\2\2\u0719\u071a\3\2\2\2\u071a\u071b\3\2\2\2\u071b\u071d\7%\2\2\u071c"+
		"\u06f4\3\2\2\2\u071c\u06fc\3\2\2\2\u071c\u070b\3\2\2\2\u071c\u0713\3\2"+
		"\2\2\u071c\u0717\3\2\2\2\u071d\177\3\2\2\2\u071e\u071f\bA\1\2\u071f\u0723"+
		"\5\u0082B\2\u0720\u0721\t\17\2\2\u0721\u0723\5\u0080A\t\u0722\u071e\3"+
		"\2\2\2\u0722\u0720\3\2\2\2\u0723\u0739\3\2\2\2\u0724\u0725\f\b\2\2\u0725"+
		"\u0726\t\20\2\2\u0726\u0738\5\u0080A\t\u0727\u0728\f\7\2\2\u0728\u0729"+
		"\t\21\2\2\u0729\u0738\5\u0080A\b\u072a\u072b\f\6\2\2\u072b\u072c\7\u0086"+
		"\2\2\u072c\u0738\5\u0080A\7\u072d\u072e\f\5\2\2\u072e\u072f\7\u0088\2"+
		"\2\u072f\u0738\5\u0080A\6\u0730\u0731\f\4\2\2\u0731\u0732\7\u0087\2\2"+
		"\u0732\u0738\5\u0080A\5\u0733\u0734\f\3\2\2\u0734\u0735\5\u0086D\2\u0735"+
		"\u0736\5\u0080A\4\u0736\u0738\3\2\2\2\u0737\u0724\3\2\2\2\u0737\u0727"+
		"\3\2\2\2\u0737\u072a\3\2\2\2\u0737\u072d\3\2\2\2\u0737\u0730\3\2\2\2\u0737"+
		"\u0733\3\2\2\2\u0738\u073b\3\2\2\2\u0739\u0737\3\2\2\2\u0739\u073a\3\2"+
		"\2\2\u073a\u0081\3\2\2\2\u073b\u0739\3\2\2\2\u073c\u073d\bB\1\2\u073d"+
		"\u078c\t\22\2\2\u073e\u0740\7-\2\2\u073f\u0741\5\u00a0Q\2\u0740\u073f"+
		"\3\2\2\2\u0741\u0742\3\2\2\2\u0742\u0740\3\2\2\2\u0742\u0743\3\2\2\2\u0743"+
		"\u0746\3\2\2\2\u0744\u0745\7\60\2\2\u0745\u0747\5x=\2\u0746\u0744\3\2"+
		"\2\2\u0746\u0747\3\2\2\2\u0747\u0748\3\2\2\2\u0748\u0749\7\61\2\2\u0749"+
		"\u078c\3\2\2\2\u074a\u074b\7-\2\2\u074b\u074d\5x=\2\u074c\u074e\5\u00a0"+
		"Q\2\u074d\u074c\3\2\2\2\u074e\u074f\3\2\2\2\u074f\u074d\3\2\2\2\u074f"+
		"\u0750\3\2\2\2\u0750\u0753\3\2\2\2\u0751\u0752\7\60\2\2\u0752\u0754\5"+
		"x=\2\u0753\u0751\3\2\2\2\u0753\u0754\3\2\2\2\u0754\u0755\3\2\2\2\u0755"+
		"\u0756\7\61\2\2\u0756\u078c\3\2\2\2\u0757\u0758\7X\2\2\u0758\u0759\7\3"+
		"\2\2\u0759\u075a\5x=\2\u075a\u075b\7\r\2\2\u075b\u075c\5\u0096L\2\u075c"+
		"\u075d\7\4\2\2\u075d\u078c\3\2\2\2\u075e\u078c\5\u0084C\2\u075f\u078c"+
		"\7\u0081\2\2\u0760\u0761\5\u00acW\2\u0761\u0762\7\6\2\2\u0762\u0763\7"+
		"\u0081\2\2\u0763\u078c\3\2\2\2\u0764\u0765\7\3\2\2\u0765\u0768\5x=\2\u0766"+
		"\u0767\7\5\2\2\u0767\u0769\5x=\2\u0768\u0766\3\2\2\2\u0769\u076a\3\2\2"+
		"\2\u076a\u0768\3\2\2\2\u076a\u076b\3\2\2\2\u076b\u076c\3\2\2\2\u076c\u076d"+
		"\7\4\2\2\u076d\u078c\3\2\2\2\u076e\u076f\7\3\2\2\u076f\u0770\5\26\f\2"+
		"\u0770\u0771\7\4\2\2\u0771\u078c\3\2\2\2\u0772\u0773\5\u00acW\2\u0773"+
		"\u077f\7\3\2\2\u0774\u0776\5T+\2\u0775\u0774\3\2\2\2\u0775\u0776\3\2\2"+
		"\2\u0776\u0777\3\2\2\2\u0777\u077c\5x=\2\u0778\u0779\7\5\2\2\u0779\u077b"+
		"\5x=\2\u077a\u0778\3\2\2\2\u077b\u077e\3\2\2\2\u077c\u077a\3\2\2\2\u077c"+
		"\u077d\3\2\2\2\u077d\u0780\3\2\2\2\u077e\u077c\3\2\2\2\u077f\u0775\3\2"+
		"\2\2\u077f\u0780\3\2\2\2\u0780\u0781\3\2\2\2\u0781\u0784\7\4\2\2\u0782"+
		"\u0783\7>\2\2\u0783\u0785\5\u00a6T\2\u0784\u0782\3\2\2\2\u0784\u0785\3"+
		"\2\2\2\u0785\u078c\3\2\2\2\u0786\u078c\5\u00aeX\2\u0787\u0788\7\3\2\2"+
		"\u0788\u0789\5x=\2\u0789\u078a\7\4\2\2\u078a\u078c\3\2\2\2\u078b\u073c"+
		"\3\2\2\2\u078b\u073e\3\2\2\2\u078b\u074a\3\2\2\2\u078b\u0757\3\2\2\2\u078b"+
		"\u075e\3\2\2\2\u078b\u075f\3\2\2\2\u078b\u0760\3\2\2\2\u078b\u0764\3\2"+
		"\2\2\u078b\u076e\3\2\2\2\u078b\u0772\3\2\2\2\u078b\u0786\3\2\2\2\u078b"+
		"\u0787\3\2\2\2\u078c\u0797\3\2\2\2\u078d\u078e\f\6\2\2\u078e\u078f\7\7"+
		"\2\2\u078f\u0790\5\u0080A\2\u0790\u0791\7\b\2\2\u0791\u0796\3\2\2\2\u0792"+
		"\u0793\f\4\2\2\u0793\u0794\7\6\2\2\u0794\u0796\5\u00aeX\2\u0795\u078d"+
		"\3\2\2\2\u0795\u0792\3\2\2\2\u0796\u0799\3\2\2\2\u0797\u0795\3\2\2\2\u0797"+
		"\u0798\3\2\2\2\u0798\u0083\3\2\2\2\u0799\u0797\3\2\2\2\u079a\u07a7\7%"+
		"\2\2\u079b\u07a7\5\u008eH\2\u079c\u079d\5\u00aeX\2\u079d\u079e\7\u00e6"+
		"\2\2\u079e\u07a7\3\2\2\2\u079f\u07a7\5\u00b4[\2\u07a0\u07a7\5\u008cG\2"+
		"\u07a1\u07a3\7\u00e6\2\2\u07a2\u07a1\3\2\2\2\u07a3\u07a4\3\2\2\2\u07a4"+
		"\u07a2\3\2\2\2\u07a4\u07a5\3\2\2\2\u07a5\u07a7\3\2\2\2\u07a6\u079a\3\2"+
		"\2\2\u07a6\u079b\3\2\2\2\u07a6\u079c\3\2\2\2\u07a6\u079f\3\2\2\2\u07a6"+
		"\u07a0\3\2\2\2\u07a6\u07a2\3\2\2\2\u07a7\u0085\3\2\2\2\u07a8\u07a9\t\23"+
		"\2\2\u07a9\u0087\3\2\2\2\u07aa\u07ab\t\24\2\2\u07ab\u0089\3\2\2\2\u07ac"+
		"\u07ad\t\25\2\2\u07ad\u008b\3\2\2\2\u07ae\u07af\t\26\2\2\u07af\u008d\3"+
		"\2\2\2\u07b0\u07b4\7,\2\2\u07b1\u07b3\5\u0090I\2\u07b2\u07b1\3\2\2\2\u07b3"+
		"\u07b6\3\2\2\2\u07b4\u07b2\3\2\2\2\u07b4\u07b5\3\2\2\2\u07b5\u008f\3\2"+
		"\2\2\u07b6\u07b4\3\2\2\2\u07b7\u07b8\5\u0092J\2\u07b8\u07bb\5\u00aeX\2"+
		"\u07b9\u07ba\7e\2\2\u07ba\u07bc\5\u00aeX\2\u07bb\u07b9\3\2\2\2\u07bb\u07bc"+
		"\3\2\2\2\u07bc\u0091\3\2\2\2\u07bd\u07bf\t\21\2\2\u07be\u07bd\3\2\2\2"+
		"\u07be\u07bf\3\2\2\2\u07bf\u07c0\3\2\2\2\u07c0\u07c3\t\r\2\2\u07c1\u07c3"+
		"\7\u00e6\2\2\u07c2\u07be\3\2\2\2\u07c2\u07c1\3\2\2\2\u07c3\u0093\3\2\2"+
		"\2\u07c4\u07c8\7F\2\2\u07c5\u07c6\7G\2\2\u07c6\u07c8\5\u00aeX\2\u07c7"+
		"\u07c4\3\2\2\2\u07c7\u07c5\3\2\2\2\u07c8\u0095\3\2\2\2\u07c9\u07ca\7j"+
		"\2\2\u07ca\u07cb\7{\2\2\u07cb\u07cc\5\u0096L\2\u07cc\u07cd\7}\2\2\u07cd"+
		"\u07ec\3\2\2\2\u07ce\u07cf\7k\2\2\u07cf\u07d0\7{\2\2\u07d0\u07d1\5\u0096"+
		"L\2\u07d1\u07d2\7\5\2\2\u07d2\u07d3\5\u0096L\2\u07d3\u07d4\7}\2\2\u07d4"+
		"\u07ec\3\2\2\2\u07d5\u07dc\7l\2\2\u07d6\u07d8\7{\2\2\u07d7\u07d9\5\u009c"+
		"O\2\u07d8\u07d7\3\2\2\2\u07d8\u07d9\3\2\2\2\u07d9\u07da\3\2\2\2\u07da"+
		"\u07dd\7}\2\2\u07db\u07dd\7y\2\2\u07dc\u07d6\3\2\2\2\u07dc\u07db\3\2\2"+
		"\2\u07dd\u07ec\3\2\2\2\u07de\u07e9\5\u00aeX\2\u07df\u07e0\7\3\2\2\u07e0"+
		"\u07e5\7\u00eb\2\2\u07e1\u07e2\7\5\2\2\u07e2\u07e4\7\u00eb\2\2\u07e3\u07e1"+
		"\3\2\2\2\u07e4\u07e7\3\2\2\2\u07e5\u07e3\3\2\2\2\u07e5\u07e6\3\2\2\2\u07e6"+
		"\u07e8\3\2\2\2\u07e7\u07e5\3\2\2\2\u07e8\u07ea\7\4\2\2\u07e9\u07df\3\2"+
		"\2\2\u07e9\u07ea\3\2\2\2\u07ea\u07ec\3\2\2\2\u07eb\u07c9\3\2\2\2\u07eb"+
		"\u07ce\3\2\2\2\u07eb\u07d5\3\2\2\2\u07eb\u07de\3\2\2\2\u07ec\u0097\3\2"+
		"\2\2\u07ed\u07f2\5\u009aN\2\u07ee\u07ef\7\5\2\2\u07ef\u07f1\5\u009aN\2"+
		"\u07f0\u07ee\3\2\2\2\u07f1\u07f4\3\2\2\2\u07f2\u07f0\3\2\2\2\u07f2\u07f3"+
		"\3\2\2\2\u07f3\u0099\3\2\2\2\u07f4\u07f2\3\2\2\2\u07f5\u07f6\5\u00aeX"+
		"\2\u07f6\u07f9\5\u0096L\2\u07f7\u07f8\7m\2\2\u07f8\u07fa\7\u00e6\2\2\u07f9"+
		"\u07f7\3\2\2\2\u07f9\u07fa\3\2\2\2\u07fa\u009b\3\2\2\2\u07fb\u0800\5\u009e"+
		"P\2\u07fc\u07fd\7\5\2\2\u07fd\u07ff\5\u009eP\2\u07fe\u07fc\3\2\2\2\u07ff"+
		"\u0802\3\2\2\2\u0800\u07fe\3\2\2\2\u0800\u0801\3\2\2\2\u0801\u009d\3\2"+
		"\2\2\u0802\u0800\3\2\2\2\u0803\u0804\5\u00aeX\2\u0804\u0805\7\t\2\2\u0805"+
		"\u0808\5\u0096L\2\u0806\u0807\7m\2\2\u0807\u0809\7\u00e6\2\2\u0808\u0806"+
		"\3\2\2\2\u0808\u0809\3\2\2\2\u0809\u009f\3\2\2\2\u080a\u080b\7.\2\2\u080b"+
		"\u080c\5x=\2\u080c\u080d\7/\2\2\u080d\u080e\5x=\2\u080e\u00a1\3\2\2\2"+
		"\u080f\u0810\7=\2\2\u0810\u0815\5\u00a4S\2\u0811\u0812\7\5\2\2\u0812\u0814"+
		"\5\u00a4S\2\u0813\u0811\3\2\2\2\u0814\u0817\3\2\2\2\u0815\u0813\3\2\2"+
		"\2\u0815\u0816\3\2\2\2\u0816\u00a3\3\2\2\2\u0817\u0815\3\2\2\2\u0818\u0819"+
		"\5\u00aeX\2\u0819\u081a\7\r\2\2\u081a\u081b\5\u00a6T\2\u081b\u00a5\3\2"+
		"\2\2\u081c\u0847\5\u00aeX\2\u081d\u0840\7\3\2\2\u081e\u081f\7\u008e\2"+
		"\2\u081f\u0820\7\22\2\2\u0820\u0825\5x=\2\u0821\u0822\7\5\2\2\u0822\u0824"+
		"\5x=\2\u0823\u0821\3\2\2\2\u0824\u0827\3\2\2\2\u0825\u0823\3\2\2\2\u0825"+
		"\u0826\3\2\2\2\u0826\u0841\3\2\2\2\u0827\u0825\3\2\2\2\u0828\u0829\t\27"+
		"\2\2\u0829\u082a\7\22\2\2\u082a\u082f\5x=\2\u082b\u082c\7\5\2\2\u082c"+
		"\u082e\5x=\2\u082d\u082b\3\2\2\2\u082e\u0831\3\2\2\2\u082f\u082d\3\2\2"+
		"\2\u082f\u0830\3\2\2\2\u0830\u0833\3\2\2\2\u0831\u082f\3\2\2\2\u0832\u0828"+
		"\3\2\2\2\u0832\u0833\3\2\2\2\u0833\u083e\3\2\2\2\u0834\u0835\t\30\2\2"+
		"\u0835\u0836\7\22\2\2\u0836\u083b\5H%\2\u0837\u0838\7\5\2\2\u0838\u083a"+
		"\5H%\2\u0839\u0837\3\2\2\2\u083a\u083d\3\2\2\2\u083b\u0839\3\2\2\2\u083b"+
		"\u083c\3\2\2\2\u083c\u083f\3\2\2\2\u083d\u083b\3\2\2\2\u083e\u0834\3\2"+
		"\2\2\u083e\u083f\3\2\2\2\u083f\u0841\3\2\2\2\u0840\u081e\3\2\2\2\u0840"+
		"\u0832\3\2\2\2\u0841\u0843\3\2\2\2\u0842\u0844\5\u00a8U\2\u0843\u0842"+
		"\3\2\2\2\u0843\u0844\3\2\2\2\u0844\u0845\3\2\2\2\u0845\u0847\7\4\2\2\u0846"+
		"\u081c\3\2\2\2\u0846\u081d\3\2\2\2\u0847\u00a7\3\2\2\2\u0848\u0849\7@"+
		"\2\2\u0849\u0859\5\u00aaV\2\u084a\u084b\7A\2\2\u084b\u0859\5\u00aaV\2"+
		"\u084c\u084d\7@\2\2\u084d\u084e\7!\2\2\u084e\u084f\5\u00aaV\2\u084f\u0850"+
		"\7\34\2\2\u0850\u0851\5\u00aaV\2\u0851\u0859\3\2\2\2\u0852\u0853\7A\2"+
		"\2\u0853\u0854\7!\2\2\u0854\u0855\5\u00aaV\2\u0855\u0856\7\34\2\2\u0856"+
		"\u0857\5\u00aaV\2\u0857\u0859\3\2\2\2\u0858\u0848\3\2\2\2\u0858\u084a"+
		"\3\2\2\2\u0858\u084c\3\2\2\2\u0858\u0852\3\2\2\2\u0859\u00a9\3\2\2\2\u085a"+
		"\u085b\7B\2\2\u085b\u0862\t\31\2\2\u085c\u085d\7E\2\2\u085d\u0862\7I\2"+
		"\2\u085e\u085f\5x=\2\u085f\u0860\t\31\2\2\u0860\u0862\3\2\2\2\u0861\u085a"+
		"\3\2\2\2\u0861\u085c\3\2\2\2\u0861\u085e\3\2\2\2\u0862\u00ab\3\2\2\2\u0863"+
		"\u0868\5\u00aeX\2\u0864\u0865\7\6\2\2\u0865\u0867\5\u00aeX\2\u0866\u0864"+
		"\3\2\2\2\u0867\u086a\3\2\2\2\u0868\u0866\3\2\2\2\u0868\u0869\3\2\2\2\u0869"+
		"\u00ad\3\2\2\2\u086a\u0868\3\2\2\2\u086b\u087b\5\u00b0Y\2\u086c\u087b"+
		"\7\u00e1\2\2\u086d\u087b\79\2\2\u086e\u087b\7\65\2\2\u086f\u087b\7\66"+
		"\2\2\u0870\u087b\7\67\2\2\u0871\u087b\78\2\2\u0872\u087b\7:\2\2\u0873"+
		"\u087b\7\62\2\2\u0874\u087b\7\63\2\2\u0875\u087b\7;\2\2\u0876\u087b\7"+
		"a\2\2\u0877\u087b\7d\2\2\u0878\u087b\7b\2\2\u0879\u087b\7c\2\2\u087a\u086b"+
		"\3\2\2\2\u087a\u086c\3\2\2\2\u087a\u086d\3\2\2\2\u087a\u086e\3\2\2\2\u087a"+
		"\u086f\3\2\2\2\u087a\u0870\3\2\2\2\u087a\u0871\3\2\2\2\u087a\u0872\3\2"+
		"\2\2\u087a\u0873\3\2\2\2\u087a\u0874\3\2\2\2\u087a\u0875\3\2\2\2\u087a"+
		"\u0876\3\2\2\2\u087a\u0877\3\2\2\2\u087a\u0878\3\2\2\2\u087a\u0879\3\2"+
		"\2\2\u087b\u00af\3\2\2\2\u087c\u0880\7\u00ef\2\2\u087d\u0880\5\u00b2Z"+
		"\2\u087e\u0880\5\u00b6\\\2\u087f\u087c\3\2\2\2\u087f\u087d\3\2\2\2\u087f"+
		"\u087e\3\2\2\2\u0880\u00b1\3\2\2\2\u0881\u0882\7\u00f0\2\2\u0882\u00b3"+
		"\3\2\2\2\u0883\u0885\7\u0080\2\2\u0884\u0883\3\2\2\2\u0884\u0885\3\2\2"+
		"\2\u0885\u0886\3\2\2\2\u0886\u08a0\7\u00ec\2\2\u0887\u0889\7\u0080\2\2"+
		"\u0888\u0887\3\2\2\2\u0888\u0889\3\2\2\2\u0889\u088a\3\2\2\2\u088a\u08a0"+
		"\7\u00eb\2\2\u088b\u088d\7\u0080\2\2\u088c\u088b\3\2\2\2\u088c\u088d\3"+
		"\2\2\2\u088d\u088e\3\2\2\2\u088e\u08a0\7\u00e7\2\2\u088f\u0891\7\u0080"+
		"\2\2\u0890\u088f\3\2\2\2\u0890\u0891\3\2\2\2\u0891\u0892\3\2\2\2\u0892"+
		"\u08a0\7\u00e8\2\2\u0893\u0895\7\u0080\2\2\u0894\u0893\3\2\2\2\u0894\u0895"+
		"\3\2\2\2\u0895\u0896\3\2\2\2\u0896\u08a0\7\u00e9\2\2\u0897\u0899\7\u0080"+
		"\2\2\u0898\u0897\3\2\2\2\u0898\u0899\3\2\2\2\u0899\u089a\3\2\2\2\u089a"+
		"\u08a0\7\u00ed\2\2\u089b\u089d\7\u0080\2\2\u089c\u089b\3\2\2\2\u089c\u089d"+
		"\3\2\2\2\u089d\u089e\3\2\2\2\u089e\u08a0\7\u00ee\2\2\u089f\u0884\3\2\2"+
		"\2\u089f\u0888\3\2\2\2\u089f\u088c\3\2\2\2\u089f\u0890\3\2\2\2\u089f\u0894"+
		"\3\2\2\2\u089f\u0898\3\2\2\2\u089f\u089c\3\2\2\2\u08a0\u00b5\3\2\2\2\u08a1"+
		"\u08a2\t\32\2\2\u08a2\u00b7\3\2\2\2\u0135\u00cc\u00d1\u00d4\u00d9\u00e6"+
		"\u00ea\u00f1\u00f6\u00fb\u00fe\u0101\u0105\u0108\u010b\u0112\u0116\u011e"+
		"\u0121\u0124\u0127\u012a\u012d\u0131\u0134\u0137\u013e\u0144\u014a\u0152"+
		"\u0169\u0171\u0175\u017a\u0180\u0188\u018e\u019b\u01a0\u01a9\u01ae\u01be"+
		"\u01c5\u01c9\u01d1\u01d8\u01df\u01ee\u01f2\u01f8\u01fe\u0201\u0204\u020a"+
		"\u020e\u0212\u0217\u021b\u0223\u0226\u022f\u0234\u023a\u0240\u024c\u024f"+
		"\u0253\u0258\u025d\u0264\u0267\u026a\u0271\u0276\u027c\u0285\u028d\u0293"+
		"\u0297\u029b\u029f\u02a1\u02aa\u02b0\u02b5\u02b8\u02bc\u02bf\u02c8\u02cd"+
		"\u02d2\u02d5\u02db\u02e3\u02e8\u02ee\u02f4\u02ff\u0307\u030e\u0316\u0319"+
		"\u0321\u0325\u032c\u03a0\u03a8\u03b0\u03b9\u03c2\u03ce\u03d2\u03d5\u03db"+
		"\u03e5\u03f1\u03f6\u03fc\u0408\u040a\u040f\u0413\u0415\u0419\u0422\u042a"+
		"\u0431\u0437\u043b\u0444\u0449\u0458\u045f\u0462\u0469\u046d\u0473\u047b"+
		"\u0486\u0491\u0498\u049e\u04a4\u04ad\u04af\u04b8\u04bb\u04c4\u04c7\u04d0"+
		"\u04d3\u04dc\u04df\u04e2\u04e6\u04e9\u04f4\u04f9\u0504\u0508\u050c\u0518"+
		"\u051b\u051f\u0529\u052d\u052f\u0532\u0536\u0539\u053d\u0541\u0545\u054a"+
		"\u054d\u054f\u0554\u0559\u055c\u0560\u0563\u0565\u056d\u0573\u057d\u058c"+
		"\u0591\u0599\u059c\u05a0\u05a5\u05ae\u05b1\u05b6\u05bd\u05c0\u05c8\u05cf"+
		"\u05d6\u05d9\u05de\u05e4\u05e8\u05eb\u05ee\u05f9\u05fe\u0613\u0615\u0617"+
		"\u0624\u062d\u0634\u063c\u0644\u0648\u064b\u064e\u0654\u0657\u065a\u0660"+
		"\u0663\u0666\u0670\u0673\u0677\u067f\u0683\u0687\u0689\u0692\u069e\u06a0"+
		"\u06a7\u06ae\u06b4\u06ba\u06bc\u06c1\u06c7\u06cb\u06cd\u06d4\u06e2\u06ea"+
		"\u06ec\u06f1\u06f4\u06fc\u0705\u070b\u0713\u0719\u071c\u0722\u0737\u0739"+
		"\u0742\u0746\u074f\u0753\u076a\u0775\u077c\u077f\u0784\u078b\u0795\u0797"+
		"\u07a4\u07a6\u07b4\u07bb\u07be\u07c2\u07c7\u07d8\u07dc\u07e5\u07e9\u07eb"+
		"\u07f2\u07f9\u0800\u0808\u0815\u0825\u082f\u0832\u083b\u083e\u0840\u0843"+
		"\u0846\u0858\u0861\u0868\u087a\u087f\u0884\u0888\u088c\u0890\u0894\u0898"+
		"\u089c\u089f";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}