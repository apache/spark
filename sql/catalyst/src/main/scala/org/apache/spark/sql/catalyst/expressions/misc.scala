/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.{SPARK_REVISION, SPARK_VERSION_SHORT}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedSeed
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.trees.TreePattern.{CURRENT_LIKE, SQL_KEYWORDS, TreePattern}
import org.apache.spark.sql.catalyst.util.RandomUUIDGenerator
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Print the result of an expression to stderr (used for debugging codegen).
 */
case class PrintToStderr(child: Expression) extends UnaryExpression {

  override def dataType: DataType = child.dataType

  protected override def nullSafeEval(input: Any): Any = {
    // scalastyle:off println
    System.err.println(outputPrefix + input)
    // scalastyle:on println
    input
  }

  private val outputPrefix = s"Result of ${child.simpleString(SQLConf.get.maxToStringFields)} is "

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val outputPrefixField = ctx.addReferenceObj("outputPrefix", outputPrefix)
    nullSafeCodeGen(ctx, ev, c =>
      s"""
         | System.err.println($outputPrefixField + $c);
         | ${ev.value} = $c;
       """.stripMargin)
  }

  override protected def withNewChildInternal(newChild: Expression): PrintToStderr =
    copy(child = newChild)
}

/**
 * Throw with the result of an expression (used for debugging).
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Throws an exception with `expr`.",
  examples = """
    Examples:
      > SELECT _FUNC_('custom error message');
       java.lang.RuntimeException
       custom error message
  """,
  since = "3.1.0",
  group = "misc_funcs")
case class RaiseError(child: Expression, dataType: DataType)
  extends UnaryExpression with ImplicitCastInputTypes {

  def this(child: Expression) = this(child, NullType)

  override def foldable: Boolean = false
  override def nullable: Boolean = true
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def prettyName: String = "raise_error"

  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      throw new RuntimeException()
    }
    throw new RuntimeException(value.toString)
  }

  // if (true) is to avoid codegen compilation exception that statement is unreachable
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    ExprCode(
      code = code"""${eval.code}
        |if (true) {
        |  if (${eval.isNull}) {
        |    throw new RuntimeException();
        |  }
        |  throw new RuntimeException(${eval.value}.toString());
        |}""".stripMargin,
      isNull = TrueLiteral,
      value = JavaCode.defaultLiteral(dataType)
    )
  }

  override protected def withNewChildInternal(newChild: Expression): RaiseError =
    copy(child = newChild)
}

object RaiseError {
  def apply(child: Expression): RaiseError = new RaiseError(child)
}

/**
 * A function that throws an exception if 'condition' is not true.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Throws an exception if `expr` is not true.",
  examples = """
    Examples:
      > SELECT _FUNC_(0 < 1);
       NULL
  """,
  since = "2.0.0",
  group = "misc_funcs")
case class AssertTrue(left: Expression, right: Expression, replacement: Expression)
  extends RuntimeReplaceable with InheritAnalysisRules {

  override def prettyName: String = "assert_true"

  def this(left: Expression, right: Expression) = {
    this(left, right, If(left, Literal(null), RaiseError(right)))
  }

  def this(left: Expression) = {
    this(left, Literal(s"'${left.simpleString(SQLConf.get.maxToStringFields)}' is not true!"))
  }

  override def parameters: Seq[Expression] = Seq(left, right)

  override protected def withNewChildInternal(newChild: Expression): AssertTrue =
    copy(replacement = newChild)
}

object AssertTrue {
  def apply(left: Expression): AssertTrue = new AssertTrue(left)
}

/**
 * Returns the current database of the SessionCatalog.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns the current database.",
  examples = """
    Examples:
      > SELECT _FUNC_();
       default
  """,
  since = "1.6.0",
  group = "misc_funcs")
case class CurrentDatabase() extends LeafExpression with Unevaluable {
  override def dataType: DataType = StringType
  override def nullable: Boolean = false
  override def prettyName: String = "current_database"
  final override val nodePatterns: Seq[TreePattern] = Seq(CURRENT_LIKE)
}

/**
 * Returns the current catalog.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns the current catalog.",
  examples = """
    Examples:
      > SELECT _FUNC_();
       spark_catalog
  """,
  since = "3.1.0",
  group = "misc_funcs")
case class CurrentCatalog() extends LeafExpression with Unevaluable {
  override def dataType: DataType = StringType
  override def nullable: Boolean = false
  override def prettyName: String = "current_catalog"
  final override val nodePatterns: Seq[TreePattern] = Seq(CURRENT_LIKE)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """_FUNC_() - Returns an universally unique identifier (UUID) string. The value is returned as a canonical UUID 36-character string.""",
  examples = """
    Examples:
      > SELECT _FUNC_();
       46707d92-02f4-4817-8116-a4c3b23e6266
  """,
  note = """
    The function is non-deterministic.
  """,
  since = "2.3.0",
  group = "misc_funcs")
// scalastyle:on line.size.limit
case class Uuid(randomSeed: Option[Long] = None) extends LeafExpression with Nondeterministic
    with ExpressionWithRandomSeed {

  def this() = this(None)

  override def seedExpression: Expression = randomSeed.map(Literal.apply).getOrElse(UnresolvedSeed)

  override def withNewSeed(seed: Long): Uuid = Uuid(Some(seed))

  override lazy val resolved: Boolean = randomSeed.isDefined

  override def nullable: Boolean = false

  override def dataType: DataType = StringType

  override def stateful: Boolean = true

  @transient private[this] var randomGenerator: RandomUUIDGenerator = _

  override protected def initializeInternal(partitionIndex: Int): Unit =
    randomGenerator = RandomUUIDGenerator(randomSeed.get + partitionIndex)

  override protected def evalInternal(input: InternalRow): Any =
    randomGenerator.getNextUUIDUTF8String()

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val randomGen = ctx.freshName("randomGen")
    ctx.addMutableState("org.apache.spark.sql.catalyst.util.RandomUUIDGenerator", randomGen,
      forceInline = true,
      useFreshName = false)
    ctx.addPartitionInitializationStatement(s"$randomGen = " +
      "new org.apache.spark.sql.catalyst.util.RandomUUIDGenerator(" +
      s"${randomSeed.get}L + partitionIndex);")
    ev.copy(code = code"final UTF8String ${ev.value} = $randomGen.getNextUUIDUTF8String();",
      isNull = FalseLiteral)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """_FUNC_() - Returns the Spark version. The string contains 2 fields, the first being a release version and the second being a git revision.""",
  examples = """
    Examples:
      > SELECT _FUNC_();
       3.1.0 a6d6ea3efedbad14d99c24143834cd4e2e52fb40
  """,
  since = "3.0.0",
  group = "misc_funcs")
// scalastyle:on line.size.limit
case class SparkVersion() extends LeafExpression with CodegenFallback {
  override def nullable: Boolean = false
  override def foldable: Boolean = true
  override def dataType: DataType = StringType
  override def prettyName: String = "version"
  override def eval(input: InternalRow): Any = {
    UTF8String.fromString(SPARK_VERSION_SHORT + " " + SPARK_REVISION)
  }
}

@ExpressionDescription(
  usage = """_FUNC_(expr) - Return DDL-formatted type string for the data type of the input.""",
  examples = """
    Examples:
      > SELECT _FUNC_(1);
       int
      > SELECT _FUNC_(array(1));
       array<int>
  """,
  since = "3.0.0",
  group = "misc_funcs")
case class TypeOf(child: Expression) extends UnaryExpression {
  override def nullable: Boolean = false
  override def foldable: Boolean = true
  override def dataType: DataType = StringType
  override def eval(input: InternalRow): Any = UTF8String.fromString(child.dataType.catalogString)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, _ => s"""UTF8String.fromString(${child.dataType.catalogString})""")
  }

  override protected def withNewChildInternal(newChild: Expression): TypeOf = copy(child = newChild)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """_FUNC_() - user name of current execution context.""",
  examples = """
    Examples:
      > SELECT _FUNC_();
       mockingjay
  """,
  since = "3.2.0",
  group = "misc_funcs")
// scalastyle:on line.size.limit
case class CurrentUser() extends LeafExpression with Unevaluable {
  override def nullable: Boolean = false
  override def dataType: DataType = StringType
  override def prettyName: String = "current_user"
  final override val nodePatterns: Seq[TreePattern] = Seq(CURRENT_LIKE)
}

/**
 * A function that encrypts input using AES. Key lengths of 128, 192 or 256 bits can be used.
 * For versions prior to JDK 8u161, 192 and 256 bits keys can be used
 * if Java Cryptography Extension (JCE) Unlimited Strength Jurisdiction Policy Files are installed.
 * If either argument is NULL or the key length is not one of the permitted values,
 * the return value is NULL.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, key[, mode[, padding]]) - Returns an encrypted value of `expr` using AES in given `mode` with the specified `padding`.
      Key lengths of 16, 24 and 32 bits are supported. Supported combinations of (`mode`, `padding`) are ('ECB', 'PKCS') and ('GCM', 'NONE').
      The default mode is GCM.
  """,
  arguments = """
    Arguments:
      * expr - The binary value to encrypt.
      * key - The passphrase to use to encrypt the data.
      * mode - Specifies which block cipher mode should be used to encrypt messages.
               Valid modes: ECB, GCM.
      * padding - Specifies how to pad messages whose length is not a multiple of the block size.
                  Valid values: PKCS, NONE, DEFAULT. The DEFAULT padding means PKCS for ECB and NONE for GCM.
  """,
  examples = """
    Examples:
      > SELECT hex(_FUNC_('Spark', '0000111122223333'));
       83F16B2AA704794132802D248E6BFD4E380078182D1544813898AC97E709B28A94
      > SELECT hex(_FUNC_('Spark SQL', '0000111122223333', 'GCM'));
       6E7CA17BBB468D3084B5744BCA729FB7B2B7BCB8E4472847D02670489D95FA97DBBA7D3210
      > SELECT base64(_FUNC_('Spark SQL', '1234567890abcdef', 'ECB', 'PKCS'));
       3lmwu+Mw0H3fi5NDvcu9lg==
  """,
  since = "3.3.0",
  group = "misc_funcs")
case class AesEncrypt(
    input: Expression,
    key: Expression,
    mode: Expression,
    padding: Expression)
  extends RuntimeReplaceable with ImplicitCastInputTypes {

  override lazy val replacement: Expression = StaticInvoke(
    classOf[ExpressionImplUtils],
    BinaryType,
    "aesEncrypt",
    Seq(input, key, mode, padding),
    inputTypes)

  def this(input: Expression, key: Expression, mode: Expression) =
    this(input, key, mode, Literal("DEFAULT"))
  def this(input: Expression, key: Expression) =
    this(input, key, Literal("GCM"))

  override def prettyName: String = "aes_encrypt"

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, BinaryType, StringType, StringType)

  override def children: Seq[Expression] = Seq(input, key, mode, padding)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    copy(newChildren(0), newChildren(1), newChildren(2), newChildren(3))
  }
}

/**
 * A function that decrypts input using AES. Key lengths of 128, 192 or 256 bits can be used.
 * For versions prior to JDK 8u161, 192 and 256 bits keys can be used
 * if Java Cryptography Extension (JCE) Unlimited Strength Jurisdiction Policy Files are installed.
 * If either argument is NULL or the key length is not one of the permitted values,
 * the return value is NULL.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr, key[, mode[, padding]]) - Returns a decrypted value of `expr` using AES in `mode` with `padding`.
      Key lengths of 16, 24 and 32 bits are supported. Supported combinations of (`mode`, `padding`) are ('ECB', 'PKCS') and ('GCM', 'NONE').
      The default mode is GCM.
  """,
  arguments = """
    Arguments:
      * expr - The binary value to decrypt.
      * key - The passphrase to use to decrypt the data.
      * mode - Specifies which block cipher mode should be used to decrypt messages.
               Valid modes: ECB, GCM.
      * padding - Specifies how to pad messages whose length is not a multiple of the block size.
                  Valid values: PKCS, NONE, DEFAULT. The DEFAULT padding means PKCS for ECB and NONE for GCM.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(unhex('83F16B2AA704794132802D248E6BFD4E380078182D1544813898AC97E709B28A94'), '0000111122223333');
       Spark
      > SELECT _FUNC_(unhex('6E7CA17BBB468D3084B5744BCA729FB7B2B7BCB8E4472847D02670489D95FA97DBBA7D3210'), '0000111122223333', 'GCM');
       Spark SQL
      > SELECT _FUNC_(unbase64('3lmwu+Mw0H3fi5NDvcu9lg=='), '1234567890abcdef', 'ECB', 'PKCS');
       Spark SQL
  """,
  since = "3.3.0",
  group = "misc_funcs")
case class AesDecrypt(
    input: Expression,
    key: Expression,
    mode: Expression,
    padding: Expression)
  extends RuntimeReplaceable with ImplicitCastInputTypes {

  override lazy val replacement: Expression = StaticInvoke(
    classOf[ExpressionImplUtils],
    BinaryType,
    "aesDecrypt",
    Seq(input, key, mode, padding),
    inputTypes)

  def this(input: Expression, key: Expression, mode: Expression) =
    this(input, key, mode, Literal("DEFAULT"))
  def this(input: Expression, key: Expression) =
    this(input, key, Literal("GCM"))

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(BinaryType, BinaryType, StringType, StringType)
  }

  override def prettyName: String = "aes_decrypt"

  override def children: Seq[Expression] = Seq(input, key, mode, padding)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    copy(newChildren(0), newChildren(1), newChildren(2), newChildren(3))
  }
}

@ExpressionDescription(
  usage = "_FUNC_(expr, key[, mode[, padding]]) - This is a special version of `aes_decrypt` that performs the same operation, but returns a NULL value instead of raising an error if the decryption cannot be performed.",
  examples = """
    Examples:
      > SELECT _FUNC_(unhex('6E7CA17BBB468D3084B5744BCA729FB7B2B7BCB8E4472847D02670489D95FA97DBBA7D3210'), '0000111122223333', 'GCM');
       Spark SQL
      > SELECT _FUNC_(unhex('----------468D3084B5744BCA729FB7B2B7BCB8E4472847D02670489D95FA97DBBA7D3210'), '0000111122223333', 'GCM');
       NULL
  """,
  since = "3.5.0",
  group = "misc_funcs")
// scalastyle:on line.size.limit
case class TryAesDecrypt(
    input: Expression,
    key: Expression,
    mode: Expression,
    padding: Expression,
    replacement: Expression) extends RuntimeReplaceable with InheritAnalysisRules {

  def this(input: Expression, key: Expression, mode: Expression, padding: Expression) =
    this(input, key, mode, padding, TryEval(AesDecrypt(input, key, mode, padding)))
  def this(input: Expression, key: Expression, mode: Expression) =
    this(input, key, mode, Literal("DEFAULT"))
  def this(input: Expression, key: Expression) =
    this(input, key, Literal("GCM"))

  override def prettyName: String = "try_aes_decrypt"

  override def parameters: Seq[Expression] = Seq(input, key, mode, padding)

  override protected def withNewChildInternal(newChild: Expression): Expression =
    this.copy(replacement = newChild)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """_FUNC_() - Get the map of Spark SQL keywords, where the keys are keywords and the values indicate whether it is reserved or not""",
  examples = """
    Examples:
      > SELECT _FUNC_();
       {"ADD":false,"AFTER":false,"ALL":false,"ALTER":false,"ALWAYS":false,"ANALYZE":false,"AND":false,"ANTI":false,"ANY":false,"ANY_VALUE":false,"ARCHIVE":false,"ARRAY":false,"AS":false,"ASC":false,"AT":false,"AUTHORIZATION":false,"BETWEEN":false,"BIGINT":false,"BINARY":false,"BOOLEAN":false,"BOTH":false,"BUCKET":false,"BUCKETS":false,"BY":false,"BYTE":false,"CACHE":false,"CASCADE":false,"CASE":false,"CAST":false,"CATALOG":false,"CATALOGS":false,"CHANGE":false,"CHAR":false,"CHARACTER":false,"CHECK":false,"CLEAR":false,"CLUSTER":false,"CLUSTERED":false,"CODEGEN":false,"COLLATE":false,"COLLECTION":false,"COLUMN":false,"COLUMNS":false,"COMMENT":false,"COMMIT":false,"COMPACT":false,"COMPACTIONS":false,"COMPUTE":false,"CONCATENATE":false,"CONSTRAINT":false,"COST":false,"CREATE":false,"CROSS":false,"CUBE":false,"CURRENT":false,"CURRENT_DATE":false,"CURRENT_TIME":false,"CURRENT_TIMESTAMP":false,"CURRENT_USER":false,"DATA":false,"DATABASE":false,"DATABASES":false,"DATE":false,"DATEADD":false,"DATEDIFF":false,"DAY":false,"DAYOFYEAR":false,"DAYS":false,"DBPROPERTIES":false,"DEC":false,"DECIMAL":false,"DEFAULT":false,"DEFINED":false,"DELETE":false,"DELIMITED":false,"DESC":false,"DESCRIBE":false,"DFS":false,"DIRECTORIES":false,"DIRECTORY":false,"DISTINCT":false,"DISTRIBUTE":false,"DIV":false,"DOUBLE":false,"DROP":false,"ELSE":false,"END":false,"ESCAPE":false,"ESCAPED":false,"EXCEPT":false,"EXCHANGE":false,"EXCLUDE":false,"EXISTS":false,"EXPLAIN":false,"EXPORT":false,"EXTENDED":false,"EXTERNAL":false,"EXTRACT":false,"FALSE":false,"FETCH":false,"FIELDS":false,"FILEFORMAT":false,"FILTER":false,"FIRST":false,"FLOAT":false,"FOLLOWING":false,"FOR":false,"FOREIGN":false,"FORMAT":false,"FORMATTED":false,"FROM":false,"FULL":false,"FUNCTION":false,"FUNCTIONS":false,"GENERATED":false,"GLOBAL":false,"GRANT":false,"GROUP":false,"GROUPING":false,"HAVING":false,"HOUR":false,"HOURS":false,"IF":false,"IGNORE":false,"ILIKE":false,"IMPORT":false,"IN":false,"INCLUDE":false,"INDEX":false,"INDEXES":false,"INNER":false,"INPATH":false,"INPUTFORMAT":false,"INSERT":false,"INT":false,"INTEGER":false,"INTERSECT":false,"INTERVAL":false,"INTO":false,"IS":false,"ITEMS":false,"JOIN":false,"KEYS":false,"LAST":false,"LATERAL":false,"LAZY":false,"LEADING":false,"LEFT":false,"LIKE":false,"LIMIT":false,"LINES":false,"LIST":false,"LOAD":false,"LOCAL":false,"LOCATION":false,"LOCK":false,"LOCKS":false,"LOGICAL":false,"LONG":false,"MACRO":false,"MAP":false,"MATCHED":false,"MERGE":false,"MICROSECOND":false,"MICROSECONDS":false,"MILLISECOND":false,"MILLISECONDS":false,"MINUS":false,"MINUTE":false,"MINUTES":false,"MONTH":false,"MONTHS":false,"MSCK":false,"NAMESPACE":false,"NAMESPACES":false,"NANOSECOND":false,"NANOSECONDS":false,"NATURAL":false,"NO":false,"NULL":false,"NULLS":false,"NUMERIC":false,"OF":false,"OFFSET":false,"ON":false,"ONLY":false,"OPTION":false,"OPTIONS":false,"OR":false,"ORDER":false,"OUT":false,"OUTER":false,"OUTPUTFORMAT":false,"OVER":false,"OVERLAPS":false,"OVERLAY":false,"OVERWRITE":false,"PARTITION":false,"PARTITIONED":false,"PARTITIONS":false,"PERCENT":false,"PERCENTILE_CONT":false,"PERCENTILE_DISC":false,"PIVOT":false,"PLACING":false,"POSITION":false,"PRECEDING":false,"PRIMARY":false,"PRINCIPALS":false,"PROPERTIES":false,"PURGE":false,"QUARTER":false,"QUERY":false,"RANGE":false,"REAL":false,"RECORDREADER":false,"RECORDWRITER":false,"RECOVER":false,"REDUCE":false,"REFERENCES":false,"REFRESH":false,"RENAME":false,"REPAIR":false,"REPEATABLE":false,"REPLACE":false,"RESET":false,"RESPECT":false,"RESTRICT":false,"REVOKE":false,"RIGHT":false,"ROLE":false,"ROLES":false,"ROLLBACK":false,"ROLLUP":false,"ROW":false,"ROWS":false,"SCHEMA":false,"SCHEMAS":false,"SECOND":false,"SECONDS":false,"SELECT":false,"SEMI":false,"SEPARATED":false,"SERDE":false,"SERDEPROPERTIES":false,"SESSION_USER":false,"SET":false,"SETS":false,"SHORT":false,"SHOW":false,"SKEWED":false,"SMALLINT":false,"SOME":false,"SORT":false,"SORTED":false,"SOURCE":false,"START":false,"STATISTICS":false,"STORED":false,"STRATIFY":false,"STRING":false,"STRUCT":false,"SUBSTR":false,"SUBSTRING":false,"SYNC":false,"SYSTEM_TIME":false,"SYSTEM_VERSION":false,"TABLE":false,"TABLES":false,"TABLESAMPLE":false,"TARGET":false,"TBLPROPERTIES":false,"TERMINATED":false,"THEN":false,"TIME":false,"TIMESTAMP":false,"TIMESTAMPADD":false,"TIMESTAMPDIFF":false,"TIMESTAMP_LTZ":false,"TIMESTAMP_NTZ":false,"TINYINT":false,"TO":false,"TOUCH":false,"TRAILING":false,"TRANSACTION":false,"TRANSACTIONS":false,"TRANSFORM":false,"TRIM":false,"TRUE":false,"TRUNCATE":false,"TRY_CAST":false,"TYPE":false,"UNARCHIVE":false,"UNBOUNDED":false,"UNCACHE":false,"UNION":false,"UNIQUE":false,"UNKNOWN":false,"UNLOCK":false,"UNPIVOT":false,"UNSET":false,"UPDATE":false,"USE":false,"USER":false,"USING":false,"VALUES":false,"VARCHAR":false,"VERSION":false,"VIEW":false,"VIEWS":false,"VOID":false,"WEEK":false,"WEEKS":false,"WHEN":false,"WHERE":false,"WINDOW":false,"WITH":false,"WITHIN":false,"X":false,"YEAR":false,"YEARS":false,"ZONE":false}
  """,
  since = "3.5.0",
  group = "misc_funcs")
// scalastyle:on line.size.limit
case class SQLKeywords() extends LeafExpression with Unevaluable {
  override def nullable: Boolean = false
  override def dataType: DataType = MapType(StringType, BooleanType, false)
  override def prettyName: String = "sql_keywords"
  final override val nodePatterns: Seq[TreePattern] = Seq(SQL_KEYWORDS)
}
