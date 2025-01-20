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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ExpressionBuilder, FunctionRegistry, UnresolvedSeed}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.trees.TreePattern.{CURRENT_LIKE, TreePattern}
import org.apache.spark.sql.catalyst.util.{MapData, RandomUUIDGenerator}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.errors.QueryExecutionErrors.raiseError
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.types.{AbstractMapType, StringTypeWithCollation}
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
 * Caller can specify the errorClass to be thrown and parameters to be passed to this error class.
 * Default is to throw USER_RAISED_EXCEPTION with provided string literal.
 */
case class RaiseError(errorClass: Expression, errorParms: Expression, dataType: DataType)
  extends BinaryExpression with ImplicitCastInputTypes {

  def this(str: Expression) = {
    this(Literal(
      if (SQLConf.get.getConf(SQLConf.LEGACY_RAISE_ERROR_WITHOUT_ERROR_CLASS)) {
        "_LEGACY_ERROR_USER_RAISED_EXCEPTION"
      } else {
        "USER_RAISED_EXCEPTION"
      }),
      CreateMap(Seq(Literal("errorMessage"), str)), NullType)
  }

  def this(errorClass: Expression, errorParms: Expression) = {
    this(errorClass, errorParms, NullType)
  }

  override def foldable: Boolean = false
  override def nullable: Boolean = true
  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      StringTypeWithCollation(supportsTrimCollation = true),
      AbstractMapType(
        StringTypeWithCollation(supportsTrimCollation = true),
        StringTypeWithCollation(supportsTrimCollation = true)
      ))

  override def left: Expression = errorClass
  override def right: Expression = errorParms

  override def prettyName: String = "raise_error"

  override def eval(input: InternalRow): Any = {
    val error = errorClass.eval(input).asInstanceOf[UTF8String]
    val parms: MapData = errorParms.eval(input).asInstanceOf[MapData]
    throw raiseError(error, parms)
  }

  // if (true) is to avoid codegen compilation exception that statement is unreachable
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val error = errorClass.genCode(ctx)
    val parms = errorParms.genCode(ctx)
    ExprCode(
      code = code"""${error.code}
        |${parms.code}
        |if (true) {
        |  throw QueryExecutionErrors.raiseError(
        |    ${error.value},
        |    ${parms.value});
        |}""".stripMargin,
      isNull = TrueLiteral,
      value = JavaCode.defaultLiteral(dataType)
    )
  }

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): RaiseError = {
    copy(errorClass = newLeft, errorParms = newRight)
  }
}

object RaiseError {
  def apply(str: Expression): RaiseError = new RaiseError(str)

  def apply(errorClass: Expression, parms: Expression): RaiseError =
    new RaiseError(errorClass, parms)
}

/**
 * Throw with the result of an expression (used for debugging).
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_( expr ) - Throws a USER_RAISED_EXCEPTION with `expr` as message.",
  examples = """
    Examples:
      > SELECT _FUNC_('custom error message');
       [USER_RAISED_EXCEPTION] custom error message
  """,
  since = "3.1.0",
  group = "misc_funcs")
// scalastyle:on line.size.limit
object RaiseErrorExpressionBuilder extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    if (expressions.length != 1) {
      throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(1), expressions.length)
    } else {
      RaiseError(expressions.head)
    }
  }
}

/**
 * A function that throws an exception if 'condition' is not true.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr [, message]) - Throws an exception if `expr` is not true.",
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
case class CurrentDatabase()
  extends LeafExpression
  with DefaultStringProducingExpression
  with Unevaluable {
  override def nullable: Boolean = false
  override def prettyName: String = "current_schema"
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
case class CurrentCatalog()
  extends LeafExpression
  with DefaultStringProducingExpression
  with Unevaluable {
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
  with DefaultStringProducingExpression
  with ExpressionWithRandomSeed {

  def this() = this(None)

  def this(seed: Expression) = this(ExpressionWithRandomSeed.expressionToSeed(seed, "UUID"))

  override def seedExpression: Expression = randomSeed.map(Literal.apply).getOrElse(UnresolvedSeed)

  override def withNewSeed(seed: Long): Uuid = Uuid(Some(seed))

  override lazy val resolved: Boolean = randomSeed.isDefined

  override def nullable: Boolean = false

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
case class SparkVersion()
  extends LeafExpression
  with RuntimeReplaceable
  with DefaultStringProducingExpression {
  override def prettyName: String = "version"

  override lazy val replacement: Expression = StaticInvoke(
    classOf[ExpressionImplUtils],
    dataType,
    "getSparkVersion",
    returnNullable = false)
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
case class TypeOf(child: Expression) extends UnaryExpression with DefaultStringProducingExpression {
  override def nullable: Boolean = false
  override def foldable: Boolean = true
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
case class CurrentUser()
  extends LeafExpression
  with DefaultStringProducingExpression
  with Unevaluable {
  override def nullable: Boolean = false
  override def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("current_user")
  final override val nodePatterns: Seq[TreePattern] = Seq(CURRENT_LIKE)
}

/**
 * A function that encrypts input using AES. Key lengths of 128, 192 or 256 bits can be used.
 * If either argument is NULL or the key length is not one of the permitted values,
 * the return value is NULL.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr, key[, mode[, padding[, iv[, aad]]]]) - Returns an encrypted value of `expr` using AES in given `mode` with the specified `padding`.
      Key lengths of 16, 24 and 32 bits are supported. Supported combinations of (`mode`, `padding`) are ('ECB', 'PKCS'), ('GCM', 'NONE') and ('CBC', 'PKCS').
      Optional initialization vectors (IVs) are only supported for CBC and GCM modes. These must be 16 bytes for CBC and 12 bytes for GCM. If not provided, a random vector will be generated and prepended to the output.
      Optional additional authenticated data (AAD) is only supported for GCM. If provided for encryption, the identical AAD value must be provided for decryption.
      The default mode is GCM.
  """,
  arguments = """
    Arguments:
      * expr - The binary value to encrypt.
      * key - The passphrase to use to encrypt the data.
      * mode - Specifies which block cipher mode should be used to encrypt messages.
               Valid modes: ECB, GCM, CBC.
      * padding - Specifies how to pad messages whose length is not a multiple of the block size.
                  Valid values: PKCS, NONE, DEFAULT. The DEFAULT padding means PKCS for ECB, NONE for GCM and PKCS for CBC.
      * iv - Optional initialization vector. Only supported for CBC and GCM modes.
             Valid values: None or ''. 16-byte array for CBC mode. 12-byte array for GCM mode.
      * aad - Optional additional authenticated data. Only supported for GCM mode. This can be any free-form input and
              must be provided for both encryption and decryption.
  """,
  examples = """
    Examples:
      > SELECT hex(_FUNC_('Spark', '0000111122223333'));
       83F16B2AA704794132802D248E6BFD4E380078182D1544813898AC97E709B28A94
      > SELECT hex(_FUNC_('Spark SQL', '0000111122223333', 'GCM'));
       6E7CA17BBB468D3084B5744BCA729FB7B2B7BCB8E4472847D02670489D95FA97DBBA7D3210
      > SELECT base64(_FUNC_('Spark SQL', '1234567890abcdef', 'ECB', 'PKCS'));
       3lmwu+Mw0H3fi5NDvcu9lg==
      > SELECT base64(_FUNC_('Apache Spark', '1234567890abcdef', 'CBC', 'DEFAULT'));
       2NYmDCjgXTbbxGA3/SnJEfFC/JQ7olk2VQWReIAAFKo=
      > SELECT base64(_FUNC_('Spark', 'abcdefghijklmnop12345678ABCDEFGH', 'CBC', 'DEFAULT', unhex('00000000000000000000000000000000')));
       AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg=
      > SELECT base64(_FUNC_('Spark', 'abcdefghijklmnop12345678ABCDEFGH', 'GCM', 'DEFAULT', unhex('000000000000000000000000'), 'This is an AAD mixed into the input'));
       AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4
  """,
  since = "3.3.0",
  group = "misc_funcs")
case class AesEncrypt(
    input: Expression,
    key: Expression,
    mode: Expression,
    padding: Expression,
    iv: Expression,
    aad: Expression)
  extends RuntimeReplaceable with ImplicitCastInputTypes {

  override lazy val replacement: Expression = StaticInvoke(
    classOf[ExpressionImplUtils],
    BinaryType,
    "aesEncrypt",
    Seq(input, key, mode, padding, iv, aad),
    inputTypes)

  def this(input: Expression, key: Expression, mode: Expression, padding: Expression, iv: Expression) =
    this(input, key, mode, padding, iv, Literal(""))
  def this(input: Expression, key: Expression, mode: Expression, padding: Expression) =
    this(input, key, mode, padding, Literal(""))
  def this(input: Expression, key: Expression, mode: Expression) =
    this(input, key, mode, Literal("DEFAULT"))
  def this(input: Expression, key: Expression) =
    this(input, key, Literal("GCM"))

  override def prettyName: String = "aes_encrypt"

  override def inputTypes: Seq[AbstractDataType] =
    Seq(BinaryType, BinaryType,
      StringTypeWithCollation(supportsTrimCollation = true),
      StringTypeWithCollation(supportsTrimCollation = true),
      BinaryType, BinaryType)

  override def children: Seq[Expression] = Seq(input, key, mode, padding, iv, aad)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    copy(newChildren(0), newChildren(1), newChildren(2), newChildren(3), newChildren(4), newChildren(5))
  }
}

/**
 * A function that decrypts input using AES. Key lengths of 128, 192 or 256 bits can be used.
 * If either argument is NULL or the key length is not one of the permitted values,
 * the return value is NULL.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr, key[, mode[, padding[, aad]]]) - Returns a decrypted value of `expr` using AES in `mode` with `padding`.
      Key lengths of 16, 24 and 32 bits are supported. Supported combinations of (`mode`, `padding`) are ('ECB', 'PKCS'), ('GCM', 'NONE') and ('CBC', 'PKCS').
      Optional additional authenticated data (AAD) is only supported for GCM. If provided for encryption, the identical AAD value must be provided for decryption.
      The default mode is GCM.
  """,
  arguments = """
    Arguments:
      * expr - The binary value to decrypt.
      * key - The passphrase to use to decrypt the data.
      * mode - Specifies which block cipher mode should be used to decrypt messages.
               Valid modes: ECB, GCM, CBC.
      * padding - Specifies how to pad messages whose length is not a multiple of the block size.
                  Valid values: PKCS, NONE, DEFAULT. The DEFAULT padding means PKCS for ECB, NONE for GCM and PKCS for CBC.
      * aad - Optional additional authenticated data. Only supported for GCM mode. This can be any free-form input and
              must be provided for both encryption and decryption.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(unhex('83F16B2AA704794132802D248E6BFD4E380078182D1544813898AC97E709B28A94'), '0000111122223333');
       Spark
      > SELECT _FUNC_(unhex('6E7CA17BBB468D3084B5744BCA729FB7B2B7BCB8E4472847D02670489D95FA97DBBA7D3210'), '0000111122223333', 'GCM');
       Spark SQL
      > SELECT _FUNC_(unbase64('3lmwu+Mw0H3fi5NDvcu9lg=='), '1234567890abcdef', 'ECB', 'PKCS');
       Spark SQL
      > SELECT _FUNC_(unbase64('2NYmDCjgXTbbxGA3/SnJEfFC/JQ7olk2VQWReIAAFKo='), '1234567890abcdef', 'CBC');
       Apache Spark
      > SELECT _FUNC_(unbase64('AAAAAAAAAAAAAAAAAAAAAPSd4mWyMZ5mhvjiAPQJnfg='), 'abcdefghijklmnop12345678ABCDEFGH', 'CBC', 'DEFAULT');
       Spark
      > SELECT _FUNC_(unbase64('AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4'), 'abcdefghijklmnop12345678ABCDEFGH', 'GCM', 'DEFAULT', 'This is an AAD mixed into the input');
       Spark
  """,
  since = "3.3.0",
  group = "misc_funcs")
case class AesDecrypt(
    input: Expression,
    key: Expression,
    mode: Expression,
    padding: Expression,
    aad: Expression)
  extends RuntimeReplaceable with ImplicitCastInputTypes {

  override lazy val replacement: Expression = StaticInvoke(
    classOf[ExpressionImplUtils],
    BinaryType,
    "aesDecrypt",
    Seq(input, key, mode, padding, aad),
    inputTypes)

  def this(input: Expression, key: Expression, mode: Expression, padding: Expression) =
    this(input, key, mode, padding, Literal(""))
  def this(input: Expression, key: Expression, mode: Expression) =
    this(input, key, mode, Literal("DEFAULT"))
  def this(input: Expression, key: Expression) =
    this(input, key, Literal("GCM"))

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(BinaryType,
      BinaryType,
      StringTypeWithCollation(supportsTrimCollation = true),
      StringTypeWithCollation(supportsTrimCollation = true), BinaryType)
  }

  override def prettyName: String = "aes_decrypt"

  override def children: Seq[Expression] = Seq(input, key, mode, padding, aad)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    copy(newChildren(0), newChildren(1), newChildren(2), newChildren(3), newChildren(4))
  }
}

@ExpressionDescription(
  usage = "_FUNC_(expr, key[, mode[, padding[, aad]]]) - This is a special version of `aes_decrypt` that performs the same operation, but returns a NULL value instead of raising an error if the decryption cannot be performed.",
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
    aad: Expression,
    replacement: Expression) extends RuntimeReplaceable with InheritAnalysisRules {

  def this(input: Expression,
           key: Expression,
           mode: Expression,
           padding: Expression,
           aad: Expression) =
    this(input, key, mode, padding, aad, TryEval(AesDecrypt(input, key, mode, padding, aad)))
  def this(input: Expression, key: Expression, mode: Expression, padding: Expression) =
    this(input, key, mode, padding, Literal(""))
  def this(input: Expression, key: Expression, mode: Expression) =
    this(input, key, mode, Literal("DEFAULT"))
  def this(input: Expression, key: Expression) =
    this(input, key, Literal("GCM"))

  override def prettyName: String = "try_aes_decrypt"

  override def parameters: Seq[Expression] = Seq(input, key, mode, padding, aad)

  override protected def withNewChildInternal(newChild: Expression): Expression =
    this.copy(replacement = newChild)
}
// scalastyle:on line.size.limit
