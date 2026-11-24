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

package org.apache.spark.sql.catalyst.util

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.connector.catalog.Column
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._

object CharVarcharUtils extends Logging with SparkCharVarcharUtils {

  // visible for testing
  private[sql] val CHAR_VARCHAR_TYPE_STRING_METADATA_KEY = "__CHAR_VARCHAR_TYPE_STRING"

  /** JVM method name used by [[addPaddingForScan]] when read-side CHAR padding is on. */
  private[sql] val READ_SIDE_PADDING = "readSidePadding"

  /** JVM method name used by [[addPaddingForScan]] under standard CHAR/VARCHAR semantics. */
  private[sql] val CHAR_TYPE_READ_SIDE_CHECK = "charTypeReadSideCheck"

  private[sql] val VARCHAR_TYPE_READ_SIDE_CHECK = "varcharTypeReadSideCheck"

  /** JVM method name used by [[narrowCharToLength]]. */
  private[sql] val CHAR_TYPE_NARROWING = "charTypeNarrowing"

  /**
   * V2 function name for CHAR read-side padding. Distinct from SQL `RPAD` so connectors that
   * already allowlist `RPAD` for explicit `rpad()` do not inherit CHAR-scan rewrite. Emitted
   * only for equality, null-safe equality, inequality, or IN against a string of the CHAR
   * length. JDBC may compile it as the column only when the remote compares CHAR
   * blank-padded (see `JdbcDialect.isSupportedFunction`) and stored values never exceed
   * that length.
   */
  private[sql] val V2_CHAR_READ_SIDE_PADDING = "CHAR_READ_SIDE_PADDING"

  /**
   * Like [[hasCharVarchar]], but also inspects [[UserDefinedType]] storage types.
   * Python/Arrow host checks must match PySpark `_has_type`, which unwraps UDTs.
   */
  def hasCharVarcharIncludingUDT(dt: DataType): Boolean = {
    dt.existsRecursively {
      case _: CharType | _: VarcharType => true
      case u: UserDefinedType[_] => hasCharVarcharIncludingUDT(u.sqlType)
      case _ => false
    }
  }

  /**
   * Creates a StringRPad expression with the pad literal inheriting the collation from the
   * str expression's data type. This is necessary because StringRPad may be created after
   * CollationTypeCasts has run, so the default pad with StringType companion object would
   * not get its collation coerced to match the str expression.
   */
  private[sql] def createStringRPad(str: Expression, len: Expression): StringRPad = {
    StringRPad(str, len, Literal.create(" ", str.dataType))
  }

  /** Pads the given CHAR value to the length of `charType` as read-side padding does. */
  private[sql] def createReadSidePadding(str: Expression, charType: CharType): Expression = {
    processStringForCharVarchar(
      str, charType, charFuncName = Some(READ_SIDE_PADDING), varcharFuncName = None)
  }

  /**
   * Replaces CharType/VarcharType with StringType recursively in the given struct type. If a
   * top-level StructField's data type is CharType/VarcharType or has nested CharType/VarcharType,
   * this method will add the original type string to the StructField's metadata, so that we can
   * re-construct the original data type with CharType/VarcharType later when needed.
   */
  def replaceCharVarcharWithStringInSchema(st: StructType): StructType = {
    replaceCharVarcharWithStringInSchema(st, replaceCharVarcharWithString)
  }

  /**
   * Like [[replaceCharVarcharWithStringInSchema]], but always rewrites the physical type to
   * unbounded STRING even when first-class CHAR/VARCHAR is enabled. Hive TRANSFORM script I/O
   * is STRING-on-the-wire; callers apply [[stringLengthCheck]] in a Project when first-class
   * CHAR/VARCHAR types are enabled.
   */
  def replaceCharVarcharWithStringInSchemaAlways(st: StructType): StructType = {
    replaceCharVarcharWithStringInSchema(st, replaceCharVarcharWithStringAlways)
  }

  private def replaceCharVarcharWithStringInSchema(
      st: StructType,
      replace: DataType => DataType): StructType = {
    StructType(st.map { field =>
      if (hasCharVarchar(field.dataType)) {
        val metadata = new MetadataBuilder().withMetadata(field.metadata)
          .putString(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY, field.dataType.catalogString).build()
        field.copy(dataType = replace(field.dataType), metadata = metadata)
      } else {
        field
      }
    })
  }

  /**
   * Replaces CharType with VarcharType recursively in the given data type.
   */
  def replaceCharWithVarchar(dt: DataType): DataType = dt match {
    case ArrayType(et, nullable) =>
      ArrayType(replaceCharWithVarchar(et), nullable)
    case MapType(kt, vt, nullable) =>
      MapType(replaceCharWithVarchar(kt), replaceCharWithVarchar(vt), nullable)
    case StructType(fields) =>
      StructType(fields.map { field =>
        field.copy(dataType = replaceCharWithVarchar(field.dataType))
      })
    case c: CharType => VarcharType(c.length, c.collation)
    case _ => dt
  }

  /**
   * Replaces CharType/VarcharType with StringType recursively in the given data type, with a
   * warning message if it has char or varchar types
   */
  def replaceCharVarcharWithStringForCast(dt: DataType): DataType = {
    // standardSemantics takes precedence over legacy charVarcharAsString.
    if (SQLConf.get.charVarcharAsString && !SQLConf.get.charVarcharStandardSemantics) {
      replaceCharVarcharWithString(dt)
    } else if (hasCharVarchar(dt) && !SQLConf.get.charVarcharFirstClassTypes) {
      logWarning(log"The Spark cast operator does not support char/varchar type and simply treats" +
        log" them as string type. Please use string type directly to avoid confusion. Otherwise," +
        log" you can set ${MDC(CONFIG, SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key)} " +
        log"to true, so that Spark treat them as string type as same as Spark 3.0 and earlier")
      replaceCharVarcharWithString(dt)
    } else {
      dt
    }
  }

  /**
   * Removes the metadata entry that contains the original type string of CharType/VarcharType from
   * the given attribute's metadata.
   */
  def cleanAttrMetadata(attr: AttributeReference): AttributeReference = {
    attr.withMetadata(cleanMetadata(attr.metadata))
  }

  /**
   * Removes the metadata entry that contains the original type string of CharType/VarcharType from
   * the given metadata.
   */
  def cleanMetadata(metadata: Metadata): Metadata = {
    new MetadataBuilder()
      .withMetadata(metadata)
      .remove(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY)
      .build()
  }

  /**
   * Adds the CHAR/VARCHAR types of the V2 `columns` to the attributes of `output` with the same
   * names. `TableOutputResolver.applyColumnMetadata` removes them from the query of a write. An
   * expression stored with the table, such as a CHECK condition or a generation expression,
   * resolved against the result pads a string literal compared with a CHAR column, as the write
   * pads the stored values. A write that treats CHAR as STRING does not pad the values, so the
   * types are removed instead, also those of a query that reads a CHAR column and already matches
   * the table: `ApplyCharTypePadding` skips such a write, but not an expression analyzed under the
   * fixed settings of `StoredExpressionText`.
   */
  def withCharVarcharTypes(output: Seq[Attribute], columns: Array[Column]): Seq[Attribute] = {
    val conf = SQLConf.get
    // standardSemantics takes precedence over legacy charVarcharAsString.
    if (conf.charVarcharAsString && !conf.charVarcharStandardSemantics) {
      output.map(a => a.withMetadata(cleanMetadata(a.metadata)))
    } else {
      val charVarcharColumns = columns.filter(c => hasCharVarchar(c.dataType()))
      output.map { a =>
        charVarcharColumns.find(c => conf.resolver(c.name(), a.name)).fold(a) { c =>
          a.withMetadata(new MetadataBuilder().withMetadata(a.metadata)
            .putString(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY, c.dataType().catalogString)
            .build())
        }
      }
    }
  }

  def getRawTypeString(metadata: Metadata): Option[String] = {
    if (metadata.contains(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY)) {
      Some(metadata.getString(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY))
    } else {
      None
    }
  }

  /**
   * Re-construct the original data type from the type string in the given metadata.
   * This is needed when dealing with char/varchar columns/fields.
   */
  def getRawType(metadata: Metadata): Option[DataType] = {
    getRawTypeString(metadata).map(CatalystSqlParser.parseDataType)
  }

  /**
   * Re-construct the original schema from the type string in the given metadata of each field.
   */
  def getRawSchema(schema: StructType): StructType = {
    val fields = schema.map { field =>
      getRawType(field.metadata).map(dt => field.copy(dataType = dt)).getOrElse(field)
    }
    StructType(fields)
  }

  def getRawSchema(schema: StructType, conf: SQLConf): StructType = {
    val fields = schema.map { field =>
      getRawType(field.metadata).map { dt =>
        if (conf.getConf(SQLConf.CHAR_AS_VARCHAR)) {
          val metadata = new MetadataBuilder().withMetadata(field.metadata)
            .remove(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY).build()
          field.copy(dataType = replaceCharWithVarchar(dt), metadata = metadata)
        } else {
          field.copy(dataType = dt)
        }
      }.getOrElse(field)
    }
    StructType(fields)
  }

  /**
   * Applies assignment semantics when parsing text into a typed schema. CHAR values are padded,
   * and excess trailing spaces are trimmed when the remaining value fits. Non-space overflow
   * raises EXCEED_LIMIT_LENGTH. Null stays null, and unbounded STRING is unchanged.
   *
   * With first-class CHAR/VARCHAR semantics enabled, for example,
   * `from_json('{"c":"ab"}', 'c CHAR(4)')` produces `"ab  "`, while parsing `{"v":"abcd"}`
   * as `v VARCHAR(3)` in FAILFAST mode raises EXCEED_LIMIT_LENGTH.
   */
  def applyTextParseSemantics(value: UTF8String, dt: DataType): UTF8String = {
    if (value == null) {
      null
    } else {
      dt match {
        case c: CharType =>
          CharVarcharCodegenUtils.charTypeWriteSideCheck(value, c.length)
        case v: VarcharType =>
          CharVarcharCodegenUtils.varcharTypeWriteSideCheck(value, v.length)
        case _ => value
      }
    }
  }

  /**
   * True if a CHAR/VARCHAR map key is not a valid identity key: CHAR(n) must be exactly n
   * characters (shorter keys fail too); VARCHAR(n) must be at most n. STRING and other
   * key types never violate. Avro map-key checks use this; JSON/XML keep local checks.
   */
  def charVarcharMapKeyViolatesLength(key: UTF8String, keyType: DataType): Boolean = {
    keyType match {
      case c: CharType => key.numChars() != c.length
      case v: VarcharType => key.numChars() > v.length
      case _ => false
    }
  }

  /**
   * Caps a map key echoed in an error so a long user-supplied name cannot blow the message.
   */
  def truncateMapKeyForError(key: UTF8String, maxChars: Int = 128): UTF8String = {
    if (key != null && key.numChars() > maxChars) {
      UTF8String.concat(key.substring(0, maxChars), UTF8String.fromString("..."))
    } else {
      key
    }
  }

  /**
   * Returns an expression to apply write-side string length check for the given expression. A
   * string value can not exceed N characters if it's written into a CHAR(N)/VARCHAR(N)
   * column/field.
   */
  def stringLengthCheck(expr: Expression, targetAttr: Attribute): Expression = {
    getRawType(targetAttr.metadata).map { rawType =>
      stringLengthCheck(expr, rawType)
    }.getOrElse(expr)
  }

  /**
   * Write-side CHAR/VARCHAR length checks apply unless the session is on the legacy
   * `charVarcharAsString` path with no first-class types. `standardSemantics` and
   * `preserveCharVarcharTypeInfo` keep first-class types even if the legacy flag is also on.
   */
  def shouldApplyWriteSideLengthCheck(conf: SQLConf): Boolean = {
    !conf.charVarcharAsString || conf.charVarcharFirstClassTypes
  }

  def stringLengthCheck(expr: Expression, dt: DataType): Expression = {
    processStringForCharVarchar(
      expr,
      dt,
      charFuncName = Some("charTypeWriteSideCheck"),
      varcharFuncName = Some("varcharTypeWriteSideCheck"))
  }

  /**
   * Applies the given CHAR and VARCHAR functions to the strings of `expr` with the CHAR and VARCHAR
   * types of `dt`, rebuilding the structs, arrays and maps around them. A rebuilt struct reads the
   * fields of a nullable struct only where the struct is not null. With `keepFieldNullability`,
   * it reads them through [[KnownNotNull]] there, so that fields that are not nullable stay so and
   * the rebuilt value has the type of `expr`.
   */
  private def processStringForCharVarchar(
      expr: Expression,
      dt: DataType,
      charFuncName: Option[String],
      varcharFuncName: Option[String],
      keepFieldNullability: Boolean = false): Expression = {
    dt match {
      case c: CharType if charFuncName.isDefined =>
        StaticInvoke(
          classOf[CharVarcharCodegenUtils],
          if (SQLConf.get.charVarcharFirstClassTypes) {
            c
          } else {
            c.toStringType
          },
          charFuncName.get,
          expr :: Literal(c.length) :: Nil,
          returnNullable = false)

      case v: VarcharType if varcharFuncName.isDefined =>
        StaticInvoke(
          classOf[CharVarcharCodegenUtils],
          if (SQLConf.get.charVarcharFirstClassTypes) {
            v
          } else {
            v.toStringType
          },
          varcharFuncName.get,
          expr :: Literal(v.length) :: Nil,
          returnNullable = false)

      case StructType(fields) =>
        val fieldsOf = if (keepFieldNullability && expr.nullable) KnownNotNull(expr) else expr
        val struct = CreateNamedStruct(fields.zipWithIndex.flatMap { case (f, i) =>
          Seq(Literal(f.name), processStringForCharVarchar(
            GetStructField(fieldsOf, i, Some(f.name)), f.dataType, charFuncName, varcharFuncName,
            keepFieldNullability))
        }.toImmutableArraySeq)
        if (struct.valExprs.forall(_.isInstanceOf[GetStructField])) {
          // No field needs char/varchar processing, just return the original expression.
          expr
        } else {
          nullSafeRebuiltStruct(expr, struct)
        }

      case ArrayType(et, containsNull) =>
        processStringForCharVarcharInArray(
          expr, et, containsNull, charFuncName, varcharFuncName, keepFieldNullability)

      case MapType(kt, vt, valueContainsNull) =>
        val keys = MapKeys(expr)
        val newKeys = processStringForCharVarcharInArray(
          keys, kt, containsNull = false, charFuncName, varcharFuncName, keepFieldNullability)
        val values = MapValues(expr)
        val newValues = processStringForCharVarcharInArray(
          values, vt, valueContainsNull, charFuncName, varcharFuncName, keepFieldNullability)
        if (newKeys.fastEquals(keys) && newValues.fastEquals(values)) {
          // If map key/value does not need char/varchar processing, return the original expression.
          expr
        } else {
          MapFromArrays(newKeys, newValues)
        }

      case _ => expr
    }
  }

  /**
   * Guard a rebuilt named_struct so a NULL input stays NULL. Use CaseWhen rather than If:
   * CollationTypeCoercion rewrites If (no same-type skip) and can widen VARCHAR collations
   * inside the struct to unbounded STRING. CaseWhen skips that when the branches already match.
   */
  private def nullSafeRebuiltStruct(expr: Expression, struct: CreateNamedStruct): Expression = {
    if (expr.nullable) {
      CaseWhen(Seq((IsNull(expr), Literal(null, struct.dataType))), Some(struct))
    } else {
      struct
    }
  }

  private def processStringForCharVarcharInArray(
      arr: Expression,
      et: DataType,
      containsNull: Boolean,
      charFuncName: Option[String],
      varcharFuncName: Option[String],
      keepFieldNullability: Boolean): Expression = {
    // A raw type doesn't record the nullability of its elements and fields, which the array does.
    val param = arr.dataType match {
      case ArrayType(elementType, elementContainsNull) if keepFieldNullability =>
        NamedLambdaVariable("x", elementType, elementContainsNull)
      case _ => NamedLambdaVariable("x", replaceCharVarcharWithString(et), containsNull)
    }
    val funcBody =
      processStringForCharVarchar(param, et, charFuncName, varcharFuncName, keepFieldNullability)
    if (funcBody.fastEquals(param)) {
      // If array element does not need char/varchar processing, return the original expression.
      arr
    } else {
      ArrayTransform(arr, LambdaFunction(funcBody, Seq(param)))
    }
  }

  def addPaddingForScan(attr: Attribute): Expression = {
    // Driven by metadata rather than attr.dataType even when Char/Varchar are first-class types.
    // The metadata is the "not yet padded" marker: ApplyCharTypePadding rebuilds the relation via
    // cleanAttrMetadata, so a second application of the rule finds no raw type and leaves the plan
    // alone. Keying off attr.dataType instead would re-pad an already-padded scan on every pass and
    // break the Once strategy's idempotence check.
    getRawType(attr.metadata).map { dt =>
      if (SQLConf.get.charVarcharStandardSemantics) {
        // Pad CHAR and enforce length limits for CHAR/VARCHAR (trim trailing blanks first).
        processStringForCharVarchar(
          attr,
          dt,
          charFuncName = Some(CHAR_TYPE_READ_SIDE_CHECK),
          varcharFuncName = Some(VARCHAR_TYPE_READ_SIDE_CHECK))
      } else {
        processStringForCharVarchar(
          attr, dt, charFuncName = Some(READ_SIDE_PADDING), varcharFuncName = None)
      }
    }.getOrElse(attr)
  }

  /**
   * Matches the read-side CHAR padding that [[addPaddingForScan]] wraps a column in (when
   * standard semantics are off) and extracts the padded expression and the CHAR length. A padding
   * over another one to a length that is not longer matches as one padding of its value.
   */
  private[sql] object ReadSidePadding {
    def unapply(e: Expression): Option[(Expression, Int)] = e match {
      case s @ StaticInvoke(_, _, READ_SIDE_PADDING, Seq(child, Literal(n: Int, _)), _, _, _, _,
          None) if s.staticObject == classOf[CharVarcharCodegenUtils] =>
        child match {
          // The padding of a set operation over the padding of a scan, to a length that is at
          // least as long, is the padding of the scan to that length.
          case ReadSidePadding(grandChild, m) if m <= n => Some((grandChild, n))
          case _ => Some((child, n))
        }
      case _ => None
    }
  }

  /**
   * Matches what [[addPaddingForScan]] wraps a column in: the read-side CHAR padding, or under
   * standard CHAR/VARCHAR semantics one of the read-side checks. Extracts the wrapped expression,
   * the length, and whether shorter values are padded to that length, as the padding and the CHAR
   * check do. Unlike [[ReadSidePadding]] it also matches the checks, which trim trailing spaces and
   * fail on values still longer than the length, so it must not be used to compile padding.
   */
  private[sql] object ReadSidePaddingOrCheck {
    def unapply(e: Expression): Option[(Expression, Int, Boolean)] = e match {
      case s @ StaticInvoke(_, _, name, Seq(child, Literal(n: Int, _)), _, _, _, _, None)
          if s.staticObject == classOf[CharVarcharCodegenUtils] =>
        name match {
          case READ_SIDE_PADDING | CHAR_TYPE_READ_SIDE_CHECK => Some((child, n, true))
          case VARCHAR_TYPE_READ_SIDE_CHECK => Some((child, n, false))
          case _ => None
        }
      case _ => None
    }
  }

  /**
   * Strips a cast of a CHAR, VARCHAR or STRING value to a STRING of the same collation, such as
   * the one a comparison with a STRING adds when CHAR and VARCHAR are first-class types. Such a
   * cast returns the value unchanged, so the result is unique, or estimated, as the child is.
   */
  def stripCastToPlainString(e: Expression): Expression = e match {
    case Cast(child, to: StringType, _, _) if StringHelper.plainStringType(child.dataType) == to =>
      child
    case _ => e
  }

  /**
   * Return expressions to apply char type padding for the string comparison between the given
   * attributes. When comparing two char type columns/fields, we need to pad the shorter one to
   * the longer length.
   */
  def addPaddingInStringComparison(attrs: Seq[Attribute], alwaysPad: Boolean): Seq[Expression] =
    addPaddingInStringComparison(attrs, attrs.map(attr => getRawType(attr.metadata)), alwaysPad)

  /**
   * Like the method above, for expressions with the given raw types, such as a scalar subquery or
   * a column of an IN subquery, whose raw type is the one of their column. With `truncate = false`,
   * it pads like read-side padding, which keeps longer values instead of truncating them.
   */
  def addPaddingInStringComparison(
      exprs: Seq[Expression],
      rawTypes: Seq[Option[DataType]],
      alwaysPad: Boolean,
      truncate: Boolean = true): Seq[Expression] = {
    if (rawTypes.exists(_.isEmpty)) {
      exprs
    } else if (exprs.exists(expr => !RowOrdering.isOrderable(expr.dataType))) {
      // Comparing a non-orderable type, such as a struct holding a MAP, is rejected later by
      // CheckAnalysis. Leave it alone so the error names the attribute the user wrote rather
      // than the rewritten struct.
      exprs
    } else {
      val typeWithTargetCharLength = rawTypes.map(_.get).reduce(typeWithWiderCharLength)
      exprs.zip(rawTypes.map(_.get)).map { case (expr, rawType) =>
        padCharToTargetLength(expr, rawType, typeWithTargetCharLength, alwaysPad, truncate)
          .getOrElse(expr)
      }
    }
  }

  /**
   * The raw type of the output of a UNION at one position, given the outputs of its children
   * there, if they are CHAR types, or structs and arrays of them, with different lengths at the
   * same places: the widest lengths, as the SQL standard gives the union of CHAR(n) and CHAR(m)
   * the length max(n, m). Keeps the names of the first attribute's fields. Returns None if the
   * raw types are the same, or not all CHAR types of the same shape. `parse` parses a raw type.
   *
   * TODO: this reads only the raw type of a column, so a set operation of structs built with
   * `named_struct`, whose CHAR types are in the metadata of their fields, keeps the lengths of its
   * first child.
   */
  private def widerCharRawType(
      attrs: Seq[Attribute],
      parse: String => DataType): Option[DataType] = {
    val rawTypeStrings = attrs.map(attr => getRawTypeString(attr.metadata))
    if (rawTypeStrings.exists(_.isEmpty) || rawTypeStrings.distinct.length == 1) {
      None
    } else {
      val rawTypes = rawTypeStrings.map(s => parse(s.get))
      // Raw types can differ only in how they print, such as with the default collation given
      // explicitly or with other names of struct fields. They have the same lengths already.
      if (rawTypes.map(normalizeCharType).distinct.length == 1) {
        None
      } else {
        rawTypes.map(Option(_)).reduce {
          (left, right) => for (l <- left; r <- right; wider <- widerCharType(l, r)) yield wider
        }
      }
    }
  }

  // The type with the collation of each CHAR type given explicitly, and without field names.
  private def normalizeCharType(dt: DataType): DataType = dt match {
    case c: CharType => CharType(c.length, Some(c.collationId))
    case StructType(fields) =>
      StructType(fields.map(f => StructField("", normalizeCharType(f.dataType))))
    case ArrayType(et, _) => ArrayType(normalizeCharType(et))
    case other => other
  }

  /**
   * Pads the CHAR columns and fields of the given attribute that are shorter than those at the
   * same places of the given raw type, see [[widerCharRawType]]. Like read-side padding, it does
   * not truncate longer values. Returns the padded value and its raw type, which keeps the names
   * of the attribute's fields, or None if the attribute is already as long, unless `alwaysPad`,
   * for values that the scan does not pad.
   */
  def padCharToWiderLength(
      attr: Attribute,
      rawType: DataType,
      alwaysPad: Boolean = false): Option[(Expression, DataType)] = {
    getRawType(attr.metadata).flatMap { attrRawType =>
      widerCharType(attrRawType, rawType).filter(alwaysPad || _ != attrRawType).map { wider =>
        val padded = processStringForCharVarchar(attr, wider,
          charFuncName = Some(READ_SIDE_PADDING), varcharFuncName = None,
          keepFieldNullability = true)
        (padded, wider)
      }
    }
  }


  /**
   * The raw type of a value with the given metadata and data type. The raw type of a column is in
   * its metadata, and the raw types of fields of a struct built from columns, such as by
   * `named_struct`, are in the metadata of the fields. None if neither has a CHAR or VARCHAR type.
   */
  def getRawType(metadata: Metadata, dataType: DataType): Option[DataType] = {
    def nested(dt: DataType): Option[DataType] = dt match {
      case StructType(fields) =>
        val rawFields = fields.map(f => getRawType(f.metadata).orElse(nested(f.dataType)))
        if (rawFields.forall(_.isEmpty)) {
          None
        } else {
          Some(StructType(fields.zip(rawFields).map { case (f, raw) =>
            f.copy(dataType = raw.getOrElse(f.dataType), metadata = cleanMetadata(f.metadata))
          }))
        }
      case ArrayType(et, containsNull) => nested(et).map(ArrayType(_, containsNull))
      case _ => None
    }
    getRawType(metadata).orElse(nested(dataType))
  }

  /**
   * [[widerCharRawType]] for each position of the outputs of the children of a set operation.
   * Returns None for every position if the outputs have different lengths.
   */
  def widerCharRawTypes(childOutputs: Seq[Seq[Attribute]]): Seq[Option[DataType]] = {
    val outputs = childOutputs.map(_.toIndexedSeq).toIndexedSeq
    val width = outputs.head.length
    // A position needs a raw type in every child, so none does if the first child has none.
    if (outputs.exists(_.length != width) ||
        !outputs.head.exists(attr => getRawTypeString(attr.metadata).isDefined)) {
      Seq.fill(width)(None)
    } else {
      val parsed = mutable.HashMap.empty[String, DataType]
      def parse(rawType: String): DataType =
        parsed.getOrElseUpdate(rawType, CatalystSqlParser.parseDataType(rawType))
      (0 until width).map(i => widerCharRawType(outputs.map(_(i)), parse))
    }
  }

  /**
   * Narrows the CHAR columns and fields of the given expression, whose raw type is `fromType`, that
   * are longer than those at the same places of `toType`, by removing the trailing spaces past the
   * length of `toType`. Values with other characters past that length are kept, as read-side
   * padding keeps them. A view stores the CHAR type of its columns, which can be shorter than the
   * CHAR type of the same column of its query, such as for a view of a UNION created before the
   * union had the widest length of its children. With `padShorter`, it also pads the CHAR columns
   * and fields that are shorter than those of `toType` to them, as for the values of the recursive
   * term of a recursive CTE, which have the lengths of its anchor. Returns None if no column or
   * field has another length.
   */
  def narrowCharToLength(
      expr: Expression,
      fromType: DataType,
      toType: DataType,
      padShorter: Boolean = false): Option[Expression] = {
    narrowerCharType(fromType, toType, padShorter).map { narrower =>
      processStringForCharVarchar(expr, narrower,
        charFuncName = Some(CHAR_TYPE_NARROWING), varcharFuncName = None,
        keepFieldNullability = true)
    }.filterNot(_.fastEquals(expr))
  }

  /** Whether the expression narrows a value with [[narrowCharToLength]]. */
  def isNarrowing(e: Expression): Boolean = e.exists {
    case s: StaticInvoke =>
      s.functionName == CHAR_TYPE_NARROWING && s.staticObject == classOf[CharVarcharCodegenUtils]
    case _ => false
  }

  // The CHAR types of toType where fromType is longer, or with `padShorter` has another length,
  // and STRING elsewhere, with the field names of toType. None if they are not both CHAR types, or
  // structs and arrays of them, of one shape. Fields of other types must be the same.
  private def narrowerCharType(
      fromType: DataType,
      toType: DataType,
      padShorter: Boolean): Option[DataType] = {
    (fromType, toType) match {
      case (c1: CharType, c2: CharType) if c1.collationId == c2.collationId =>
        Some(if (c1.length > c2.length || padShorter && c1.length < c2.length) {
          c2
        } else {
          c2.toStringType
        })
      case (StructType(fields1), StructType(fields2)) if fields1.length == fields2.length =>
        val narrowerFields = fields1.zip(fields2).map { case (from, to) =>
          narrowerCharType(from.dataType, to.dataType, padShorter).map(dt => to.copy(dataType = dt))
        }
        if (narrowerFields.forall(_.isDefined)) {
          Some(StructType(narrowerFields.map(_.get)))
        } else {
          None
        }
      // The value narrowed is of fromType, so its elements are as nullable as there.
      case (ArrayType(et1, containsNull), ArrayType(et2, _)) =>
        narrowerCharType(et1, et2, padShorter).map(ArrayType(_, containsNull))
      // other fields of a struct, such as an INT next to a CHAR
      case _ if fromType == toType && !hasChar(toType) => Some(toType)
      case _ => None
    }
  }

  /**
   * The raw type of a value of the given raw type cast to `dataType`: the CHAR and VARCHAR types
   * of the raw type where the cast keeps a string of the same collation, and the types of
   * `dataType` elsewhere, with its field names. None if the types don't have the same shape.
   */
  def rawTypeOfCast(rawType: DataType, dataType: DataType): Option[DataType] = {
    (rawType, dataType) match {
      // A CHAR or VARCHAR type of its own, under first-class CHAR and VARCHAR types, is the type.
      case (_, t: CharType) => Some(t)
      case (_, t: VarcharType) => Some(t)
      case (c: CharType, s: StringType) if c.collationId == s.collationId => Some(c)
      case (v: VarcharType, s: StringType) if v.collationId == s.collationId => Some(v)
      case (StructType(rawFields), StructType(fields)) if rawFields.length == fields.length =>
        val castFields = rawFields.zip(fields).map { case (rawField, field) =>
          rawTypeOfCast(rawField.dataType, field.dataType).map(dt => field.copy(dataType = dt))
        }
        if (castFields.forall(_.isDefined)) Some(StructType(castFields.map(_.get))) else None
      case (ArrayType(rawElementType, _), ArrayType(elementType, containsNull)) =>
        rawTypeOfCast(rawElementType, elementType).map(ArrayType(_, containsNull))
      case (MapType(rawKeyType, rawValueType, _), MapType(keyType, valueType, valueContainsNull)) =>
        for {
          k <- rawTypeOfCast(rawKeyType, keyType)
          v <- rawTypeOfCast(rawValueType, valueType)
        } yield MapType(k, v, valueContainsNull)
      case (_: StructType | _: ArrayType | _: MapType, _) => None
      case (_, other) => Some(other)
    }
  }

  /**
   * The metadata of the alias of a value with the given metadata cast to `dataType`, such as a
   * widening cast of a child of a set operation, which an alias of a cast would otherwise lose: the
   * raw type of the cast value, see [[rawTypeOfCast]]. None if the cast keeps no CHAR or VARCHAR
   * type.
   */
  def metadataOfCast(metadata: Metadata, dataType: DataType): Option[Metadata] = {
    getRawType(metadata).flatMap(rawTypeOfCast(_, dataType)).filter(hasCharVarchar)
      .map(withRawType(Metadata.empty, _))
  }

  /**
   * For each position of the outputs of the children of a set operation cast to the given types
   * (None for no cast), whether a cast keeps the CHAR and VARCHAR types of the value, see
   * [[metadataOfCast]]. Only where each child has a raw type, and the raw types after the casts are
   * the same or can be widened to one, so that the set operation pads the children to the same
   * lengths; and only if `enabled`, as the set operation pads them, see SetOperationCharPadding.
   * Elsewhere, the output would keep the raw type of the first child for values that don't have it.
   */
  def keepsCharTypesInCast(
      childOutputs: Seq[Seq[Attribute]],
      targetTypes: Seq[Option[DataType]],
      enabled: Boolean): Seq[Boolean] = {
    if (!enabled) {
      targetTypes.map(_ => false)
    } else {
      val outputs = childOutputs.map(_.toIndexedSeq).toIndexedSeq
      targetTypes.zipWithIndex.map {
        // No cast at this position.
        case (None, _) => false
        case (Some(targetType), i) =>
          val attrs = outputs.map(_(i))
          attrs.forall(attr => getRawTypeString(attr.metadata).isDefined) && {
            val castTypes = attrs.map { attr =>
              getRawType(attr.metadata).flatMap(rawTypeOfCast(_, targetType))
            }
            castTypes.forall(_.isDefined) &&
              castTypes.flatten.exists(hasCharVarchar) &&
              // The same raw types need no padding; others need padding to the same lengths.
              (castTypes.flatten.map(normalizeCharType).distinct.length == 1 ||
                castTypes.flatten.map(Option(_)).reduce { (left, right) =>
                  for (l <- left; r <- right; wider <- widerCharType(l, r)) yield wider
                }.isDefined)
          }
      }
    }
  }

  /** The metadata with the given raw type, for a value padded to the CHAR lengths of that type. */
  def withRawType(metadata: Metadata, rawType: DataType): Metadata = {
    new MetadataBuilder()
      .withMetadata(metadata)
      .putString(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY, rawType.catalogString)
      .build()
  }

  private def hasChar(dt: DataType): Boolean = dt.existsRecursively(_.isInstanceOf[CharType])

  // Like typeWithWiderCharLength, but keeps the field names of type1, and None for types that are
  // not both CHAR types, or structs and arrays of them, with the same shape. Fields of other types
  // must be the same.
  private[sql] def widerCharType(type1: DataType, type2: DataType): Option[DataType] = {
    (type1, type2) match {
      // collation is None for the default collation, and collationId compares the effective one
      case (c1: CharType, c2: CharType) if c1.collationId == c2.collationId =>
        Some(CharType(math.max(c1.length, c2.length), c1.collation))
      case (StructType(fields1), StructType(fields2)) if fields1.length == fields2.length =>
        val widerFields = fields1.zip(fields2).map { case (left, right) =>
          widerCharType(left.dataType, right.dataType).map(dt => left.copy(dataType = dt))
        }
        if (widerFields.forall(_.isDefined)) Some(StructType(widerFields.map(_.get))) else None
      case (ArrayType(et1, containsNull1), ArrayType(et2, containsNull2)) =>
        widerCharType(et1, et2).map(ArrayType(_, containsNull1 || containsNull2))
      // other fields of a struct, such as an INT next to a CHAR
      case _ if type1 == type2 && !hasChar(type1) => Some(type1)
      case _ => None
    }
  }

  private def typeWithWiderCharLength(type1: DataType, type2: DataType): DataType = {
    (type1, type2) match {
      case (c1: CharType, c2: CharType) =>
        CharType(math.max(c1.length, c2.length), c1.collation)
      case (StructType(fields1), StructType(fields2)) =>
        assert(fields1.length == fields2.length)
        StructType(fields1.zip(fields2).map { case (left, right) =>
          StructField("", typeWithWiderCharLength(left.dataType, right.dataType))
        })
      case (ArrayType(et1, _), ArrayType(et2, _)) =>
        ArrayType(typeWithWiderCharLength(et1, et2))
      case _ => NullType
    }
  }

  private def padCharToTargetLength(
      expr: Expression,
      rawType: DataType,
      typeWithTargetCharLength: DataType,
      alwaysPad: Boolean,
      truncate: Boolean = true): Option[Expression] = {
    (rawType, typeWithTargetCharLength) match {
      case (c: CharType, t: CharType) if alwaysPad || t.length > c.length =>
        Some(if (truncate) {
          createStringRPad(expr, Literal(t.length))
        } else {
          createReadSidePadding(expr, CharType(t.length, c.collation))
        })

      case (StructType(fields), StructType(targets)) =>
        assert(fields.length == targets.length)
        var i = 0
        var needPadding = false
        val createStructExprs = mutable.ArrayBuffer.empty[Expression]
        while (i < fields.length) {
          val field = fields(i)
          val fieldExpr = GetStructField(expr, i, Some(field.name))
          val padded = padCharToTargetLength(
            fieldExpr, field.dataType, targets(i).dataType, alwaysPad, truncate)
          // Accumulate: any single field needing padding means the struct must be rebuilt,
          // otherwise the padding computed for earlier fields would be discarded.
          needPadding |= padded.isDefined
          createStructExprs += Literal(field.name)
          createStructExprs += padded.getOrElse(fieldExpr)
          i += 1
        }
        if (needPadding) {
          val struct = CreateNamedStruct(createStructExprs.toSeq)
          // Rebuilding the struct field by field loses the nullability of the struct itself:
          // GetStructField on a NULL struct yields NULL fields, which CreateNamedStruct would
          // turn into a non-NULL struct of NULLs. Guard it the same way the scan-side rewrite
          // in processStringForCharVarchar does.
          Some(nullSafeRebuiltStruct(expr, struct))
        } else {
          None
        }

      case (ArrayType(et, containsNull), ArrayType(target, _)) =>
        val param = NamedLambdaVariable("x", replaceCharVarcharWithString(et), containsNull)
        padCharToTargetLength(param, et, target, alwaysPad, truncate).map { padded =>
          val func = LambdaFunction(padded, Seq(param))
          ArrayTransform(expr, func)
        }

      // We don't handle MapType here as it's not comparable.

      case _ => None
    }
  }
}
