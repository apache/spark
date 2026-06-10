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

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{BinaryType, DataType, IntegerType, LongType, StringType}

/**
 * The kind of Apache DataSketches sketch carried by an envelope. The value is persisted, so
 * existing entries must never be renumbered; only append new values.
 */
object SketchKind {
  final val HLL: Byte = 0
  final val THETA: Byte = 1
  final val TUPLE_DOUBLE: Byte = 2
  final val TUPLE_INTEGER: Byte = 3
  final val KLL_LONG: Byte = 4
  final val KLL_FLOAT: Byte = 5
  final val KLL_DOUBLE: Byte = 6
  final val ITEMS: Byte = 7
  final val FREQUENCY: Byte = 8

  def name(kind: Byte): String = kind match {
    case HLL => "HLL"
    case THETA => "Theta"
    case TUPLE_DOUBLE => "TupleDouble"
    case TUPLE_INTEGER => "TupleInteger"
    case KLL_LONG => "KllLong"
    case KLL_FLOAT => "KllFloat"
    case KLL_DOUBLE => "KllDouble"
    case ITEMS => "Items"
    case FREQUENCY => "Frequency"
    case _ => "Unknown"
  }
}

/**
 * How the bytes that were hashed/fed into the sketch were derived from the logical input value.
 * Persisted; only append new values.
 */
object KeyEncoding {
  // Numeric or binary value updated directly into the sketch (no collation involved).
  final val NUMERIC_OR_BINARY: Byte = 0
  // Raw UTF-8 bytes of the string (UTF8_BINARY collation).
  final val RAW_BYTES: Byte = 1
  // ICU collation sort key (collation.sortKeyFunction for an ICU-family collation).
  final val ICU_SORTKEY: Byte = 2
  // UTF8_LCASE code points (collation.sortKeyFunction for UTF8_LCASE).
  final val UTF8_LCASE_CPS: Byte = 3
  // ICU CollationKey string (CollationFactory.getCollationKey, used by approx_top_k).
  final val ICU_COLLATION_KEY_STRING: Byte = 4

  def name(enc: Byte): String = enc match {
    case NUMERIC_OR_BINARY => "numeric_or_binary"
    case RAW_BYTES => "raw_bytes"
    case ICU_SORTKEY => "icu_sortkey"
    case UTF8_LCASE_CPS => "utf8_lcase_cps"
    case ICU_COLLATION_KEY_STRING => "icu_collation_key_string"
    case _ => "unknown"
  }

  /** Whether this encoding depends on string collation semantics. */
  def isStringy(enc: Byte): Boolean =
    enc == RAW_BYTES || enc == ICU_SORTKEY || enc == UTF8_LCASE_CPS ||
      enc == ICU_COLLATION_KEY_STRING

  /** Whether this encoding depends on the ICU library version. */
  def usesIcu(enc: Byte): Boolean =
    enc == ICU_SORTKEY || enc == ICU_COLLATION_KEY_STRING || enc == UTF8_LCASE_CPS
}

/** Origin engine that produced the sketch. Reserved for future non-JVM engines. */
object EngineOrigin {
  final val JVM_SPARK: Byte = 0
}

/**
 * Provenance recorded alongside a materialized DataSketches buffer that the native preamble does
 * not capture. See [[SketchEnvelope]] for the on-the-wire layout.
 */
case class SketchProfile(
    sketchKind: Byte,
    keyEncoding: Byte,
    engineOrigin: Byte,
    collationId: Int,
    icuMajor: Short,
    icuMinor: Short,
    dsLibMajor: Short,
    dsLibMinor: Short,
    sparkCollationRev: Short,
    featureFlags: Byte) {

  def icuVersionString: String =
    if (icuMajor == 0 && icuMinor == 0) null else s"$icuMajor.$icuMinor"

  def datasketchesVersionString: String = s"$dsLibMajor.$dsLibMinor"

  def engineName: String = engineOrigin match {
    case EngineOrigin.JVM_SPARK => "jvm_spark"
    case _ => "unknown"
  }
}

/**
 * A small self-identifying envelope wrapped around the binary payload that the DataSketches
 * aggregates emit into `BinaryType` columns. It records provenance (ICU version, collation id,
 * key encoding, datasketches lib version, and a manually bumped Spark collation revision) so that
 * incompatible sketches can be detected at union/intersection/merge time instead of silently
 * producing wrong answers.
 *
 * Layout (28 bytes, little-endian), followed by the unchanged native sketch payload:
 * {{{
 *   offset size field
 *      0    4   magic = 0xDB,0x53,0x4B,0x01
 *      4    1   envelope_version (== 1)
 *      5    1   sketch_kind
 *      6    1   key_encoding
 *      7    1   engine_origin
 *      8    4   collation_id (0xFFFFFFFF if not applicable)
 *     12    2   icu_major
 *     14    2   icu_minor
 *     16    2   datasketches_lib_major
 *     18    2   datasketches_lib_minor
 *     20    2   spark_collation_rev
 *     22    1   feature_flags
 *     23    1   reserved (== 0)
 *     24    4   payload_length
 *     28   ...  native sketch payload (unchanged Apache DataSketches bytes)
 * }}}
 *
 * Byte index 2 of the magic is fixed to 0x4B, which can never be a valid DataSketches `FamilyId`
 * (1..14), so the magic can never collide with a native preamble. The `payload_length` check is a
 * second, independent guard. Buffers that fail either check are treated as legacy "v0" sketches
 * and passed through unchanged, so already-materialized sketches keep working without a rewrite.
 */
object SketchEnvelope extends Logging {

  private val MAGIC: Array[Byte] = Array(0xDB.toByte, 0x53.toByte, 0x4B.toByte, 0x01.toByte)

  val HEADER_LEN: Int = 28

  /** Byte offset of the little-endian payload_length field within the header. */
  private val PAYLOAD_LEN_OFFSET: Int = 24

  val ENVELOPE_VERSION: Byte = 1

  /** Sentinel used in the `collation_id` slot when collation does not apply. */
  val NO_COLLATION_ID: Int = 0xFFFFFFFF

  def hasEnvelope(b: Array[Byte]): Boolean = {
    b != null &&
      b.length >= HEADER_LEN &&
      b(0) == MAGIC(0) &&
      b(1) == MAGIC(1) &&
      b(2) == MAGIC(2) &&
      b(3) == MAGIC(3) &&
      ByteBuffer.wrap(b, PAYLOAD_LEN_OFFSET, 4).order(ByteOrder.LITTLE_ENDIAN)
        .getInt == b.length - HEADER_LEN
  }

  /** Wrap a native sketch payload with the envelope describing the given profile. */
  def wrap(payload: Array[Byte], p: SketchProfile): Array[Byte] = {
    val out = new Array[Byte](HEADER_LEN + payload.length)
    val bb = ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN)
    bb.put(MAGIC)
    bb.put(ENVELOPE_VERSION)
    bb.put(p.sketchKind)
    bb.put(p.keyEncoding)
    bb.put(p.engineOrigin)
    bb.putInt(p.collationId)
    bb.putShort(p.icuMajor)
    bb.putShort(p.icuMinor)
    bb.putShort(p.dsLibMajor)
    bb.putShort(p.dsLibMinor)
    bb.putShort(p.sparkCollationRev)
    bb.put(p.featureFlags)
    bb.put(0.toByte)
    bb.putInt(payload.length)
    bb.put(payload)
    out
  }

  /**
   * Unwrap a buffer. If it carries a valid envelope, returns its profile and the inner payload.
   * Otherwise the buffer is a legacy "v0" sketch and is returned unchanged with no profile.
   */
  def unwrap(b: Array[Byte]): (Option[SketchProfile], Array[Byte]) = {
    if (!hasEnvelope(b)) {
      (None, b)
    } else {
      val bb = ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN)
      bb.position(4)
      bb.get() // envelope_version
      val p = SketchProfile(
        sketchKind = bb.get(),
        keyEncoding = bb.get(),
        engineOrigin = bb.get(),
        collationId = bb.getInt,
        icuMajor = bb.getShort,
        icuMinor = bb.getShort,
        dsLibMajor = bb.getShort,
        dsLibMinor = bb.getShort,
        sparkCollationRev = bb.getShort,
        featureFlags = bb.get())
      bb.get() // reserved
      val payloadLen = bb.getInt
      val payload = new Array[Byte](payloadLen)
      bb.get(payload)
      (Some(p), payload)
    }
  }

  /** Returns the native payload of a buffer, stripping the envelope if present. */
  def payloadOf(b: Array[Byte]): Array[Byte] = unwrap(b)._2

  /** Returns the provenance profile of a buffer, or None for legacy (un-enveloped) buffers. */
  def profileOf(b: Array[Byte]): Option[SketchProfile] = unwrap(b)._1

  /**
   * Validate that an observed input profile is compatible with the runtime profile of the engine
   * consuming it, per the compatibility policy. Throws a `SKETCH_*` error on a hard mismatch
   * unless `spark.sql.sketch.allowVersionMismatch` is set, in which case the mismatch is logged.
   * Soft mismatches (e.g. only the collation factory revision differs) are always logged.
   */
  def assertCompatible(
      observed: SketchProfile,
      runtime: SketchProfile,
      prettyName: String,
      allowMismatch: Boolean): Unit = {
    def fail(error: => Throwable): Unit = {
      if (allowMismatch) {
        logWarning(s"Ignoring incompatible sketch profile for $prettyName because " +
          "spark.sql.sketch.allowVersionMismatch is enabled.")
      } else {
        throw error
      }
    }

    if (observed.keyEncoding != runtime.keyEncoding) {
      fail(QueryExecutionErrors.sketchKeyEncodingMismatch(
        prettyName,
        KeyEncoding.name(observed.keyEncoding),
        KeyEncoding.name(runtime.keyEncoding)))
    } else if (KeyEncoding.isStringy(observed.keyEncoding) &&
        observed.collationId != runtime.collationId) {
      fail(QueryExecutionErrors.sketchCollationMismatch(
        prettyName, observed.collationId, runtime.collationId))
    } else if (KeyEncoding.usesIcu(observed.keyEncoding) &&
        (observed.icuMajor != runtime.icuMajor || observed.icuMinor != runtime.icuMinor)) {
      fail(QueryExecutionErrors.sketchIcuVersionMismatch(
        prettyName, observed.icuVersionString, runtime.icuVersionString))
    } else if (observed.sparkCollationRev != runtime.sparkCollationRev) {
      logWarning(s"Sketch input for $prettyName was built under a different Spark collation " +
        "factory revision; results may be approximate.")
    }
  }

  /**
   * Build the profile describing how the current runtime would encode values of `dataType` into a
   * sketch of the given kind, reading the ICU version from the relevant collation.
   */
  def currentProfile(sketchKind: Byte, dataType: DataType): SketchProfile = {
    val (keyEncoding, collationId, icuMajor, icuMinor) = dataType match {
      case st: StringType =>
        val collation = CollationFactory.fetchCollation(st.collationId)
        val (major, minor) = parseIcuVersion(collation.version)
        val encoding =
          if (st.collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) KeyEncoding.RAW_BYTES
          else if (collation.collationName.startsWith("UTF8_LCASE")) KeyEncoding.UTF8_LCASE_CPS
          else KeyEncoding.ICU_SORTKEY
        (encoding, st.collationId, major, minor)
      case _: IntegerType | _: LongType | _: BinaryType =>
        (KeyEncoding.NUMERIC_OR_BINARY, NO_COLLATION_ID, 0.toShort, 0.toShort)
      case _ =>
        (KeyEncoding.NUMERIC_OR_BINARY, NO_COLLATION_ID, 0.toShort, 0.toShort)
    }
    SketchProfile(
      sketchKind = sketchKind,
      keyEncoding = keyEncoding,
      engineOrigin = EngineOrigin.JVM_SPARK,
      collationId = collationId,
      icuMajor = icuMajor,
      icuMinor = icuMinor,
      dsLibMajor = DATASKETCHES_LIB_MAJOR,
      dsLibMinor = DATASKETCHES_LIB_MINOR,
      sparkCollationRev = SPARK_COLLATION_FACTORY_REVISION,
      featureFlags = 0.toByte)
  }

  /**
   * Variant for `approx_top_k`, which keys strings through
   * `CollationFactory.getCollationKey` rather than `sortKeyFunction`.
   */
  def currentItemsProfile(dataType: DataType): SketchProfile = {
    dataType match {
      case st: StringType =>
        val collation = CollationFactory.fetchCollation(st.collationId)
        val (major, minor) = parseIcuVersion(collation.version)
        SketchProfile(
          sketchKind = SketchKind.ITEMS,
          keyEncoding = KeyEncoding.ICU_COLLATION_KEY_STRING,
          engineOrigin = EngineOrigin.JVM_SPARK,
          collationId = st.collationId,
          icuMajor = major,
          icuMinor = minor,
          dsLibMajor = DATASKETCHES_LIB_MAJOR,
          dsLibMinor = DATASKETCHES_LIB_MINOR,
          sparkCollationRev = SPARK_COLLATION_FACTORY_REVISION,
          featureFlags = 0.toByte)
      case other => currentProfile(SketchKind.ITEMS, other)
    }
  }

  // Version of the bundled Apache DataSketches Java library (see dev/deps).
  private[spark] val DATASKETCHES_LIB_MAJOR: Short = 6
  private[spark] val DATASKETCHES_LIB_MINOR: Short = 2

  // Bump this manually whenever the semantics of CollationFactory.sortKeyFunction /
  // getCollationKey change in a way that alters the bytes fed into a sketch.
  private[spark] val SPARK_COLLATION_FACTORY_REVISION: Short = 1

  private def parseIcuVersion(version: String): (Short, Short) = {
    if (version == null) {
      (0.toShort, 0.toShort)
    } else {
      // CollationFactory exposes "1.0" for UTF8_BINARY and the ICU library version (e.g. "75.1")
      // for ICU-family collations and UTF8_LCASE.
      val parts = version.split("\\.")
      val major = if (parts.length > 0) parseShortOr0(parts(0)) else 0.toShort
      val minor = if (parts.length > 1) parseShortOr0(parts(1)) else 0.toShort
      (major, minor)
    }
  }

  private def parseShortOr0(s: String): Short = {
    try {
      s.trim.toShort
    } catch {
      case _: NumberFormatException => 0.toShort
    }
  }
}
