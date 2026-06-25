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

import java.io.IOException
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.sketch.SketchEnvelopeProtos.SketchMetadata
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{BinaryType, DataType, IntegerType, LongType, StringType}

/**
 * How the bytes that were hashed/fed into the sketch were derived from the logical input value.
 * This is not persisted; it is derived from the collation ID recorded in the envelope at the time
 * a compatibility check is performed (see [[SketchEnvelope.keyEncodingForCollation]]).
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

  def name(enc: Byte): String = enc match {
    case NUMERIC_OR_BINARY => "numeric_or_binary"
    case RAW_BYTES => "raw_bytes"
    case ICU_SORTKEY => "icu_sortkey"
    case UTF8_LCASE_CPS => "utf8_lcase_cps"
    case _ => "unknown"
  }

  /** Whether this encoding depends on string collation semantics. */
  def isStringy(enc: Byte): Boolean =
    enc == RAW_BYTES || enc == ICU_SORTKEY || enc == UTF8_LCASE_CPS

  /** Whether this encoding depends on the ICU library version. */
  def usesIcu(enc: Byte): Boolean =
    enc == ICU_SORTKEY || enc == UTF8_LCASE_CPS
}

/**
 * A small self-identifying envelope wrapped around the binary payload that the DataSketches
 * aggregates emit into `BinaryType` columns. It records provenance (ICU version, collation ID and
 * the DataSketches library version) so that incompatible sketches can be detected at
 * union/intersection/merge time instead of silently producing wrong answers.
 *
 * On-the-wire layout, followed by the unchanged native sketch payload:
 * {{{
 *   offset size field
 *      0     1   magic = 0xFF (marks our encoding, never a valid DataSketches preamble byte 0)
 *      1     4   proto_length (big-endian int32, length of the serialized SketchMetadata)
 *      5    proto_length   serialized SketchMetadata protobuf (see sketch_envelope.proto)
 *      ...  ...  native sketch payload (unchanged Apache DataSketches bytes)
 * }}}
 *
 * A native DataSketches preamble never begins with 0xFF, and the `proto_length` cross-check (it
 * must be non-negative and fit within the buffer) is a second, independent guard. Buffers that
 * fail either check are treated as legacy (un-enveloped) sketches and passed through unchanged, so
 * already-materialized sketches keep working without a rewrite.
 */
object SketchEnvelope extends Logging {

  /** First byte of an envelope; chosen because no native DataSketches preamble starts with it. */
  private val MAGIC: Byte = 0xFF.toByte

  /** Size of the fixed header: 1 magic byte + a 4-byte big-endian protobuf length. */
  private val HEADER_LEN: Int = 5

  /** Sentinel used in the `collation_id` slot when collation does not apply. */
  val NO_COLLATION_ID: Int = -1

  def hasEnvelope(b: Array[Byte]): Boolean = {
    if (b == null || b.length < HEADER_LEN || b(0) != MAGIC) {
      false
    } else {
      val protoLen = ByteBuffer.wrap(b, 1, 4).order(ByteOrder.BIG_ENDIAN).getInt
      protoLen >= 0 && HEADER_LEN.toLong + protoLen <= b.length
    }
  }

  /** Wrap a native sketch payload with the envelope describing the given metadata. */
  def wrap(payload: Array[Byte], meta: SketchMetadata): Array[Byte] = {
    val proto = meta.toByteArray
    val out = new Array[Byte](HEADER_LEN + proto.length + payload.length)
    val bb = ByteBuffer.wrap(out).order(ByteOrder.BIG_ENDIAN)
    bb.put(MAGIC)
    bb.putInt(proto.length)
    bb.put(proto)
    bb.put(payload)
    out
  }

  /**
   * Unwrap a buffer. If it carries a valid envelope, returns its metadata and the inner payload.
   * Otherwise the buffer is a legacy sketch and is returned unchanged with no metadata. A buffer
   * that passes the framing checks but whose protobuf fails to parse is also treated as legacy.
   */
  def unwrap(b: Array[Byte]): (Option[SketchMetadata], Array[Byte]) = {
    if (!hasEnvelope(b)) {
      (None, b)
    } else {
      val protoLen = ByteBuffer.wrap(b, 1, 4).order(ByteOrder.BIG_ENDIAN).getInt
      try {
        val meta = SketchMetadata.parseFrom(b.slice(HEADER_LEN, HEADER_LEN + protoLen))
        (Some(meta), b.slice(HEADER_LEN + protoLen, b.length))
      } catch {
        // InvalidProtocolBufferException extends IOException; if the bytes are not a real
        // envelope we conservatively fall back to treating the buffer as a legacy sketch.
        case _: IOException => (None, b)
      }
    }
  }

  /** Returns the native payload of a buffer, stripping the envelope if present. */
  def payloadOf(b: Array[Byte]): Array[Byte] = unwrap(b)._2

  /** Returns the recorded metadata of a buffer, or None for legacy (un-enveloped) buffers. */
  def metadataOf(b: Array[Byte]): Option[SketchMetadata] = unwrap(b)._1

  /**
   * Validate that an observed input's metadata is compatible with the runtime metadata of the
   * engine consuming it, per the compatibility policy. Throws a `SKETCH_*` error on a hard mismatch
   * unless `spark.sql.sketch.allowVersionMismatch` is set, in which case the mismatch is logged.
   *
   * The key encoding is derived from the recorded collation ID at check time, so two sketches that
   * encoded their keys differently (for example one numeric and one ICU-collated) are rejected.
   */
  def assertCompatible(
      observed: SketchMetadata,
      runtime: SketchMetadata,
      prettyName: String,
      allowMismatch: Boolean): Unit = {
    def fail(error: => Throwable): Unit = {
      if (allowMismatch) {
        logWarning(s"Ignoring incompatible sketch metadata for $prettyName because " +
          "spark.sql.sketch.allowVersionMismatch is enabled.")
      } else {
        throw error
      }
    }

    val observedEncoding = keyEncodingForCollation(observed.getCollationId)
    val runtimeEncoding = keyEncodingForCollation(runtime.getCollationId)

    if (observedEncoding != runtimeEncoding) {
      fail(QueryExecutionErrors.sketchKeyEncodingMismatch(
        prettyName,
        KeyEncoding.name(observedEncoding),
        KeyEncoding.name(runtimeEncoding)))
    } else if (KeyEncoding.isStringy(observedEncoding) &&
        observed.getCollationId != runtime.getCollationId) {
      fail(QueryExecutionErrors.sketchCollationMismatch(
        prettyName, observed.getCollationId, runtime.getCollationId))
    } else if (KeyEncoding.usesIcu(observedEncoding) &&
        (observed.getIcuMajor != runtime.getIcuMajor ||
          observed.getIcuMinor != runtime.getIcuMinor)) {
      fail(QueryExecutionErrors.sketchIcuVersionMismatch(
        prettyName, icuVersionString(observed), icuVersionString(runtime)))
    }
  }

  /**
   * Derive the key encoding for a recorded collation ID. Numeric/binary sketches record
   * [[NO_COLLATION_ID]]; otherwise the encoding follows from the collation semantics.
   */
  def keyEncodingForCollation(collationId: Int): Byte = {
    if (collationId == NO_COLLATION_ID) {
      KeyEncoding.NUMERIC_OR_BINARY
    } else if (collationId == CollationFactory.UTF8_BINARY_COLLATION_ID) {
      KeyEncoding.RAW_BYTES
    } else if (CollationFactory.fetchCollation(collationId)
        .collationName.startsWith("UTF8_LCASE")) {
      KeyEncoding.UTF8_LCASE_CPS
    } else {
      KeyEncoding.ICU_SORTKEY
    }
  }

  /**
   * Build the metadata describing how the current runtime would encode values of `dataType` into a
   * sketch, reading the ICU version from the relevant collation.
   */
  def currentMetadata(dataType: DataType): SketchMetadata = {
    val (collationId, icuMajor, icuMinor) = dataType match {
      case st: StringType =>
        val collation = CollationFactory.fetchCollation(st.collationId)
        val (major, minor) = parseIcuVersion(collation.version)
        (st.collationId, major, minor)
      case _: IntegerType | _: LongType | _: BinaryType =>
        (NO_COLLATION_ID, 0, 0)
      case _ =>
        (NO_COLLATION_ID, 0, 0)
    }
    SketchMetadata.newBuilder()
      .setDatasketchesMajor(DATASKETCHES_LIB_MAJOR)
      .setDatasketchesMinor(DATASKETCHES_LIB_MINOR)
      .setCollationId(collationId)
      .setIcuMajor(icuMajor)
      .setIcuMinor(icuMinor)
      .build()
  }

  /** Human-readable ICU version recorded in the metadata, or null when ICU does not apply. */
  def icuVersionString(meta: SketchMetadata): String =
    if (meta.getIcuMajor == 0 && meta.getIcuMinor == 0) {
      null
    } else {
      s"${meta.getIcuMajor}.${meta.getIcuMinor}"
    }

  /** Human-readable DataSketches library version recorded in the metadata. */
  def datasketchesVersionString(meta: SketchMetadata): String =
    s"${meta.getDatasketchesMajor}.${meta.getDatasketchesMinor}"

  // Version of the bundled Apache DataSketches Java library (see dev/deps).
  private[spark] val DATASKETCHES_LIB_MAJOR: Int = 6
  private[spark] val DATASKETCHES_LIB_MINOR: Int = 2

  private def parseIcuVersion(version: String): (Int, Int) = {
    if (version == null) {
      (0, 0)
    } else {
      // CollationFactory exposes "1.0" for UTF8_BINARY and the ICU library version (e.g. "75.1")
      // for ICU-family collations and UTF8_LCASE.
      val parts = version.split("\\.")
      val major = if (parts.length > 0) parseIntOr0(parts(0)) else 0
      val minor = if (parts.length > 1) parseIntOr0(parts(1)) else 0
      (major, minor)
    }
  }

  private def parseIntOr0(s: String): Int = {
    try {
      s.trim.toInt
    } catch {
      case _: NumberFormatException => 0
    }
  }
}
