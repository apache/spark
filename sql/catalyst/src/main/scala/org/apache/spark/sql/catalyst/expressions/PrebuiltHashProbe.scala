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

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

/**
 * Shared machinery for a constant, driver-built key -> index lookup that is probed inline in
 * generated code with no autoboxing. A power-of-two open-addressed `int[]` bucket table maps
 * `hash(key) & hashMask` to the key's index in a constant key `ArrayData`; the probe returns the
 * matching index (or -1). Callers embed the key/value arrays via [[CodegenContext.addReferenceObj]]
 * and decide what to do with the returned index (a map lookup returns the value / null; a
 * CaseWhen lookup returns the branch value / else).
 *
 * The two hash implementations - [[hashOnDriver]] (Scala, used to build the buckets) and
 * [[genHash]] (Java, emitted into the probe) - MUST stay bit-identical for the same `keyType`;
 * keeping them adjacent here is the point of this object. Extracted from
 * `GetMapValueUtil.PrebuiltHashExecutor` (SPARK-55959) so both it and `CaseWhen` share one copy.
 */
private[expressions] object PrebuiltHashProbe {

  /**
   * Builds a power-of-two open-addressed bucket table mapping `hash(key) & hashMask` to the key's
   * index in `keys`. Load factor < 0.5, min capacity 4, clamped to 2^30 so `cap - 1` fits in int.
   * Duplicates take the next free slot, so the probe (which stops at the first match) returns the
   * first-inserted index -- first-wins semantics. The hash must match [[genHash]] for `keyType`.
   */
  def buildBuckets(keys: ArrayData, keyType: DataType): (Array[Int], Int) = {
    val len = keys.numElements()
    val target = math.min(math.max(len.toLong * 2L - 1L, 1L), (1L << 30) - 1L).toInt
    val cap = math.max(java.lang.Integer.highestOneBit(target) << 1, 4)
    val buckets = new Array[Int](cap)
    java.util.Arrays.fill(buckets, -1)
    val mask = cap - 1
    var i = 0
    while (i < len) {
      var h = hashOnDriver(keys.get(i, keyType), keyType) & mask
      while (buckets(h) != -1) h = (h + 1) & mask
      buckets(h) = i
      i += 1
    }
    (buckets, mask)
  }

  /** Scala-side hash for a Spark value; mirrors [[genHash]] per keyType. */
  def hashOnDriver(v: Any, keyType: DataType): Int = keyType match {
    case BooleanType => if (v.asInstanceOf[Boolean]) 1 else 0
    case ByteType => v.asInstanceOf[Byte].toInt
    case ShortType => v.asInstanceOf[Short].toInt
    case IntegerType | DateType | _: YearMonthIntervalType => v.asInstanceOf[Int]
    case LongType | TimestampType | TimestampNTZType | _: DayTimeIntervalType | _: TimeType =>
      val l = v.asInstanceOf[Long]
      (l ^ (l >>> 32)).toInt
    case FloatType => java.lang.Float.floatToIntBits(v.asInstanceOf[Float])
    case DoubleType =>
      val l = java.lang.Double.doubleToLongBits(v.asInstanceOf[Double])
      (l ^ (l >>> 32)).toInt
    case _ => v.hashCode()
  }

  /** Java-side hash expression over the primitive/object `v`. Mirrors [[hashOnDriver]]. */
  def genHash(v: String, keyType: DataType): String = keyType match {
    case BooleanType => s"($v ? 1 : 0)"
    case ByteType | ShortType | IntegerType | DateType | _: YearMonthIntervalType => s"$v"
    case LongType | TimestampType | TimestampNTZType | _: DayTimeIntervalType | _: TimeType =>
      s"(int)($v ^ ($v >>> 32))"
    case FloatType => s"Float.floatToIntBits($v)"
    case DoubleType =>
      s"(int)(Double.doubleToLongBits($v) ^ (Double.doubleToLongBits($v) >>> 32))"
    case _ => s"$v.hashCode()"
  }

  /**
   * Emits the inline open-addressing probe loop. `resultIdx` must already be declared by the
   * caller (typically `int resultIdx = -1;`) so it stays in scope after the loop; the loop sets it
   * to the matching key's index or leaves it unchanged on a miss. `keyVal` is the (non-null)
   * runtime key value expression; `bucketsRef` / `keysRef` are the reference-array accessors for
   * the `int[]` buckets and the key `ArrayData`. Equality uses `ctx.genEqual`, so it is correct
   * for the eligible key types (primitives, binary-collation strings, decimals).
   */
  def genFindIndex(
      ctx: CodegenContext,
      keyType: DataType,
      keyVal: String,
      bucketsRef: String,
      keysRef: String,
      hashMask: Int,
      resultIdx: String): String = {
    val h = ctx.freshName("h")
    val idx = ctx.freshName("idx")
    val candidate = ctx.freshName("candidate")
    val keyJavaType = CodeGenerator.javaType(keyType)
    s"""
       |int $h = (${genHash(keyVal, keyType)}) & $hashMask;
       |while ($bucketsRef[$h] != -1) {
       |  int $idx = $bucketsRef[$h];
       |  $keyJavaType $candidate = ${CodeGenerator.getValue(keysRef, keyType, idx)};
       |  if (${ctx.genEqual(keyType, candidate, keyVal)}) {
       |    $resultIdx = $idx;
       |    break;
       |  }
       |  $h = ($h + 1) & $hashMask;
       |}
     """.stripMargin
  }
}
