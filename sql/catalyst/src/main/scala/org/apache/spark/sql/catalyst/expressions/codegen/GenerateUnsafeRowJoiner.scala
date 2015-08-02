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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.catalyst.expressions.{UnsafeRow, Attribute}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.PlatformDependent


abstract class UnsafeRowJoiner {
  def join(row1: UnsafeRow, row2: UnsafeRow): UnsafeRow
}


/**
 * A code generator for concatenating two [[UnsafeRow]]s into a single [[UnsafeRow]].
 *
 * The high level algorithm is:
 *
 * 1. Concatenate the two bitsets together into a single one, taking padding into account.
 * 2. Move fixed-length data.
 * 3. Move variable-length data.
 * 4. Update the offset position (i.e. the upper 32 bits in the fixed length part) for all
 *    variable-length data.
 */
object GenerateUnsafeRowJoiner extends CodeGenerator[(StructType, StructType), UnsafeRowJoiner] {

  def dump(word: Long): String = {
    Seq.tabulate(64) { i => if ((word >> i) % 2 == 0) "0" else "1" }.reverse.mkString
  }

  override protected def create(in: (StructType, StructType)): UnsafeRowJoiner = {
    create(in._1, in._2)
  }

  override protected def canonicalize(in: (StructType, StructType)): (StructType, StructType) = in

  override protected def bind(in: (StructType, StructType), inputSchema: Seq[Attribute])
    : (StructType, StructType) = {
    in
  }

  def create(schema1: StructType, schema2: StructType): UnsafeRowJoiner = {
    val ctx = newCodeGenContext()
    val offset = PlatformDependent.BYTE_ARRAY_OFFSET

    val bitset1Words = (schema1.size + 63) / 64
    val bitset2Words = (schema2.size + 63) / 64
    val outputBitsetWords = (schema1.size + schema2.size + 63) / 64
    val bitset1Remainder = schema1.size % 64
    val bitset2Remainder = schema2.size % 64

    // The number of words we can reduce when we concat two rows together.
    // The only reduction comes from merging the bitset portion of the two rows, saving 1 word.
    val sizeReduction = bitset1Words + bitset2Words - outputBitsetWords

    // --------------------- copy bitset from row 1 ----------------------- //
    val copyBitset1 = Seq.tabulate(bitset1Words) { i =>
      s"""
         |PlatformDependent.UNSAFE.putLong(buf, ${offset + i * 8},
         |  PlatformDependent.UNSAFE.getLong(obj1, ${offset + i * 8}));
       """.stripMargin
    }.mkString


    // --------------------- copy bitset from row 2 ----------------------- //
    var copyBitset2 = ""
    if (bitset1Remainder == 0) {
      copyBitset2 += Seq.tabulate(bitset2Words) { i =>
        s"""
           |PlatformDependent.UNSAFE.putLong(buf, ${offset + (bitset1Words + i) * 8},
           |  PlatformDependent.UNSAFE.getLong(obj2, ${offset + i * 8}));
         """.stripMargin
      }.mkString
    } else {
      copyBitset2 = Seq.tabulate(bitset2Words) { i =>
        s"""
           |long bs2w$i = PlatformDependent.UNSAFE.getLong(obj2, ${offset + i * 8});
           |long bs2w${i}p1 = (bs2w$i << $bitset1Remainder) & ~((1L << $bitset1Remainder) - 1);
           |long bs2w${i}p2 = (bs2w$i >>> ${64 - bitset1Remainder});
         """.stripMargin
      }.mkString

      copyBitset2 += Seq.tabulate(bitset2Words) { i =>
        val currentOffset = offset + (bitset1Words + i - 1) * 8
        if (i == 0) {
          if (bitset1Words > 0) {
            s"""
               |PlatformDependent.UNSAFE.putLong(buf, $currentOffset,
               |  bs2w${i}p1 | PlatformDependent.UNSAFE.getLong(obj1, $currentOffset));
            """.stripMargin
          } else {
            s"""
               |PlatformDependent.UNSAFE.putLong(buf, $currentOffset + 8, bs2w${i}p1);
            """.stripMargin
          }
        } else {
          s"""
             |PlatformDependent.UNSAFE.putLong(buf, $currentOffset, bs2w${i}p1 | bs2w${i - 1}p2);
          """.stripMargin
        }
      }.mkString("\n")

      if (bitset2Words > 0 &&
        (bitset2Remainder == 0 || bitset2Remainder > (64 - bitset1Remainder))) {
        val lastWord = bitset2Words - 1
        copyBitset2 +=
          s"""
             |PlatformDependent.UNSAFE.putLong(buf, ${offset + (outputBitsetWords - 1) * 8},
             |  bs2w${lastWord}p2);
          """.stripMargin
      }
    }

    // --------------------- copy fixed length portion from row 1 ----------------------- //
    var cursor = offset + outputBitsetWords * 8
    val copyFixedLengthRow1 = s"""
       |// Copy fixed length data for row1
       |PlatformDependent.copyMemory(
       |  obj1, offset1 + ${bitset1Words * 8},
       |  buf, $cursor,
       |  ${schema1.size * 8});
     """.stripMargin
    cursor += schema1.size * 8

    // --------------------- copy fixed length portion from row 2 ----------------------- //
    val copyFixedLengthRow2 = s"""
       |// Copy fixed length data for row2
       |PlatformDependent.copyMemory(
       |  obj2, offset2 + ${bitset2Words * 8},
       |  buf, $cursor,
       |  ${schema2.size * 8});
     """.stripMargin
    cursor += schema2.size * 8

    // --------------------- copy variable length portion from row 1 ----------------------- //
    val copyVariableLengthRow1 = s"""
       |// Copy variable length data for row1
       |long numBytesBitsetAndFixedRow1 = ${(bitset1Words + schema1.size) * 8};
       |long numBytesVariableRow1 = row1.getSizeInBytes() - numBytesBitsetAndFixedRow1;
       |PlatformDependent.copyMemory(
       |  obj1, offset1 + ${(bitset1Words + schema1.size) * 8},
       |  buf, $cursor,
       |  numBytesVariableRow1);
     """.stripMargin

    // --------------------- copy variable length portion from row 2 ----------------------- //
    val copyVariableLengthRow2 = s"""
       |// Copy variable length data for row2
       |long numBytesBitsetAndFixedRow2 = ${(bitset2Words + schema2.size) * 8};
       |long numBytesVariableRow2 = row2.getSizeInBytes() - numBytesBitsetAndFixedRow2;
       |PlatformDependent.copyMemory(
       |  obj2, offset2 + ${(bitset2Words + schema2.size) * 8},
       |  buf, $cursor + numBytesVariableRow1,
       |  numBytesVariableRow2);
     """.stripMargin

    // ------------- update fixed length data for variable length data type  --------------- //
    val updateOffset = (schema1 ++ schema2).zipWithIndex.map { case (field, i) =>
      // Skip fixed length data types, and only generate code for variable length data
      if (UnsafeRow.isFixedLength(field.dataType)) {
        ""
      } else {
        // Number of bytes to increase for the offset. Note that since in UnsafeRow we store the
        // offset in the upper 32 bit of the words, we can just shift the offset to the left by
        // 32 and increment that amount in place.
        val shift =
          if (i < schema1.size) {
            s"${(outputBitsetWords - bitset1Words + schema2.size) * 8}L"
          } else {
            s"${(outputBitsetWords - bitset2Words + schema1.size) * 8}L + numBytesVariableRow1"
          }
        val cursor = offset + outputBitsetWords * 8 + i * 8
        s"""
           |PlatformDependent.UNSAFE.putLong(buf, $cursor,
           |  PlatformDependent.UNSAFE.getLong(buf, $cursor) + ($shift << 32));
         """.stripMargin
      }
    }.mkString

    // ------------------------ Finally, put everything together  --------------------------- //
    val code = s"""
       |public Object generate($exprType[] exprs) {
       |  return new SpecificUnsafeRowJoiner();
       |}
       |
       |class SpecificUnsafeRowJoiner extends ${classOf[UnsafeRowJoiner].getName} {
       |  private byte[] buf = new byte[64];
       |  private UnsafeRow out = new UnsafeRow();
       |
       |  public UnsafeRow join(UnsafeRow row1, UnsafeRow row2) {
       |    // row1: ${schema1.size} fields, $bitset1Words words in bitset
       |    // row2: ${schema2.size}, $bitset2Words words in bitset
       |    // output: ${schema1.size + schema2.size} fields, $outputBitsetWords words in bitset
       |    final int sizeInBytes = row1.getSizeInBytes() + row2.getSizeInBytes();
       |    if (sizeInBytes > buf.length) {
       |      buf = new byte[sizeInBytes];
       |    }
       |
       |    final Object obj1 = row1.getBaseObject();
       |    final long offset1 = row1.getBaseOffset();
       |    final Object obj2 = row2.getBaseObject();
       |    final long offset2 = row2.getBaseOffset();
       |
       |    $copyBitset1
       |    $copyBitset2
       |    $copyFixedLengthRow1
       |    $copyFixedLengthRow2
       |    $copyVariableLengthRow1
       |    $copyVariableLengthRow2
       |    $updateOffset
       |
       |    out.pointTo(buf, ${schema1.size + schema2.size}, sizeInBytes - $sizeReduction);
       |
       |    return out;
       |  }
       |}
     """.stripMargin

    logDebug(s"SpecificUnsafeRowJoiner($schema1, $schema2):\n${CodeFormatter.format(code)}")
    // println(CodeFormatter.format(code))

    val c = compile(code)
    c.generate(Array.empty).asInstanceOf[UnsafeRowJoiner]
  }
}
