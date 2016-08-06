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

import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeRow}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.Platform

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

  override protected def create(in: (StructType, StructType)): UnsafeRowJoiner = {
    create(in._1, in._2)
  }

  override protected def canonicalize(in: (StructType, StructType)): (StructType, StructType) = in

  override protected def bind(in: (StructType, StructType), inputSchema: Seq[Attribute])
    : (StructType, StructType) = {
    in
  }

  def create(schema1: StructType, schema2: StructType): UnsafeRowJoiner = {
    val offset = Platform.BYTE_ARRAY_OFFSET
    val getLong = "Platform.getLong"
    val putLong = "Platform.putLong"

    val bitset1Words = (schema1.size + 63) / 64
    val bitset2Words = (schema2.size + 63) / 64
    val outputBitsetWords = (schema1.size + schema2.size + 63) / 64
    val bitset1Remainder = schema1.size % 64

    // The number of bytes we can reduce when we concat two rows together.
    // The only reduction comes from merging the bitset portion of the two rows, saving 1 word.
    val sizeReduction = (bitset1Words + bitset2Words - outputBitsetWords) * 8

    // --------------------- copy bitset from row 1 and row 2 --------------------------- //
    val copyBitset = Seq.tabulate(outputBitsetWords) { i =>
      val bits = if (bitset1Remainder > 0) {
        if (i < bitset1Words - 1) {
          s"$getLong(obj1, offset1 + ${i * 8})"
        } else if (i == bitset1Words - 1) {
          // combine last work of bitset1 and first word of bitset2
          s"$getLong(obj1, offset1 + ${i * 8}) | ($getLong(obj2, offset2) << $bitset1Remainder)"
        } else if (i - bitset1Words < bitset2Words - 1) {
          // combine next two words of bitset2
          s"($getLong(obj2, offset2 + ${(i - bitset1Words) * 8}) >>> (64 - $bitset1Remainder))" +
            s" | ($getLong(obj2, offset2 + ${(i - bitset1Words + 1) * 8}) << $bitset1Remainder)"
        } else {
          // last word of bitset2
          s"$getLong(obj2, offset2 + ${(i - bitset1Words) * 8}) >>> (64 - $bitset1Remainder)"
        }
      } else {
        // they are aligned by word
        if (i < bitset1Words) {
          s"$getLong(obj1, offset1 + ${i * 8})"
        } else {
          s"$getLong(obj2, offset2 + ${(i - bitset1Words) * 8})"
        }
      }
      s"$putLong(buf, ${offset + i * 8}, $bits);"
    }.mkString("\n")

    // --------------------- copy fixed length portion from row 1 ----------------------- //
    var cursor = offset + outputBitsetWords * 8
    val copyFixedLengthRow1 = s"""
       |// Copy fixed length data for row1
       |Platform.copyMemory(
       |  obj1, offset1 + ${bitset1Words * 8},
       |  buf, $cursor,
       |  ${schema1.size * 8});
     """.stripMargin
    cursor += schema1.size * 8

    // --------------------- copy fixed length portion from row 2 ----------------------- //
    val copyFixedLengthRow2 = s"""
       |// Copy fixed length data for row2
       |Platform.copyMemory(
       |  obj2, offset2 + ${bitset2Words * 8},
       |  buf, $cursor,
       |  ${schema2.size * 8});
     """.stripMargin
    cursor += schema2.size * 8

    // --------------------- copy variable length portion from row 1 ----------------------- //
    val numBytesBitsetAndFixedRow1 = (bitset1Words + schema1.size) * 8
    val copyVariableLengthRow1 = s"""
       |// Copy variable length data for row1
       |long numBytesVariableRow1 = row1.getSizeInBytes() - $numBytesBitsetAndFixedRow1;
       |Platform.copyMemory(
       |  obj1, offset1 + ${(bitset1Words + schema1.size) * 8},
       |  buf, $cursor,
       |  numBytesVariableRow1);
     """.stripMargin

    // --------------------- copy variable length portion from row 2 ----------------------- //
    val numBytesBitsetAndFixedRow2 = (bitset2Words + schema2.size) * 8
    val copyVariableLengthRow2 = s"""
       |// Copy variable length data for row2
       |long numBytesVariableRow2 = row2.getSizeInBytes() - $numBytesBitsetAndFixedRow2;
       |Platform.copyMemory(
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
            s"(${(outputBitsetWords - bitset2Words + schema1.size) * 8}L + numBytesVariableRow1)"
          }
        val cursor = offset + outputBitsetWords * 8 + i * 8
        s"""
           |$putLong(buf, $cursor, $getLong(buf, $cursor) + ($shift << 32));
         """.stripMargin
      }
    }.mkString("\n")

    // ------------------------ Finally, put everything together  --------------------------- //
    val codeBody = s"""
       |public java.lang.Object generate(Object[] references) {
       |  return new SpecificUnsafeRowJoiner();
       |}
       |
       |class SpecificUnsafeRowJoiner extends ${classOf[UnsafeRowJoiner].getName} {
       |  private byte[] buf = new byte[64];
       |  private UnsafeRow out = new UnsafeRow(${schema1.size + schema2.size});
       |
       |  public UnsafeRow join(UnsafeRow row1, UnsafeRow row2) {
       |    // row1: ${schema1.size} fields, $bitset1Words words in bitset
       |    // row2: ${schema2.size}, $bitset2Words words in bitset
       |    // output: ${schema1.size + schema2.size} fields, $outputBitsetWords words in bitset
       |    final long sizeInBytes = row1.getSizeInBytes() + row2.getSizeInBytes() - $sizeReduction;
       |    if (sizeInBytes > buf.length) {
       |      if (sizeInBytes > Integer.MAX_VALUE) {
       |        throw new UnsupportedOperationException("Cannot allocate an array as " +
       |          "it's too big.");
       |      }
       |      buf = new byte[sizeInBytes];
       |    }
       |
       |    final java.lang.Object obj1 = row1.getBaseObject();
       |    final long offset1 = row1.getBaseOffset();
       |    final java.lang.Object obj2 = row2.getBaseObject();
       |    final long offset2 = row2.getBaseOffset();
       |
       |    $copyBitset
       |    $copyFixedLengthRow1
       |    $copyFixedLengthRow2
       |    $copyVariableLengthRow1
       |    $copyVariableLengthRow2
       |    $updateOffset
       |
       |    out.pointTo(buf, sizeInBytes);
       |
       |    return out;
       |  }
       |}
     """.stripMargin
    val code = CodeFormatter.stripOverlappingComments(new CodeAndComment(codeBody, Map.empty))
    logDebug(s"SpecificUnsafeRowJoiner($schema1, $schema2):\n${CodeFormatter.format(code)}")

    val c = CodeGenerator.compile(code)
    c.generate(Array.empty).asInstanceOf[UnsafeRowJoiner]
  }
}
