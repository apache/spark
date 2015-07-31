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


abstract class UnsafeRowConcat {
  def concat(row1: UnsafeRow, row2: UnsafeRow): UnsafeRow
}


object GenerateRowConcat extends CodeGenerator[(StructType, StructType), UnsafeRowConcat] {

  def dump(word: Long): String = {
    Seq.tabulate(64) { i => if ((word >> i) % 2 == 0) "0" else "1" }.reverse.mkString
  }

  override protected def create(in: (StructType, StructType)): UnsafeRowConcat = {
    create(in._1, in._2)
  }

  override protected def canonicalize(in: (StructType, StructType)): (StructType, StructType) = in

  override protected def bind(in: (StructType, StructType), inputSchema: Seq[Attribute])
    : (StructType, StructType) = {
    in
  }

  def create(schema1: StructType, schema2: StructType): UnsafeRowConcat = {
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

    // Copy bitset from row1: pretty straightforward
    val copyBitset1 = Seq.tabulate(bitset1Words) { i =>
      s"""
         |PlatformDependent.UNSAFE.putLong(buf, ${offset + i * 8},
         |  PlatformDependent.UNSAFE.getLong(obj1, ${offset + i * 8}));
       """.stripMargin
    }.mkString

    var copyBitset2 = ""
    if (bitset1Remainder == 0) {
      copyBitset2 += Seq.tabulate(bitset2Words) { i =>
        s"""
           |PlatformDependent.UNSAFE.putLong(buf, ${offset + (bitset1Words + i) * 8},
           |  PlatformDependent.UNSAFE.getLong(obj2, ${offset + i * 8}));
         """.stripMargin
      }.mkString
    } else {
      // Copy bitset from row2: slightly more complicated here, as we need to use the leftover
      // space from the first bitset.
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

      if (bitset2Remainder > (64 - bitset1Remainder)) {
        val lastWord = bitset2Words - 1
        copyBitset2 +=
          s"""
             |PlatformDependent.UNSAFE.putLong(buf, ${offset + (outputBitsetWords - 1) * 8},
             |  bs2w${lastWord}p2);
          """.stripMargin
      }
    }


    val code = s"""
       |public Object generate($exprType[] exprs) {
       |  return new SpecificRowConat();
       |}
       |
       |class SpecificRowConat extends ${classOf[UnsafeRowConcat].getName} {
       |  private byte[] buf = new byte[64];
       |  private UnsafeRow out = new UnsafeRow();
       |
       |  public UnsafeRow concat(UnsafeRow row1, UnsafeRow row2) {
       |    // row1: ${schema1.size} fields, $bitset1Words words in bitset
       |    // row2: ${schema2.size}, $bitset2Words words in bitset
       |    // output: ${schema1.size + schema2.size} fields, $outputBitsetWords words in bitset
       |    final int sizeInBytes = row1.getSizeInBytes() + row2.getSizeInBytes();
       |    if (sizeInBytes > buf.length) {
       |      buf = new byte[sizeInBytes];
       |    }
       |
       |    final Object obj1 = row1.getBaseObject();
       |    final Object offset1 = row1.getBaseOffset();
       |    final Object obj2 = row2.getBaseObject();
       |    final Object offset2 = row2.getBaseOffset();
       |
       |    $copyBitset1
       |    $copyBitset2
       |
       |    out.pointTo(buf, ${schema1.size + schema2.size}, sizeInBytes - $sizeReduction);
       |
       |    return out;
       |  }
       |}
     """.stripMargin

    logDebug(s"code for GenerateRowConcat($schema1, $schema2):\n${CodeFormatter.format(code)}")

    val c = compile(code)
    c.generate(Array.empty).asInstanceOf[UnsafeRowConcat]
  }
}
