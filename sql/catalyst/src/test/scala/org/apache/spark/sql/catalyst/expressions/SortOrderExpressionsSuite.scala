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

import java.sql.Timestamp
import java.util.TimeZone

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.collection.unsafe.sort.PrefixComparators._

class SortOrderExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("SortPrefix") {
    val b1 = Literal.create(false, BooleanType)
    val b2 = Literal.create(true, BooleanType)
    val i1 = Literal.create(20132983, IntegerType)
    val i2 = Literal.create(-20132983, IntegerType)
    val l1 = Literal.create(20132983L, LongType)
    val l2 = Literal.create(-20132983L, LongType)
    val millis = 1524954911000L
    // Explicitly choose a time zone, since Date objects can create different values depending on
    // local time zone of the machine on which the test is running
    val oldDefaultTZ = TimeZone.getDefault
    val d1 = try {
      TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
      Literal.create(new java.sql.Date(millis), DateType)
    } finally {
      TimeZone.setDefault(oldDefaultTZ)
    }
    val t1 = Literal.create(new Timestamp(millis), TimestampType)
    val f1 = Literal.create(0.7788229f, FloatType)
    val f2 = Literal.create(-0.7788229f, FloatType)
    val db1 = Literal.create(0.7788229d, DoubleType)
    val db2 = Literal.create(-0.7788229d, DoubleType)
    val s1 = Literal.create("T", StringType)
    val s2 = Literal.create("This is longer than 8 characters", StringType)
    val bin1 = Literal.create(Array[Byte](12), BinaryType)
    val bin2 = Literal.create(Array[Byte](12, 17, 99, 0, 0, 0, 2, 3, 0xf4.asInstanceOf[Byte]),
      BinaryType)
    val dec1 = Literal(Decimal(20132983L, 10, 2))
    val dec2 = Literal(Decimal(20132983L, 19, 2))
    val dec3 = Literal(Decimal(20132983L, 21, 2))
    val list1 = Literal.create(Seq(1, 2), ArrayType(IntegerType))
    val nullVal = Literal.create(null, IntegerType)

    checkEvaluation(SortPrefix(SortOrder(b1, Ascending)), 0L)
    checkEvaluation(SortPrefix(SortOrder(b2, Ascending)), 1L)
    checkEvaluation(SortPrefix(SortOrder(i1, Ascending)), 20132983L)
    checkEvaluation(SortPrefix(SortOrder(i2, Ascending)), -20132983L)
    checkEvaluation(SortPrefix(SortOrder(l1, Ascending)), 20132983L)
    checkEvaluation(SortPrefix(SortOrder(l2, Ascending)), -20132983L)
    // For some reason, the Literal.create code gives us the number of days since the epoch
    checkEvaluation(SortPrefix(SortOrder(d1, Ascending)), 17649L)
    checkEvaluation(SortPrefix(SortOrder(t1, Ascending)), millis * 1000)
    checkEvaluation(SortPrefix(SortOrder(f1, Ascending)),
      DoublePrefixComparator.computePrefix(f1.value.asInstanceOf[Float].toDouble))
    checkEvaluation(SortPrefix(SortOrder(f2, Ascending)),
      DoublePrefixComparator.computePrefix(f2.value.asInstanceOf[Float].toDouble))
    checkEvaluation(SortPrefix(SortOrder(db1, Ascending)),
      DoublePrefixComparator.computePrefix(db1.value.asInstanceOf[Double]))
    checkEvaluation(SortPrefix(SortOrder(db2, Ascending)),
      DoublePrefixComparator.computePrefix(db2.value.asInstanceOf[Double]))
    checkEvaluation(SortPrefix(SortOrder(s1, Ascending)),
      StringPrefixComparator.computePrefix(s1.value.asInstanceOf[UTF8String]))
    checkEvaluation(SortPrefix(SortOrder(s2, Ascending)),
      StringPrefixComparator.computePrefix(s2.value.asInstanceOf[UTF8String]))
    checkEvaluation(SortPrefix(SortOrder(bin1, Ascending)),
      BinaryPrefixComparator.computePrefix(bin1.value.asInstanceOf[Array[Byte]]))
    checkEvaluation(SortPrefix(SortOrder(bin2, Ascending)),
      BinaryPrefixComparator.computePrefix(bin2.value.asInstanceOf[Array[Byte]]))
    checkEvaluation(SortPrefix(SortOrder(dec1, Ascending)), 20132983L)
    checkEvaluation(SortPrefix(SortOrder(dec2, Ascending)), 2013298L)
    checkEvaluation(SortPrefix(SortOrder(dec3, Ascending)),
      DoublePrefixComparator.computePrefix(201329.83d))
    checkEvaluation(SortPrefix(SortOrder(list1, Ascending)), 0L)
    checkEvaluation(SortPrefix(SortOrder(nullVal, Ascending)), null)
  }
}
