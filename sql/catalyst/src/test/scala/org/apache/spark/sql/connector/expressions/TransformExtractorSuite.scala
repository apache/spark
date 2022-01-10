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

package org.apache.spark.sql.connector.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst
import org.apache.spark.sql.connector.expressions.LogicalExpressions.bucket
import org.apache.spark.sql.types.DataType

class TransformExtractorSuite extends SparkFunSuite {
  /**
   * Creates a Literal using an anonymous class.
   */
  private def lit[T](literal: T): Literal[T] = new Literal[T] {
    override def value: T = literal
    override def dataType: DataType = catalyst.expressions.Literal(literal).dataType
    override def toString: String = literal.toString
  }

  /**
   * Creates a NamedReference using an anonymous class.
   */
  private def ref(names: String*): NamedReference = new NamedReference {
    override def fieldNames: Array[String] = names.toArray
    override def toString: String = names.mkString(".")
  }

  /**
   * Creates a Transform using an anonymous class.
   */
  private def transform(func: String, ref: NamedReference): Transform = new Transform {
    override def name: String = func
    override def references: Array[NamedReference] = Array(ref)
    override def arguments: Array[Expression] = Array(ref)
    override def toString: String = ref.describe
  }

  test("Identity extractor") {
    transform("identity", ref("a", "b")) match {
      case IdentityTransform(FieldReference(seq)) =>
        assert(seq === Seq("a", "b"))
      case _ =>
        fail("Did not match IdentityTransform extractor")
    }

    transform("unknown", ref("a", "b")) match {
      case IdentityTransform(FieldReference(_)) =>
        fail("Matched unknown transform")
      case _ =>
        // expected
    }
  }

  test("Years extractor") {
    transform("years", ref("a", "b")) match {
      case YearsTransform(FieldReference(seq)) =>
        assert(seq === Seq("a", "b"))
      case _ =>
        fail("Did not match YearsTransform extractor")
    }

    transform("unknown", ref("a", "b")) match {
      case YearsTransform(FieldReference(_)) =>
        fail("Matched unknown transform")
      case _ =>
        // expected
    }
  }

  test("Months extractor") {
    transform("months", ref("a", "b")) match {
      case MonthsTransform(FieldReference(seq)) =>
        assert(seq === Seq("a", "b"))
      case _ =>
        fail("Did not match MonthsTransform extractor")
    }

    transform("unknown", ref("a", "b")) match {
      case MonthsTransform(FieldReference(_)) =>
        fail("Matched unknown transform")
      case _ =>
        // expected
    }
  }

  test("Days extractor") {
    transform("days", ref("a", "b")) match {
      case DaysTransform(FieldReference(seq)) =>
        assert(seq === Seq("a", "b"))
      case _ =>
        fail("Did not match DaysTransform extractor")
    }

    transform("unknown", ref("a", "b")) match {
      case DaysTransform(FieldReference(_)) =>
        fail("Matched unknown transform")
      case _ =>
        // expected
    }
  }

  test("Hours extractor") {
    transform("hours", ref("a", "b")) match {
      case HoursTransform(FieldReference(seq)) =>
        assert(seq === Seq("a", "b"))
      case _ =>
        fail("Did not match HoursTransform extractor")
    }

    transform("unknown", ref("a", "b")) match {
      case HoursTransform(FieldReference(_)) =>
        fail("Matched unknown transform")
      case _ =>
        // expected
    }
  }

  test("Bucket extractor") {
    val col = ref("a", "b")
    val bucketTransform = new Transform {
      override def name: String = "bucket"
      override def references: Array[NamedReference] = Array(col)
      override def arguments: Array[Expression] = Array(lit(16), col)
      override def toString: String = s"bucket(16, ${col.describe})"
    }

    bucketTransform match {
      case BucketTransform(numBuckets, cols, _) =>
        assert(numBuckets === 16)
        assert(cols(0).fieldNames === Seq("a", "b"))
      case _ =>
        fail("Did not match BucketTransform extractor")
    }

    transform("unknown", ref("a", "b")) match {
      case BucketTransform(_, _, _) =>
        fail("Matched unknown transform")
      case _ =>
      // expected
    }
  }

  test("Sorted Bucket extractor") {
    val col = Array(ref("a"), ref("b"))
    val sortedCol = Array(ref("c"), ref("d"))

    val sortedBucketTransform = new Transform {
      override def name: String = "sorted_bucket"
      override def references: Array[NamedReference] = col ++ sortedCol
      override def arguments: Array[Expression] = (col :+ lit(16)) ++ sortedCol
      override def describe: String = s"bucket(16, ${col(0).describe}, ${col(1).describe} " +
        s"${sortedCol(0).describe} ${sortedCol(1).describe})"
    }

    sortedBucketTransform match {
      case BucketTransform(numBuckets, cols, sortCols) =>
        assert(numBuckets === 16)
        assert(cols.flatMap(c => c.fieldNames()) === Seq("a", "b"))
        assert(sortCols.flatMap(c => c.fieldNames()) === Seq("c", "d"))
      case _ =>
        fail("Did not match BucketTransform extractor")
    }
  }

  test("test bucket") {
    val col = Array(ref("a"), ref("b"))
    val sortedCol = Array(ref("c"), ref("d"))

    val bucketTransform = bucket(16, col)
    val reference1 = bucketTransform.references
    assert(reference1.length == 2)
    assert(reference1(0).fieldNames() === Seq("a"))
    assert(reference1(1).fieldNames() === Seq("b"))
    val arguments1 = bucketTransform.arguments
    assert(arguments1.length == 3)
    assert(arguments1(0).asInstanceOf[LiteralValue[Integer]].value === 16)
    assert(arguments1(1).asInstanceOf[NamedReference].fieldNames() === Seq("a"))
    assert(arguments1(2).asInstanceOf[NamedReference].fieldNames() === Seq("b"))
    val copied1 = bucketTransform.withReferences(reference1)
    assert(copied1.equals(bucketTransform))

    val sortedBucketTransform = bucket(16, col, sortedCol)
    val reference2 = sortedBucketTransform.references
    assert(reference2.length == 4)
    assert(reference2(0).fieldNames() === Seq("a"))
    assert(reference2(1).fieldNames() === Seq("b"))
    assert(reference2(2).fieldNames() === Seq("c"))
    assert(reference2(3).fieldNames() === Seq("d"))
    val arguments2 = sortedBucketTransform.arguments
    assert(arguments2.length == 5)
    assert(arguments2(0).asInstanceOf[NamedReference].fieldNames() === Seq("a"))
    assert(arguments2(1).asInstanceOf[NamedReference].fieldNames() === Seq("b"))
    assert(arguments2(2).asInstanceOf[LiteralValue[Integer]].value === 16)
    assert(arguments2(3).asInstanceOf[NamedReference].fieldNames() === Seq("c"))
    assert(arguments2(4).asInstanceOf[NamedReference].fieldNames() === Seq("d"))
    val copied2 = sortedBucketTransform.withReferences(reference2)
    assert(copied2.equals(sortedBucketTransform))
  }
}
