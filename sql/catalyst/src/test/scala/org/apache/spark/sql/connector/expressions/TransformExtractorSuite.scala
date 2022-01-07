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
      case BucketTransform(numBuckets, FieldReference(seq), _) =>
        assert(numBuckets === 16)
        assert(seq === Seq("a", "b"))
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
}
