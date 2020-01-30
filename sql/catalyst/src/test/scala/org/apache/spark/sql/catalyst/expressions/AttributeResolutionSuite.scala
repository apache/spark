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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{caseInsensitiveResolution, caseSensitiveResolution}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class AttributeResolutionSuite extends SparkFunSuite {
  val resolver = caseInsensitiveResolution

  test("basic attribute resolution with namespaces") {
    val attrs = Seq(
      AttributeReference("a", IntegerType)(qualifier = Seq("ns1", "ns2")),
      AttributeReference("a", IntegerType)(qualifier = Seq("ns1", "ns2", "ns3")))

    // Try to match attribute reference with name "a" with qualifier "ns1.ns2".
    Seq(Seq("ns2", "a"), Seq("ns1", "ns2", "a")).foreach { nameParts =>
      attrs.resolve(nameParts, resolver) match {
        case Some(attr) => assert(attr.semanticEquals(attrs(0)))
        case _ => fail()
      }
    }

    // Resolution is ambiguous.
    val ex = intercept[AnalysisException] {
      attrs.resolve(Seq("a"), resolver)
    }
    assert(ex.getMessage.contains(
      "Reference 'a' is ambiguous, could be: ns1.ns2.a, ns1.ns2.ns3.a."))

    // Non-matching cases.
    Seq(Seq("ns1", "ns2"), Seq("ns1", "a")).foreach { nameParts =>
      val resolved = attrs.resolve(nameParts, resolver)
      assert(resolved.isEmpty)
    }
  }

  test("attribute resolution with nested fields") {
    val attrType = StructType(Seq(StructField("aa", IntegerType), StructField("bb", IntegerType)))
    val attrs = Seq(AttributeReference("a", attrType)(qualifier = Seq("ns1", "ns2")))

    val resolved = attrs.resolve(Seq("ns1", "ns2", "a", "aa"), resolver)
    resolved match {
      case Some(Alias(_, name)) => assert(name == "aa")
      case _ => fail()
    }

    val ex = intercept[AnalysisException] {
      attrs.resolve(Seq("ns1", "ns2", "a", "cc"), resolver)
    }
    assert(ex.getMessage.contains("No such struct field cc in aa, bb"))
  }

  test("attribute resolution with case insensitive resolver") {
    val attrs = Seq(AttributeReference("a", IntegerType)(qualifier = Seq("ns1", "ns2")))
    attrs.resolve(Seq("Ns1", "nS2", "A"), caseInsensitiveResolution) match {
      case Some(attr) => assert(attr.semanticEquals(attrs(0)) && attr.name == "A")
      case _ => fail()
    }
  }

  test("attribute resolution with case sensitive resolver") {
    val attrs = Seq(AttributeReference("a", IntegerType)(qualifier = Seq("ns1", "ns2")))
    assert(attrs.resolve(Seq("Ns1", "nS2", "A"), caseSensitiveResolution).isEmpty)
    assert(attrs.resolve(Seq("ns1", "ns2", "A"), caseSensitiveResolution).isEmpty)
    attrs.resolve(Seq("ns1", "ns2", "a"), caseSensitiveResolution) match {
      case Some(attr) => assert(attr.semanticEquals(attrs(0)))
      case _ => fail()
    }
  }

  test("attribute resolution should try to match the longest qualifier") {
    // We have two attributes:
    // 1) "a.b" where "a" is the name and "b" is the nested field.
    // 2) "a.b.a" where "b" is the name, left-side "a" is the qualifier and the right-side "a"
    //    is the nested field.
    // When "a.b" is resolved, "b" is tried first as the name, so it is resolved to #2 attribute.
    val a1Type = StructType(Seq(StructField("b", IntegerType)))
    val a2Type = StructType(Seq(StructField("a", IntegerType)))
    val attrs = Seq(
      AttributeReference("a", a1Type)(),
      AttributeReference("b", a2Type)(qualifier = Seq("a")))
    attrs.resolve(Seq("a", "b"), resolver) match {
      case Some(attr) => assert(attr.semanticEquals(attrs(1)))
      case _ => fail()
    }
  }
}
