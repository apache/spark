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
      AttributeReference("a", IntegerType)(qualifier = Seq("ns1", "ns2", "t1")),
      AttributeReference("a", IntegerType)(qualifier = Seq("ns1", "ns2", "ns3", "t2")))

    // Try to match attribute reference with name "a" with qualifier "ns1.ns2.t1".
    Seq(Seq("t1", "a"), Seq("ns2", "t1", "a"), Seq("ns1", "ns2", "t1", "a")).foreach { nameParts =>
      attrs.resolve(nameParts, resolver) match {
        case Some(attr) => assert(attr.semanticEquals(attrs(0)))
        case _ => fail()
      }
    }

    // Non-matching cases
    Seq(Seq("ns1", "ns2", "t1"), Seq("ns2", "a")).foreach { nameParts =>
      assert(attrs.resolve(nameParts, resolver).isEmpty)
    }
  }

  test("attribute resolution where table and attribute names are the same") {
    val attrs = Seq(AttributeReference("t", IntegerType)(qualifier = Seq("ns1", "ns2", "t")))
    // Matching cases
    Seq(
      Seq("t"), Seq("t", "t"), Seq("ns2", "t", "t"), Seq("ns1", "ns2", "t", "t")
    ).foreach { nameParts =>
      attrs.resolve(nameParts, resolver) match {
        case Some(attr) => assert(attr.semanticEquals(attrs(0)))
        case _ => fail()
      }
    }

    // Non-matching case
    assert(attrs.resolve(Seq("ns1", "ns2", "t"), resolver).isEmpty)
  }

  test("attribute resolution ambiguity at the attribute name level") {
    val attrs = Seq(
      AttributeReference("a", IntegerType)(qualifier = Seq("ns1", "t1")),
      AttributeReference("a", IntegerType)(qualifier = Seq("ns1", "ns2", "t2")))

    checkError(
      exception = intercept[AnalysisException] {
        attrs.resolve(Seq("a"), resolver)
      },
      errorClass = "AMBIGUOUS_REFERENCE",
      parameters = Map(
        "name" -> "`a`",
        "referenceNames" -> "[`ns1`.`ns2`.`t2`.`a`, `ns1`.`t1`.`a`]"
      )
    )
  }

  test("attribute resolution ambiguity at the qualifier level") {
    val attrs = Seq(
      AttributeReference("a", IntegerType)(qualifier = Seq("ns1", "t")),
      AttributeReference("a", IntegerType)(qualifier = Seq("ns2", "ns1", "t")))

    checkError(
      exception = intercept[AnalysisException] {
        attrs.resolve(Seq("ns1", "t", "a"), resolver)
      },
      errorClass = "AMBIGUOUS_REFERENCE",
      parameters = Map(
        "name" -> "`ns1`.`t`.`a`",
        "referenceNames" -> "[`ns1`.`t`.`a`, `ns2`.`ns1`.`t`.`a`]"
      )
    )
  }

  test("attribute resolution with nested fields") {
    val attrType = StructType(Seq(StructField("aa", IntegerType), StructField("bb", IntegerType)))
    val attrs = Seq(AttributeReference("a", attrType)(qualifier = Seq("ns1", "t")))

    val resolved = attrs.resolve(Seq("ns1", "t", "a", "aa"), resolver)
    resolved match {
      case Some(Alias(_, name)) => assert(name == "aa")
      case _ => fail()
    }

    checkError(
      exception = intercept[AnalysisException] {
        attrs.resolve(Seq("ns1", "t", "a", "cc"), resolver)
      },
      errorClass = "FIELD_NOT_FOUND",
      parameters = Map("fieldName" -> "`cc`", "fields" -> "`aa`, `bb`"))
  }

  test("attribute resolution with case insensitive resolver") {
    val attrs = Seq(AttributeReference("a", IntegerType)(qualifier = Seq("ns1", "t")))
    attrs.resolve(Seq("Ns1", "T", "A"), caseInsensitiveResolution) match {
      case Some(attr) => assert(attr.semanticEquals(attrs(0)) && attr.name == "A")
      case _ => fail()
    }
  }

  test("attribute resolution with case sensitive resolver") {
    val attrs = Seq(AttributeReference("a", IntegerType)(qualifier = Seq("ns1", "t")))
    assert(attrs.resolve(Seq("Ns1", "T", "A"), caseSensitiveResolution).isEmpty)
    assert(attrs.resolve(Seq("ns1", "t", "A"), caseSensitiveResolution).isEmpty)
    attrs.resolve(Seq("ns1", "t", "a"), caseSensitiveResolution) match {
      case Some(attr) => assert(attr.semanticEquals(attrs(0)))
      case _ => fail()
    }
  }

  test("SPARK-57725: resolution does not fail when an attribute has a null name") {
    // An AttributeReference can carry a null name (e.g. from a StructField built with a null name
    // via the DataFrame API). Such an attribute is unaddressable by any column reference -- name
    // parts are never null -- so it must simply be skipped by the case-insensitive name maps
    // rather than causing a NullPointerException in groupBy(_.name.toLowerCase(...)).
    val a = AttributeReference("a", IntegerType)()
    val nullNamed = AttributeReference(null, IntegerType)()

    // Plain (no qualifier) -> exercises the `direct` map.
    val attrs = Seq(a, nullNamed)
    attrs.resolve(Seq("a"), resolver) match {
      case Some(attr) => assert(attr.semanticEquals(a))
      case _ => fail()
    }
    assert(attrs.resolve(Seq("b"), resolver).isEmpty)

    // 2-part qualifier -> exercises the `qualified` map.
    val qa = AttributeReference("a", IntegerType)(qualifier = Seq("ns1", "t1"))
    val qNullNamed = AttributeReference(null, IntegerType)(qualifier = Seq("ns1", "t1"))
    val qattrs = Seq(qa, qNullNamed)
    qattrs.resolve(Seq("t1", "a"), resolver) match {
      case Some(attr) => assert(attr.semanticEquals(qa))
      case _ => fail()
    }

    // 3-part qualifier -> forces the `qualified3Part` (3-part lookup) and `qualified4Part`
    // (4-part lookup) maps, so their population is also exercised with a null-named sibling.
    val q3a = AttributeReference("a", IntegerType)(qualifier = Seq("cat", "db", "tbl"))
    val q3NullNamed = AttributeReference(null, IntegerType)(qualifier = Seq("cat", "db", "tbl"))
    val q3attrs = Seq(q3a, q3NullNamed)
    Seq(Seq("db", "tbl", "a"), Seq("cat", "db", "tbl", "a")).foreach { nameParts =>
      q3attrs.resolve(nameParts, resolver) match {
        case Some(attr) => assert(attr.semanticEquals(q3a))
        case _ => fail()
      }
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
