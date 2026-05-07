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
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType, StructField, StructType}

class NamedExpressionSuite extends SparkFunSuite {

  test("SPARK-34636: sql method should quote qualified names properly") {
    val attr1 = UnresolvedAttribute("a"::Nil)
    assert(attr1.sql === "a")
    val attr2 = UnresolvedAttribute("a"::"b"::Nil)
    assert(attr2.sql === "a.b")
    val attr3 = UnresolvedAttribute("a.b"::"c.d"::Nil)
    assert(attr3.sql === "`a.b`.`c.d`")
    val attr4 = UnresolvedAttribute("a`b"::"c.d"::Nil)
    assert(attr4.sql === "`a``b`.`c.d`")
    val attr5 = AttributeReference("a", IntegerType)()
    assert(attr5.sql === "a")
    val attr6 = AttributeReference("a", IntegerType)(qualifier = "b"::Nil)
    assert(attr6.sql === "b.a")
    val attr7 = AttributeReference("a.b", IntegerType)(qualifier = "c.d"::Nil)
    assert(attr7.sql === "`c.d`.`a.b`")
    val attr8 = AttributeReference("a.b", IntegerType)(qualifier = "c`d"::Nil)
    assert(attr8.sql === "`c``d`.`a.b`")
    val attr9 = Alias(attr8, "e")()
    assert(attr9.sql === "`c``d`.`a.b` AS e")
    val attr10 = Alias(attr8, "e")(qualifier = "f"::Nil)
    assert(attr10.sql === "`c``d`.`a.b` AS f.e")
    val attr11 = Alias(attr8, "e.f")(qualifier = "g.h"::Nil)
    assert(attr11.sql === "`c``d`.`a.b` AS `g.h`.`e.f`")
    val attr12 = Alias(attr8, "e`f")(qualifier = "g.h"::Nil)
    assert(attr12.sql === "`c``d`.`a.b` AS `g.h`.`e``f`")
    val attr13 = UnresolvedAttribute("`a.b`")
    assert(attr13.sql === "`a.b`")
  }

  test("SPARK-34805: non inheritable metadata should be removed from child struct in Alias") {
    val nonInheritableMetadataKey = "non-inheritable-key"
    val metadata = new MetadataBuilder()
      .putString(nonInheritableMetadataKey, "value1")
      .putString("key", "value2")
      .build()
    val structType = StructType(Seq(StructField("value", StringType, metadata = metadata)))
    val alias = Alias(GetStructField(AttributeReference("a", structType)(), 0), "my-alias")(
      nonInheritableMetadataKeys = Seq(nonInheritableMetadataKey))
    assert(!alias.metadata.contains(nonInheritableMetadataKey))
    assert(alias.metadata.contains("key"))
  }
}
