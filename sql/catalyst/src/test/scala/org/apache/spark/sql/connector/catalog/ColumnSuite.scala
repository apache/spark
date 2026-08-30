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

package org.apache.spark.sql.connector.catalog

import org.apache.spark.SparkFunSuite
import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.ColumnDefinition
import org.apache.spark.sql.catalyst.util.FieldMetadataUtils.FIELD_ID_METADATA_KEY
import org.apache.spark.sql.connector.expressions.LiteralValue
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, StringType, StructField, StructType}

class ColumnSuite extends SparkFunSuite {

  private val intLiteral = LiteralValue(42, IntegerType)
  private val defaultValue = new ColumnDefaultValue("42", intLiteral)
  private val identitySpec = new IdentityColumnSpec(1L, 1L, false)

  // ---------------------------------------------------------------------------
  // Column.create factory overloads
  // ---------------------------------------------------------------------------

  test("create(name, type) defaults: nullable, no comment, no generation expr, no metadata") {
    val col = Column.create("c", IntegerType)
    assert(col.name() == "c")
    assert(col.dataType() == IntegerType)
    assert(col.nullable())
    assert(col.comment() == null)
    assert(col.defaultValue() == null)
    assert(col.generationExpression() == null)
    assert(col.identityColumnSpec() == null)
    assert(col.metadataInJSON() == null)
    assert(col.id() == null)
  }

  test("create(name, type, nullable) controls nullable flag") {
    assert(Column.create("c", IntegerType, false).nullable() == false)
    assert(Column.create("c", IntegerType, true).nullable() == true)
  }

  test("create(name, type, nullable, comment, metadataInJSON)") {
    val col = Column.create("c", StringType, false, "a comment", """{"key":"val"}""")
    assert(col.name() == "c")
    assert(col.dataType() == StringType)
    assert(col.nullable() == false)
    assert(col.comment() == "a comment")
    assert(col.metadataInJSON() == """{"key":"val"}""")
    assert(col.defaultValue() == null)
    assert(col.generationExpression() == null)
    assert(col.identityColumnSpec() == null)
  }

  test("create(name, type, nullable, comment, defaultValue, metadataInJSON)") {
    val col = Column.create("c", IntegerType, true, "doc", defaultValue, null)
    assert(col.defaultValue() == defaultValue)
    assert(col.generationExpression() == null)
    assert(col.identityColumnSpec() == null)
  }

  test("create(name, type, nullable, comment, generationExpression, metadataInJSON)") {
    val col = Column.create("c", IntegerType, true, null, "a + 1", null)
    assert(col.generationExpression() == "a + 1")
    assert(col.defaultValue() == null)
    assert(col.identityColumnSpec() == null)
  }

  test("create(name, type, nullable, comment, identityColumnSpec, metadataInJSON)") {
    val col = Column.create("c", IntegerType, false, null, identitySpec, null)
    assert(col.identityColumnSpec() == identitySpec)
    assert(col.defaultValue() == null)
    assert(col.generationExpression() == null)
  }

  // ---------------------------------------------------------------------------
  // Column.builderFor / Builder
  // ---------------------------------------------------------------------------

  test("builder defaults: nullable=true, everything else null") {
    val col = Column.builderFor("c", IntegerType).build()
    assert(col.name() == "c")
    assert(col.dataType() == IntegerType)
    assert(col.nullable())
    assert(col.comment() == null)
    assert(col.defaultValue() == null)
    assert(col.generationExpression() == null)
    assert(col.identityColumnSpec() == null)
    assert(col.metadataInJSON() == null)
    assert(col.id() == null)
  }

  test("builder sets all fields") {
    val col = Column.builderFor("c", IntegerType)
      .nullable(false)
      .comment("doc")
      .defaultValue(defaultValue)
      .metadata("""{"k":"v"}""")
      .id("abc-123")
      .build()
    assert(col.name() == "c")
    assert(col.dataType() == IntegerType)
    assert(col.nullable() == false)
    assert(col.comment() == "doc")
    assert(col.defaultValue() == defaultValue)
    assert(col.metadataInJSON() == """{"k":"v"}""")
    assert(col.id() == "abc-123")
  }

  test("builder with generationExpression") {
    val col = Column.builderFor("c", IntegerType)
      .generationExpression("a * 2")
      .build()
    assert(col.generationExpression() == "a * 2")
    assert(col.defaultValue() == null)
    assert(col.identityColumnSpec() == null)
  }

  test("builder with identityColumnSpec") {
    val col = Column.builderFor("c", IntegerType)
      .identityColumnSpec(identitySpec)
      .build()
    assert(col.identityColumnSpec() == identitySpec)
    assert(col.defaultValue() == null)
    assert(col.generationExpression() == null)
  }

  // ---------------------------------------------------------------------------
  // Builder invariants: conflicting definitions are rejected
  // ---------------------------------------------------------------------------

  test("builder rejects defaultValue + generationExpression") {
    val ex = intercept[SparkIllegalArgumentException] {
      Column.builderFor("c", IntegerType)
        .defaultValue(defaultValue)
        .generationExpression("a + 1")
        .build()
    }
    assert(ex.getMessage.contains("cannot have more than one definition"))
  }

  test("builder rejects generationExpression + identityColumnSpec") {
    val ex = intercept[SparkIllegalArgumentException] {
      Column.builderFor("c", IntegerType)
        .generationExpression("a + 1")
        .identityColumnSpec(identitySpec)
        .build()
    }
    assert(ex.getMessage.contains("cannot have more than one definition"))
  }

  test("builder rejects defaultValue + identityColumnSpec") {
    val ex = intercept[SparkIllegalArgumentException] {
      Column.builderFor("c", IntegerType)
        .defaultValue(defaultValue)
        .identityColumnSpec(identitySpec)
        .build()
    }
    assert(ex.getMessage.contains("cannot have more than one definition"))
  }

  test("builder rejects all three definitions set simultaneously") {
    val ex = intercept[SparkIllegalArgumentException] {
      Column.builderFor("c", IntegerType)
        .defaultValue(defaultValue)
        .generationExpression("a + 1")
        .identityColumnSpec(identitySpec)
        .build()
    }
    assert(ex.getMessage.contains("cannot have more than one definition"))
  }

  test("builder error message names the column") {
    val ex = intercept[SparkIllegalArgumentException] {
      Column.builderFor("my_column", IntegerType)
        .defaultValue(defaultValue)
        .generationExpression("x")
        .build()
    }
    assert(ex.getMessage.contains("my_column"))
  }

  // ---------------------------------------------------------------------------
  // newBuilder rejects null name / type
  // ---------------------------------------------------------------------------

  test("newBuilder rejects null name") {
    intercept[NullPointerException] {
      Column.builderFor(null, IntegerType)
    }
  }

  test("newBuilder rejects null dataType") {
    intercept[NullPointerException] {
      Column.builderFor("c", null)
    }
  }

  // ---------------------------------------------------------------------------
  // ColumnDefinition.fromV1Column - metadata cleaning
  // ---------------------------------------------------------------------------

  test("fromV1Column strips FIELD_ID_METADATA_KEY from metadata") {
    val metadata = new MetadataBuilder()
      .putString(FIELD_ID_METADATA_KEY, "42")
      .putString("custom_key", "custom_value")
      .build()
    val field = StructField("col", IntegerType, nullable = true, metadata)
    val colDef = ColumnDefinition.fromV1Column(field, CatalystSqlParser)
    assert(!colDef.metadata.contains(FIELD_ID_METADATA_KEY))
    assert(colDef.metadata.contains("custom_key"))
  }

  test("fromV1Column strips nested field IDs from struct dataType") {
    val nestedType = StructType(Array(
      StructField("x", IntegerType).withId("source-id"),
      StructField("y", IntegerType).withId("source-id")))
    val field = StructField("col", nestedType)
    val colDef = ColumnDefinition.fromV1Column(field, CatalystSqlParser)
    val resultType = colDef.dataType.asInstanceOf[StructType]
    resultType.fields.foreach { f => assert(f.id.isEmpty) }
  }
}
