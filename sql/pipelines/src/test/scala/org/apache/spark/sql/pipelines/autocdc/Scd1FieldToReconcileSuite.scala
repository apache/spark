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

package org.apache.spark.sql.pipelines.autocdc

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{caseInsensitiveResolution, caseSensitiveResolution, Resolver}
import org.apache.spark.sql.types._

class Scd1FieldToReconcileSuite extends SparkFunSuite {

  private val profileType = new StructType()
    .add("city", StringType)
    .add("contact", new StructType().add("zip", StringType))

  private def fieldsOf(
      schema: StructType,
      ignoreNullSelection: Option[ColumnSelection],
      resolver: Resolver = caseSensitiveResolution): Set[Scd1FieldToReconcile] = {
    val fields = Scd1FieldToReconcile.fromSchema(
      schema = schema,
      ignoreNullSelection = ignoreNullSelection,
      resolver = resolver)
    // Compare as a set, since the order is unspecified, without letting it hide duplicates.
    assert(fields.distinct.size == fields.size, s"Duplicate fields to reconcile: $fields")
    fields.toSet
  }

  private def includeColumns(names: String*): Option[ColumnSelection] =
    Some(ColumnSelection.IncludeColumns(names.map(UnqualifiedColumnName(_))))

  private def wholeColumn(name: String, leafPaths: Seq[String]*): Scd1FieldToReconcile =
    Scd1FieldToReconcile(path = Seq(name), leafPaths = leafPaths, isIgnoreNullLeaf = false)

  private def ignoreNullLeaf(path: String*): Scd1FieldToReconcile =
    Scd1FieldToReconcile(path = path, leafPaths = Seq(path), isIgnoreNullLeaf = true)

  test("without an ignore-null selection, every column is reconciled whole") {
    val schema = new StructType()
      .add("name", StringType)
      .add("profile", profileType)

    assert(fieldsOf(schema, ignoreNullSelection = None) == Set(
      wholeColumn("name", Seq("name")),
      wholeColumn("profile", Seq("profile", "city"), Seq("profile", "contact", "zip"))))
  }

  test("a column selected for ignore-null is reconciled leaf by leaf") {
    val schema = new StructType()
      .add("name", StringType)
      .add("profile", profileType)
      .add("age", IntegerType)

    assert(fieldsOf(schema, includeColumns("profile", "age")) == Set(
      wholeColumn("name", Seq("name")),
      ignoreNullLeaf("profile", "city"),
      ignoreNullLeaf("profile", "contact", "zip"),
      ignoreNullLeaf("age")))
  }

  test("an exclude-list selection selects every column it does not name") {
    val schema = new StructType()
      .add("name", StringType)
      .add("profile", profileType)
    val ignoreNullSelection =
      Some(ColumnSelection.ExcludeColumns(Seq(UnqualifiedColumnName("name"))))

    assert(fieldsOf(schema, ignoreNullSelection) == Set(
      wholeColumn("name", Seq("name")),
      ignoreNullLeaf("profile", "city"),
      ignoreNullLeaf("profile", "contact", "zip")))
  }

  test("arrays and maps are leaves whose element types are not traversed") {
    val schema = new StructType()
      .add("tags", ArrayType(new StructType().add("label", StringType)))
      .add("properties", MapType(StringType, profileType))

    assert(fieldsOf(schema, includeColumns("tags")) == Set(
      ignoreNullLeaf("tags"),
      wholeColumn("properties", Seq("properties"))))
  }

  test("an empty struct is a whole column without leaves, and has no fields when selected") {
    val schema = new StructType()
      .add("empty", new StructType())
      .add("name", StringType)

    assert(fieldsOf(schema, ignoreNullSelection = None) == Set(
      wholeColumn("empty"),
      wholeColumn("name", Seq("name"))))
    assert(fieldsOf(schema, includeColumns("empty")) == Set(wholeColumn("name", Seq("name"))))
  }

  test("a top-level name containing a dot stays distinct from a nested path") {
    val schema = new StructType()
      .add("profile", new StructType().add("city", StringType))
      .add("profile.city", StringType)

    assert(fieldsOf(schema, includeColumns("profile")) == Set(
      ignoreNullLeaf("profile", "city"),
      wholeColumn("profile.city", Seq("profile.city"))))
  }

  test("the selection is resolved with the supplied resolver, keeping the schema's names") {
    val schema = new StructType()
      .add("Profile", new StructType().add("City", StringType))
      .add("Name", StringType)

    assert(fieldsOf(schema, includeColumns("profile"), caseInsensitiveResolution) == Set(
      ignoreNullLeaf("Profile", "City"),
      wholeColumn("Name", Seq("Name"))))
  }
}
