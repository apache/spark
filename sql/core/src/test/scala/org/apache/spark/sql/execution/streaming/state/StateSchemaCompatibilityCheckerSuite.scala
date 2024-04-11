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

package org.apache.spark.sql.execution.streaming.state

import java.util.UUID

import scala.util.Random

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.execution.streaming.state.StateStoreTestsHelper.newDir
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class StateSchemaCompatibilityCheckerSuite extends SharedSparkSession {

  private val hadoopConf: Configuration = new Configuration()
  private val opId = Random.nextInt(100000)
  private val partitionId = StateStore.PARTITION_ID_TO_CHECK_SCHEMA

  private val structSchema = new StructType()
    .add(StructField("nested1", IntegerType, nullable = true))
    .add(StructField("nested2", StringType, nullable = true))

  private val keySchema = new StructType()
    .add(StructField("key1", IntegerType, nullable = true))
    .add(StructField("key2", StringType, nullable = true))
    .add(StructField("key3", structSchema, nullable = true))

  private val valueSchema = new StructType()
    .add(StructField("value1", IntegerType, nullable = true))
    .add(StructField("value2", StringType, nullable = true))
    .add(StructField("value3", structSchema, nullable = true))

  private val longKeySchema = new StructType()
    .add(StructField("key" + "1" * 64 * 1024, IntegerType, nullable = true))
    .add(StructField("key" + "2" * 64 * 1024, StringType, nullable = true))
    .add(StructField("key" + "3" * 64 * 1024, structSchema, nullable = true))

  private val longValueSchema = new StructType()
    .add(StructField("value" + "1" * 64 * 1024, IntegerType, nullable = true))
    .add(StructField("value" + "2" * 64 * 1024, StringType, nullable = true))
    .add(StructField("value" + "3" * 64 * 1024, structSchema, nullable = true))

  private val keySchema65535Bytes = new StructType()
    .add(StructField("k" * (65535 - 87), IntegerType, nullable = true))

  private val valueSchema65535Bytes = new StructType()
    .add(StructField("v" * (65535 - 87), IntegerType, nullable = true))

  private val keySchemaWithCollation = new StructType()
    .add(StructField("key1", IntegerType, nullable = true))
    .add(StructField("key2", StringType("UTF8_BINARY_LCASE"), nullable = true))
    .add(StructField("key3", structSchema, nullable = true))

  private val valueSchemaWithCollation = new StructType()
    .add(StructField("value1", IntegerType, nullable = true))
    .add(StructField("value2", StringType("UTF8_BINARY_LCASE"), nullable = true))
    .add(StructField("value3", structSchema, nullable = true))

  // Checks on adding/removing (nested) field.

  test("adding field to key should fail") {
    val fieldAddedKeySchema = keySchema.add(StructField("newKey", IntegerType))
    verifyException(keySchema, valueSchema, fieldAddedKeySchema, valueSchema)
  }

  test("adding field to value should fail") {
    val fieldAddedValueSchema = valueSchema.add(StructField("newValue", IntegerType))
    verifyException(keySchema, valueSchema, keySchema, fieldAddedValueSchema)
  }

  test("adding nested field in key should fail") {
    val fieldAddedNestedSchema = structSchema.add(StructField("newNested", IntegerType))
    val newKeySchema = applyNewSchemaToNestedFieldInKey(fieldAddedNestedSchema)
    verifyException(keySchema, valueSchema, newKeySchema, valueSchema)
  }

  test("adding nested field in value should fail") {
    val fieldAddedNestedSchema = structSchema.add(StructField("newNested", IntegerType))
    val newValueSchema = applyNewSchemaToNestedFieldInValue(fieldAddedNestedSchema)
    verifyException(keySchema, valueSchema, keySchema, newValueSchema)
  }

  test("removing field from key should fail") {
    val fieldRemovedKeySchema = StructType(keySchema.dropRight(1))
    verifyException(keySchema, valueSchema, fieldRemovedKeySchema, valueSchema)
  }

  test("removing field from value should fail") {
    val fieldRemovedValueSchema = StructType(valueSchema.drop(1))
    verifyException(keySchema, valueSchema, keySchema, fieldRemovedValueSchema)
  }

  test("removing nested field from key should fail") {
    val fieldRemovedNestedSchema = StructType(structSchema.dropRight(1))
    val newKeySchema = applyNewSchemaToNestedFieldInKey(fieldRemovedNestedSchema)
    verifyException(keySchema, valueSchema, newKeySchema, valueSchema)
  }

  test("removing nested field from value should fail") {
    val fieldRemovedNestedSchema = StructType(structSchema.drop(1))
    val newValueSchema = applyNewSchemaToNestedFieldInValue(fieldRemovedNestedSchema)
    verifyException(keySchema, valueSchema, keySchema, newValueSchema)
  }

  // Checks on changing type of (nested) field.

  test("changing the type of field in key should fail") {
    val typeChangedKeySchema = StructType(keySchema.map(_.copy(dataType = TimestampType)))
    verifyException(keySchema, valueSchema, typeChangedKeySchema, valueSchema)
  }

  test("changing the type of field in value should fail") {
    val typeChangedValueSchema = StructType(valueSchema.map(_.copy(dataType = TimestampType)))
    verifyException(keySchema, valueSchema, keySchema, typeChangedValueSchema)
  }

  test("changing the type of nested field in key should fail") {
    val typeChangedNestedSchema = StructType(structSchema.map(_.copy(dataType = TimestampType)))
    val newKeySchema = applyNewSchemaToNestedFieldInKey(typeChangedNestedSchema)
    verifyException(keySchema, valueSchema, newKeySchema, valueSchema)
  }

  test("changing the type of nested field in value should fail") {
    val typeChangedNestedSchema = StructType(structSchema.map(_.copy(dataType = TimestampType)))
    val newValueSchema = applyNewSchemaToNestedFieldInValue(typeChangedNestedSchema)
    verifyException(keySchema, valueSchema, keySchema, newValueSchema)
  }

  // Checks on changing nullability of (nested) field.
  // Note that these tests have different format of the test name compared to others, since it was
  // misleading to understand the assignment as the opposite way.

  test("storing non-nullable column into nullable column in key should be allowed") {
    val nonNullChangedKeySchema = StructType(keySchema.map(_.copy(nullable = false)))
    verifySuccess(keySchema, valueSchema, nonNullChangedKeySchema, valueSchema)
  }

  test("storing non-nullable column into nullable column in value schema should be allowed") {
    val nonNullChangedValueSchema = StructType(valueSchema.map(_.copy(nullable = false)))
    verifySuccess(keySchema, valueSchema, keySchema, nonNullChangedValueSchema)
  }

  test("storing non-nullable into nullable in nested field in key should be allowed") {
    val typeChangedNestedSchema = StructType(structSchema.map(_.copy(nullable = false)))
    val newKeySchema = applyNewSchemaToNestedFieldInKey(typeChangedNestedSchema)
    verifySuccess(keySchema, valueSchema, newKeySchema, valueSchema)
  }

  test("storing non-nullable into nullable in nested field in value should be allowed") {
    val typeChangedNestedSchema = StructType(structSchema.map(_.copy(nullable = false)))
    val newValueSchema = applyNewSchemaToNestedFieldInValue(typeChangedNestedSchema)
    verifySuccess(keySchema, valueSchema, keySchema, newValueSchema)
  }

  test("storing nullable column into non-nullable column in key should fail") {
    val nonNullChangedKeySchema = StructType(keySchema.map(_.copy(nullable = false)))
    verifyException(nonNullChangedKeySchema, valueSchema, keySchema, valueSchema)
  }

  test("storing nullable column into non-nullable column in value schema should fail") {
    val nonNullChangedValueSchema = StructType(valueSchema.map(_.copy(nullable = false)))
    verifyException(keySchema, nonNullChangedValueSchema, keySchema, valueSchema)
  }

  test("storing nullable column into non-nullable column in nested field in key should fail") {
    val typeChangedNestedSchema = StructType(structSchema.map(_.copy(nullable = false)))
    val newKeySchema = applyNewSchemaToNestedFieldInKey(typeChangedNestedSchema)
    verifyException(newKeySchema, valueSchema, keySchema, valueSchema)
  }

  test("storing nullable column into non-nullable column in nested field in value should fail") {
    val typeChangedNestedSchema = StructType(structSchema.map(_.copy(nullable = false)))
    val newValueSchema = applyNewSchemaToNestedFieldInValue(typeChangedNestedSchema)
    verifyException(keySchema, newValueSchema, keySchema, valueSchema)
  }

  // Checks on changing name of (nested) field.
  // Changing the name is allowed since it may be possible Spark can make relevant changes from
  // operators/functions by chance. This opens a risk that end users swap two fields having same
  // data type, but there is no way to address both.

  test("changing the name of field in key should be allowed") {
    val newName: StructField => StructField = f => f.copy(name = f.name + "_new")
    val fieldNameChangedKeySchema = StructType(keySchema.map(newName))
    verifySuccess(keySchema, valueSchema, fieldNameChangedKeySchema, valueSchema)
  }

  test("changing the name of field in value should be allowed") {
    val newName: StructField => StructField = f => f.copy(name = f.name + "_new")
    val fieldNameChangedValueSchema = StructType(valueSchema.map(newName))
    verifySuccess(keySchema, valueSchema, keySchema, fieldNameChangedValueSchema)
  }

  test("changing the name of nested field in key should be allowed") {
    val newName: StructField => StructField = f => f.copy(name = f.name + "_new")
    val newNestedFieldsSchema = StructType(structSchema.map(newName))
    val fieldNameChangedKeySchema = applyNewSchemaToNestedFieldInKey(newNestedFieldsSchema)
    verifySuccess(keySchema, valueSchema, fieldNameChangedKeySchema, valueSchema)
  }

  test("changing the name of nested field in value should be allowed") {
    val newName: StructField => StructField = f => f.copy(name = f.name + "_new")
    val newNestedFieldsSchema = StructType(structSchema.map(newName))
    val fieldNameChangedValueSchema = applyNewSchemaToNestedFieldInValue(newNestedFieldsSchema)
    verifySuccess(keySchema, valueSchema, keySchema, fieldNameChangedValueSchema)
  }

  test("SPARK-35602: checking for long length schema") {
    verifySuccess(longKeySchema, longValueSchema, longKeySchema, longValueSchema)
    verifySuccess(
      keySchema65535Bytes, valueSchema65535Bytes, keySchema65535Bytes, valueSchema65535Bytes)
  }

  test("SPARK-35602: checking for compatibility with schema version 1") {
    val dir = newDir()
    val queryId = UUID.randomUUID()
    val providerId = StateStoreProviderId(
      StateStoreId(dir, opId, partitionId), queryId)
    val checker = new StateSchemaCompatibilityChecker(providerId, hadoopConf)
    checker.createSchemaFile(keySchema, valueSchema,
      SchemaHelper.SchemaWriter.createSchemaWriter(1))
    val (resultKeySchema, resultValueSchema) = checker.readSchemaFile()

    assert((resultKeySchema, resultValueSchema) === (keySchema, valueSchema))
  }

  test("SPARK-39650: ignore value schema on compatibility check") {
    val typeChangedValueSchema = StructType(valueSchema.map(_.copy(dataType = TimestampType)))
    verifySuccess(keySchema, valueSchema, keySchema, typeChangedValueSchema,
      ignoreValueSchema = true)

    val typeChangedKeySchema = StructType(keySchema.map(_.copy(dataType = TimestampType)))
    verifyException(keySchema, valueSchema, typeChangedKeySchema, valueSchema,
      ignoreValueSchema = true)
  }

  test("SPARK-47776: checking for compatibility with collation change in key") {
    verifyException(keySchema, valueSchema, keySchemaWithCollation, valueSchema,
      ignoreValueSchema = false)
    verifyException(keySchemaWithCollation, valueSchema, keySchema, valueSchema,
      ignoreValueSchema = false)
  }

  test("SPARK-47776: checking for compatibility with collation change in value") {
    verifyException(keySchema, valueSchema, keySchema, valueSchemaWithCollation,
      ignoreValueSchema = false)
    verifyException(keySchema, valueSchemaWithCollation, keySchema, valueSchema,
      ignoreValueSchema = false)
  }

  private def applyNewSchemaToNestedFieldInKey(newNestedSchema: StructType): StructType = {
    applyNewSchemaToNestedField(keySchema, newNestedSchema, "key3")
  }

  private def applyNewSchemaToNestedFieldInValue(newNestedSchema: StructType): StructType = {
    applyNewSchemaToNestedField(valueSchema, newNestedSchema, "value3")
  }

  private def applyNewSchemaToNestedField(
      originSchema: StructType,
      newNestedSchema: StructType,
      fieldName: String): StructType = {
    val newFields = originSchema.map { field =>
      if (field.name == fieldName) {
        field.copy(dataType = newNestedSchema)
      } else {
        field
      }
    }
    StructType(newFields)
  }

  private def runSchemaChecker(
      dir: String,
      queryId: UUID,
      newKeySchema: StructType,
      newValueSchema: StructType,
      ignoreValueSchema: Boolean): Unit = {
    // in fact, Spark doesn't support online state schema change, so need to check
    // schema only once for each running of JVM
    val providerId = StateStoreProviderId(
      StateStoreId(dir, opId, partitionId), queryId)

    new StateSchemaCompatibilityChecker(providerId, hadoopConf)
      .check(newKeySchema, newValueSchema, ignoreValueSchema = ignoreValueSchema)
  }

  private def verifyException(
      oldKeySchema: StructType,
      oldValueSchema: StructType,
      newKeySchema: StructType,
      newValueSchema: StructType,
      ignoreValueSchema: Boolean = false): Unit = {
    val dir = newDir()
    val queryId = UUID.randomUUID()
    runSchemaChecker(dir, queryId, oldKeySchema, oldValueSchema,
      ignoreValueSchema = ignoreValueSchema)

    val e = intercept[StateSchemaNotCompatible] {
      runSchemaChecker(dir, queryId, newKeySchema, newValueSchema,
        ignoreValueSchema = ignoreValueSchema)
    }

    assert(e.getMessage.contains("Provided schema doesn't match to the schema for existing state!"))
    assert(e.getMessage.contains(newKeySchema.toString()))
    assert(e.getMessage.contains(oldKeySchema.toString()))

    if (ignoreValueSchema) {
      assert(!e.getMessage.contains(newValueSchema.toString()))
      assert(!e.getMessage.contains(oldValueSchema.toString()))
    } else {
      assert(e.getMessage.contains(newValueSchema.toString()))
      assert(e.getMessage.contains(oldValueSchema.toString()))
    }
  }

  private def verifySuccess(
      oldKeySchema: StructType,
      oldValueSchema: StructType,
      newKeySchema: StructType,
      newValueSchema: StructType,
      ignoreValueSchema: Boolean = false): Unit = {
    val dir = newDir()
    val queryId = UUID.randomUUID()
    runSchemaChecker(dir, queryId, oldKeySchema, oldValueSchema,
      ignoreValueSchema = ignoreValueSchema)
    runSchemaChecker(dir, queryId, newKeySchema, newValueSchema,
      ignoreValueSchema = ignoreValueSchema)
  }
}
