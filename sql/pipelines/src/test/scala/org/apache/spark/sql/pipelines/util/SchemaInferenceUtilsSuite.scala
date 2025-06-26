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

package org.apache.spark.sql.pipelines.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.types._

class SchemaInferenceUtilsSuite extends SparkFunSuite {

  test("determineColumnChanges - adding new columns") {
    val currentSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType)

    val targetSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType)
      .add("age", IntegerType)
      .add("email", StringType, nullable = true, "Email address")

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)

    // Should have 2 changes - adding 'age' and 'email' columns
    assert(changes.length === 2)

    // Verify the changes are of the correct type and have the right properties
    val ageChange = changes
      .find {
        case addCol: TableChange.AddColumn => addCol.fieldNames().sameElements(Array("age"))
        case _ => false
      }
      .get
      .asInstanceOf[TableChange.AddColumn]

    val emailChange = changes
      .find {
        case addCol: TableChange.AddColumn => addCol.fieldNames().sameElements(Array("email"))
        case _ => false
      }
      .get
      .asInstanceOf[TableChange.AddColumn]

    // Verify age column properties
    assert(ageChange.dataType() === IntegerType)
    assert(ageChange.isNullable() === true) // Default nullable is true
    assert(ageChange.comment() === null)

    // Verify email column properties
    assert(emailChange.dataType() === StringType)
    assert(emailChange.isNullable() === true)
    assert(emailChange.comment() === "Email address")
  }

  test("determineColumnChanges - updating column types") {
    val currentSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("amount", DoubleType)
      .add("timestamp", TimestampType)

    val targetSchema = new StructType()
      .add("id", LongType, nullable = false) // Changed type from Int to Long
      .add("amount", DecimalType(10, 2)) // Changed type from Double to Decimal
      .add("timestamp", TimestampType) // No change

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)

    // Should have 2 changes - updating 'id' and 'amount' column types
    assert(changes.length === 2)

    // Verify the changes are of the correct type
    val idChange = changes
      .find {
        case update: TableChange.UpdateColumnType => update.fieldNames().sameElements(Array("id"))
        case _ => false
      }
      .get
      .asInstanceOf[TableChange.UpdateColumnType]

    val amountChange = changes
      .find {
        case update: TableChange.UpdateColumnType =>
          update.fieldNames().sameElements(Array("amount"))
        case _ => false
      }
      .get
      .asInstanceOf[TableChange.UpdateColumnType]

    // Verify the new data types
    assert(idChange.newDataType() === LongType)
    assert(amountChange.newDataType() === DecimalType(10, 2))
  }

  test("determineColumnChanges - updating nullability and comments") {
    val currentSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType, nullable = true)
      .add("description", StringType, nullable = true, "Item description")

    val targetSchema = new StructType()
      .add("id", IntegerType, nullable = true) // Changed nullability
      .add("name", StringType, nullable = false) // Changed nullability
      .add("description", StringType, nullable = true, "Product description") // Changed comment

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)

    // Should have 3 changes - updating nullability for 'id' and 'name', and comment for
    // 'description'
    assert(changes.length === 3)

    // Verify the nullability changes
    val idNullabilityChange = changes
      .find {
        case update: TableChange.UpdateColumnNullability =>
          update.fieldNames().sameElements(Array("id"))
        case _ => false
      }
      .get
      .asInstanceOf[TableChange.UpdateColumnNullability]

    val nameNullabilityChange = changes
      .find {
        case update: TableChange.UpdateColumnNullability =>
          update.fieldNames().sameElements(Array("name"))
        case _ => false
      }
      .get
      .asInstanceOf[TableChange.UpdateColumnNullability]

    // Verify the comment change
    val descriptionCommentChange = changes
      .find {
        case update: TableChange.UpdateColumnComment =>
          update.fieldNames().sameElements(Array("description"))
        case _ => false
      }
      .get
      .asInstanceOf[TableChange.UpdateColumnComment]

    // Verify the new nullability values
    assert(idNullabilityChange.nullable() === true)
    assert(nameNullabilityChange.nullable() === false)

    // Verify the new comment
    assert(descriptionCommentChange.newComment() === "Product description")
  }

  test("determineColumnChanges - complex changes") {
    val currentSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType)
      .add("old_field", BooleanType)

    val targetSchema = new StructType()
      .add("id", LongType, nullable = true) // Changed type and nullability
      // Added comment and changed nullability
      .add("name", StringType, nullable = false, "Full name")
      .add("new_field", StringType) // New field

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)

    // Should have these changes:
    // 1. Update id type
    // 2. Update id nullability
    // 3. Update name nullability
    // 4. Update name comment
    // 5. Add new_field
    // 6. Remove old_field
    assert(changes.length === 6)

    // Count the types of changes
    val typeChanges = changes.collect { case _: TableChange.UpdateColumnType => 1 }.size
    val nullabilityChanges = changes.collect {
      case _: TableChange.UpdateColumnNullability => 1
    }.size
    val commentChanges = changes.collect { case _: TableChange.UpdateColumnComment => 1 }.size
    val addColumnChanges = changes.collect { case _: TableChange.AddColumn => 1 }.size

    assert(typeChanges === 1)
    assert(nullabilityChanges === 2)
    assert(commentChanges === 1)
    assert(addColumnChanges === 1)
  }

  test("determineColumnChanges - no changes") {
    val schema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType)
      .add("timestamp", TimestampType)

    // Same schema, no changes expected
    val changes = SchemaInferenceUtils.diffSchemas(schema, schema)
    assert(changes.isEmpty)
  }

  test("determineColumnChanges - deleting columns") {
    val currentSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType)
      .add("age", IntegerType)
      .add("email", StringType)
      .add("phone", StringType)

    val targetSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType)
      // age, email, and phone columns are removed

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)

    // Should have 3 changes - deleting 'age', 'email', and 'phone' columns
    assert(changes.length === 3)

    // Verify all changes are DeleteColumn operations
    val deleteChanges = changes.collect { case dc: TableChange.DeleteColumn => dc }
    assert(deleteChanges.length === 3)

    // Verify the specific columns being deleted
    val columnNames = deleteChanges.map(_.fieldNames()(0)).toSet
    assert(columnNames === Set("age", "email", "phone"))
  }

  test("determineColumnChanges - mixed additions and deletions") {
    val currentSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("first_name", StringType)
      .add("last_name", StringType)
      .add("age", IntegerType)

    val targetSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("full_name", StringType) // New column
      .add("email", StringType) // New column
      .add("age", IntegerType) // Unchanged
      // first_name and last_name are removed

    val changes = SchemaInferenceUtils.diffSchemas(currentSchema, targetSchema)

    // Should have 4 changes:
    // - 2 additions (full_name, email)
    // - 2 deletions (first_name, last_name)
    assert(changes.length === 4)

    // Count the types of changes
    val addChanges = changes.collect { case ac: TableChange.AddColumn => ac }
    val deleteChanges = changes.collect { case dc: TableChange.DeleteColumn => dc }

    assert(addChanges.length === 2)
    assert(deleteChanges.length === 2)

    // Verify the specific columns being added and deleted
    val addedColumnNames = addChanges.map(_.fieldNames()(0)).toSet
    val deletedColumnNames = deleteChanges.map(_.fieldNames()(0)).toSet

    assert(addedColumnNames === Set("full_name", "email"))
    assert(deletedColumnNames === Set("first_name", "last_name"))
  }
}
