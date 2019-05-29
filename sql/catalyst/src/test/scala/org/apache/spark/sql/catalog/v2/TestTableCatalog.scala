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

package org.apache.spark.sql.catalog.v2

import java.util
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalog.v2.TableChange.{AddColumn, DeleteColumn, RemoveProperty, RenameColumn, SetProperty, UpdateColumnComment, UpdateColumnType}
import org.apache.spark.sql.catalog.v2.expressions.Transform
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.sources.v2.{Table, TableCapability}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class TestTableCatalog extends TableCatalog {
  import CatalogV2Implicits._

  private val tables: util.Map[Identifier, Table] = new ConcurrentHashMap[Identifier, Table]()
  private var _name: Option[String] = None

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    _name = Some(name)
  }

  override def name: String = _name.get

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    tables.keySet.asScala.filter(_.namespace.sameElements(namespace)).toArray
  }

  override def loadTable(ident: Identifier): Table = {
    Option(tables.get(ident)) match {
      case Some(table) =>
        table
      case _ =>
        throw new NoSuchTableException(ident)
    }
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {

    if (tables.containsKey(ident)) {
      throw new TableAlreadyExistsException(ident)
    }

    if (partitions.nonEmpty) {
      throw new UnsupportedOperationException(
        s"Catalog $name: Partitioned tables are not supported")
    }

    val table = InMemoryTable(ident.quoted, schema, properties)

    tables.put(ident, table)

    table
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val table = loadTable(ident)
    val properties = TestTableCatalog.applyPropertiesChanges(table.properties, changes)
    val schema = TestTableCatalog.applySchemaChanges(table.schema, changes)
    val newTable = InMemoryTable(table.name, schema, properties)

    tables.put(ident, newTable)

    newTable
  }

  override def dropTable(ident: Identifier): Boolean = Option(tables.remove(ident)).isDefined
}

object TestTableCatalog {
  /**
   * Apply properties changes to a map and return the result.
   */
  def applyPropertiesChanges(
      properties: util.Map[String, String],
      changes: Seq[TableChange]): util.Map[String, String] = {
    val newProperties = new util.HashMap[String, String](properties)

    changes.foreach {
      case set: SetProperty =>
        newProperties.put(set.property, set.value)

      case unset: RemoveProperty =>
        newProperties.remove(unset.property)

      case _ =>
      // ignore non-property changes
    }

    Collections.unmodifiableMap(newProperties)
  }

  /**
   * Apply schema changes to a schema and return the result.
   */
  def applySchemaChanges(schema: StructType, changes: Seq[TableChange]): StructType = {
    changes.foldLeft(schema) { (schema, change) =>
      change match {
        case add: AddColumn =>
          add.fieldNames match {
            case Array(name) =>
              val newField = StructField(name, add.dataType, nullable = add.isNullable)
              Option(add.comment) match {
                case Some(comment) =>
                  schema.add(newField.withComment(comment))
                case _ =>
                  schema.add(newField)
              }

            case names =>
              replace(schema, names.init, parent => parent.dataType match {
                case parentType: StructType =>
                  val field = StructField(names.last, add.dataType, nullable = add.isNullable)
                  val newParentType = Option(add.comment) match {
                    case Some(comment) =>
                      parentType.add(field.withComment(comment))
                    case None =>
                      parentType.add(field)
                  }

                  Some(StructField(parent.name, newParentType, parent.nullable, parent.metadata))

                case _ =>
                  throw new IllegalArgumentException(s"Not a struct: ${names.init.last}")
              })
          }

        case rename: RenameColumn =>
          replace(schema, rename.fieldNames, field =>
            Some(StructField(rename.newName, field.dataType, field.nullable, field.metadata)))

        case update: UpdateColumnType =>
          replace(schema, update.fieldNames, field => {
            if (!update.isNullable && field.nullable) {
              throw new IllegalArgumentException(
                s"Cannot change optional column to required: $field.name")
            }
            Some(StructField(field.name, update.newDataType, update.isNullable, field.metadata))
          })

        case update: UpdateColumnComment =>
          replace(schema, update.fieldNames, field =>
            Some(field.withComment(update.newComment)))

        case delete: DeleteColumn =>
          replace(schema, delete.fieldNames, _ => None)

        case _ =>
          // ignore non-schema changes
          schema
      }
    }
  }

  private def replace(
      struct: StructType,
      path: Seq[String],
      update: StructField => Option[StructField]): StructType = {

    val pos = struct.getFieldIndex(path.head)
        .getOrElse(throw new IllegalArgumentException(s"Cannot find field: ${path.head}"))
    val field = struct.fields(pos)
    val replacement: Option[StructField] = if (path.tail.isEmpty) {
      update(field)
    } else {
      field.dataType match {
        case nestedStruct: StructType =>
          val updatedType: StructType = replace(nestedStruct, path.tail, update)
          Some(StructField(field.name, updatedType, field.nullable, field.metadata))
        case _ =>
          throw new IllegalArgumentException(s"Not a struct: ${path.head}")
      }
    }

    val newFields = struct.fields.zipWithIndex.flatMap {
      case (_, index) if pos == index =>
        replacement
      case (other, _) =>
        Some(other)
    }

    new StructType(newFields)
  }
}

case class InMemoryTable(
    name: String,
    schema: StructType,
    override val properties: util.Map[String, String]) extends Table {
  override def partitioning: Array[Transform] = Array.empty
  override def capabilities: util.Set[TableCapability] = InMemoryTable.CAPABILITIES
}

object InMemoryTable {
  val CAPABILITIES: util.Set[TableCapability] = Set.empty[TableCapability].asJava
}
