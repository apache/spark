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

package org.apache.spark.sql.catalyst.catalog

import java.util.UUID

import org.json4s._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SchemaUtils

/**
 * A container class to hold all the constraints defined on a table. Scope of the
 * constraint names are at the table level.
 */
case class TableConstraints(
    primaryKey: Option[PrimaryKey] = None,
    foreignKeys: Seq[ForeignKey] = Seq.empty) {

  /**
   * Adds the given constraint to the existing table constraints, after verifying the
   * constraint name is not a duplicate.
   */
  def addConstraint(constraint: TableConstraint, resolver: Resolver): TableConstraints = {
    if ((primaryKey.exists(pk => resolver(pk.constraintName, constraint.constraintName))
      || foreignKeys.exists(fk => resolver(fk.constraintName, constraint.constraintName)))) {
      throw new AnalysisException(
        s"Failed to add constraint, duplicate constraint name '${constraint.constraintName}'")
    }
    constraint match {
      case pk: PrimaryKey =>
        if (primaryKey.nonEmpty) {
          throw new AnalysisException(
            s"Primary key '${primaryKey.get.constraintName}' already exists.")
        }
        this.copy(primaryKey = Option(pk))
      case fk: ForeignKey => this.copy(foreignKeys = foreignKeys :+ fk)
    }
  }
}

object TableConstraints {
  /**
   * Returns a [[TableConstraints]] containing [[PrimaryKey]] or [[ForeignKey]]
   */
  def apply(tableConstraint: TableConstraint): TableConstraints = {
    tableConstraint match {
      case pk: PrimaryKey => TableConstraints(primaryKey = Option(pk))
      case fk: ForeignKey => TableConstraints(foreignKeys = Seq(fk))
    }
  }

  /**
   * Converts constraints represented in Json strings to [[TableConstraints]].
   */
  def fromJson(pkJson: Option[String], fksJson: Seq[String]): TableConstraints = {
    val pk = pkJson.map(pk => PrimaryKey.fromJson(parse(pk)))
    val fks = fksJson.map(fk => ForeignKey.fromJson(parse(fk)))
    TableConstraints(pk, fks)
  }
}

/**
 * Common type representing a table constraint.
 */
sealed trait TableConstraint {
  val constraintName : String
  val keyColumnNames : Seq[String]
}

object TableConstraint {
  private[TableConstraint] val curId = new java.util.concurrent.atomic.AtomicLong(0L)
  private[TableConstraint] val jvmId = UUID.randomUUID()

  /**
   * Generates unique constraint name to use when adding table constraints,
   * if user does not specify a name. The `curId` field is unique within a given JVM,
   * while the `jvmId` is used to uniquely identify JVMs.
   */
  def generateConstraintName(constraintType: String = "constraint"): String = {
    s"${constraintType}_${jvmId}_${curId.getAndIncrement()}"
  }

  def parseColumn(json: JValue): String = json match {
    case JString(name) => name
    case _ => json.toString
  }

  object JSortedObject {
    def unapplySeq(value: JValue): Option[List[(String, JValue)]] = value match {
      case JObject(seq) => Some(seq.toList.sortBy(_._1))
      case _ => None
    }
  }

  /**
   * Returns [[StructField]] for the given column name if it exists in the given schema.
   */
  def findColumnByName(
    schema: StructType, name: String, resolver: Resolver): StructField = {
    schema.fields.collectFirst {
      case field if resolver(field.name, name) => field
    }.getOrElse(throw new AnalysisException(
      s"Invalid column reference '$name', table data schema is '${schema}'"))
  }

  /**
   * Verify the user input constraint information, and add the missing information
   * like the unspecified reference columns that defaults to reference table's primary key.
   */
  def verifyAndBuildConstraint(
      inputConstraint: TableConstraint,
      table: CatalogTable,
      catalog: SessionCatalog,
      resolver: Resolver): TableConstraint = {
    SchemaUtils.checkColumnNameDuplication(
      inputConstraint.keyColumnNames, "in the constraint key definition", resolver)
    // check if the column names are valid non-partition columns.
    val keyColFields = inputConstraint.keyColumnNames
      .map(findColumnByName(table.dataSchema, _, resolver))
    // Constraints are only supported for basic sql types, throw error for any other data types.
    keyColFields.map(_.dataType).foreach {
      case ByteType | ShortType | IntegerType | LongType | FloatType |
           DoubleType | BooleanType | _: DecimalType | TimestampType |
           DateType | StringType | BinaryType =>
      case otherType => throw new UnsupportedOperationException(
        s"Constraints are not supported for ${otherType.simpleString} data type.")
    }
    inputConstraint match {
      case pk: PrimaryKey => pk
      case fk: ForeignKey => ForeignKey.verifyAndBuildForeignKey(fk, table, catalog, resolver)
    }
  }
}

/**
 * A Primary key constraint defined on a table.
 */
case class PrimaryKey(
  constraintName: String,
  keyColumnNames: Seq[String],
  isValidated: Boolean = false,
  isRely: Boolean = false) extends TableConstraint {

  def json: String = compact(render(jsonValue))

  private def jsonValue: JValue = {
    ("id" -> constraintName) ~
      ("keyCols" -> keyColumnNames) ~
      ("rely" -> isRely) ~
      ("validate" -> isValidated)
  }
}

object PrimaryKey {
  import TableConstraint._

  /**
   * Converts the primary key represented in Json string to [[PrimaryKey]].
   */
  def fromJson(json: JValue): PrimaryKey = json match {
    case JSortedObject(
    ("id", JString(id)),
    ("keyCols", JArray(keyCols)),
    ("rely", JBool(rely)),
    ("validate", JBool(validate))
    ) =>
      PrimaryKey(
        constraintName = id,
        keyColumnNames = keyCols.map(parseColumn),
        isValidated = validate,
        isRely = rely)
  }
}

/**
 * A Foreign key defined on a table.
 */
case class ForeignKey(
  constraintName: String,
  keyColumnNames: Seq[String],
  referenceTableIdentifier: TableIdentifier,
  referenceColumnNames: Seq[String] = Seq.empty,
  isValidated: Boolean = false,
  isRely: Boolean = false) extends TableConstraint {

  def json: String = compact(render(jsonValue))

  private def jsonValue: JValue = {
    ("id" -> constraintName) ~
      ("keyCols" -> keyColumnNames) ~
      ("refCols" -> referenceColumnNames) ~
      ("refDb" -> referenceTableIdentifier.database.get) ~
      ("refTable" -> referenceTableIdentifier.table) ~
      ("rely" -> isRely) ~
      ("validate" -> isValidated)
  }
}

object ForeignKey {
  import TableConstraint._
  /**
   * Converts a foreign key represented in Json string to [[ForeignKey]].
   */
  def fromJson(json: JValue): ForeignKey = json match {
    case JSortedObject(
    ("id", JString(id)),
    ("keyCols", JArray(keyCols)),
    ("refCols", JArray(referenceCols)),
    ("refDb", JString(referenceDb)),
    ("refTable", JString(referenceTable)),
    ("rely", JBool(rely)),
    ("validate", JBool(validate))
    ) =>
      ForeignKey(
        constraintName = id,
        keyColumnNames = keyCols.map(parseColumn),
        referenceTableIdentifier = TableIdentifier(referenceTable, Option(referenceDb)),
        referenceColumnNames = referenceCols.map(parseColumn),
        isValidated = validate,
        isRely = rely)
  }

  /**
   * Verify the user specified foreign key information, and add the missing information
   * such as the the unspecified reference columns and the database of the reference table.
   * When user does not specify reference columns, they are set to reference table's
   * primary key columns.
   */
  def verifyAndBuildForeignKey(
      inputFk: ForeignKey,
      table: CatalogTable,
      catalog: SessionCatalog,
      resolver: Resolver): TableConstraint = {
    SchemaUtils.checkColumnNameDuplication(
      inputFk.referenceColumnNames, "in the foreign key references clause", resolver)
    // verify the reference table has a primary key on the foreign key reference columns.
    val refTable = catalog.getTableMetadata(inputFk.referenceTableIdentifier)
    val refPk = refTable.tableConstraints.flatMap(_.primaryKey).getOrElse(
      throw new AnalysisException(
        "Primary key is not defined on the reference table:" + refTable.identifier)
    )
    // Set the reference column names to the referenced table primary key columns,
    // when user does not specify reference column names using references clause.
    val referenceColumnNames = if (inputFk.referenceColumnNames.isEmpty) {
      refPk.keyColumnNames
    } else {
      // size of the reference columns list and the reference table
      // primary key column list should be same
      if (inputFk.referenceColumnNames.size != refPk.keyColumnNames.size) {
        throw new AnalysisException(
          s"""
            |Referencing columns list size ${inputFk.referenceColumnNames.size}
            |is not same as referenced table primary key columns list
            |size ${refPk.keyColumnNames.size}")
           """.stripMargin.replace("\n", " ").trim())
      }

      // reference column names should match primary key columns in the same order
      for ((refColumn, refPkColumn) <- (inputFk.referenceColumnNames zip refPk.keyColumnNames)) {
        if (!resolver(refColumn, refPkColumn)) {
          throw new AnalysisException(
            s"""
              |Referencing columns ${inputFk.referenceColumnNames.mkString("(", ",", ")")} does not
              |match reference table primary key
              |columns ${refPk.keyColumnNames.mkString("(", ",", ")")}
             """.stripMargin.replace("\n", " ").trim())
        }
      }
      inputFk.referenceColumnNames
    }

    // size foreign key columns list and reference key column list should be same
    if (inputFk.keyColumnNames.size != referenceColumnNames.size) {
      throw new AnalysisException(
        s"""
          |Foreign key columns list size ${inputFk.keyColumnNames.size}
          |is not same as referencing columns list size ${referenceColumnNames.size}")
         """.stripMargin.replace("\n", " ").trim())
    }

    // Foreign key columns and referenced primary key columns data types should match
    val keyColFields = inputFk.keyColumnNames.map(findColumnByName(table.dataSchema, _, resolver))
    val refKeyColFields = referenceColumnNames
      .map(findColumnByName(refTable.dataSchema, _, resolver))

    for ((keyField, refKeyField) <- (keyColFields zip refKeyColFields)) {
      if (!keyField.dataType.sameType(refKeyField.dataType)) {
        throw new AnalysisException(
          s"""
            |Foreign key column data type ${keyField.name}:${keyField.dataType.simpleString}
            |does not match with referencing column
            |data type ${refKeyField.name}:${refKeyField.dataType.simpleString}.
           """.stripMargin.replace("\n", " ").trim())
      }
    }
    inputFk.copy(
      referenceColumnNames = referenceColumnNames,
      referenceTableIdentifier = refTable.identifier)
  }
}

