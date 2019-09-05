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

package org.apache.spark.sql.hive.thriftserver.cli

import java.sql.DatabaseMetaData

import org.apache.spark.sql.types.DecimalType

trait Type {

  def getName: String

  def isComplex: Boolean

  def isQualifiedType: Boolean

  def isCollectionType: Boolean

  def isPrimitiveType: Boolean = !isComplex

  def isComplexType: Boolean = isComplex

  /**
   * Radix for this type (typically either 2 or 10)
   * Null is returned for data types where this is not applicable.
   */
  def getNumPrecRadix: Integer = {
    if (this.isNumericType) {
      10
    } else {
      null
    }
  }

  def getMaxPrecision(): Option[Int]

  def isNumericType(): Boolean = false

  /**
   * Prefix used to quote a literal of this type (may be null)
   */
  def getLiteralPrefix: String = null

  /**
   * Suffix used to quote a literal of this type (may be null)
   *
   * @return
   */
  def getLiteralSuffix: String = null

  /**
   * Can you use NULL for this type?
   *
   * @return
   * DatabaseMetaData.typeNoNulls - does not allow NULL values
   * DatabaseMetaData.typeNullable - allows NULL values
   * DatabaseMetaData.typeNullableUnknown - nullability unknown
   */
  def getNullable: Short = {
    // All Hive types are nullable
    DatabaseMetaData.typeNullable.toShort
  }

  /**
   * Is the type case sensitive?
   *
   * @return
   */
  def isCaseSensitive: Boolean = false

  /**
   * Parameters used in creating the type (may be null)
   *
   * @return
   */
  def getCreateParams: String = null

  /**
   * Can you use WHERE based on this type?
   *
   * @return
   * DatabaseMetaData.typePredNone - No support
   * DatabaseMetaData.typePredChar - Only support with WHERE .. LIKE
   * DatabaseMetaData.typePredBasic - Supported except for WHERE .. LIKE
   * DatabaseMetaData.typeSearchable - Supported for all WHERE ..
   */
  def getSearchable: Short = {
    if (isPrimitiveType) {
      DatabaseMetaData.typeSearchable.toShort
    } else {
      DatabaseMetaData.typePredNone.toShort
    }
  }

  /**
   * Is this type unsigned?
   *
   * @return
   */
  def isUnsignedAttribute: Boolean = {
    if (isNumericType) {
      false
    } else {
      true
    }
  }

  /**
   * Can this type represent money?
   *
   * @return
   */
  def isFixedPrecScale: Boolean = false

  /**
   * Can this type be used for an auto-increment value?
   *
   * @return
   */
  def isAutoIncrement: Boolean = false

  /**
   * Localized version of type name (may be null).
   *
   * @return
   */
  def getLocalizedName: String = null

  /**
   * Minimum scale supported for this type
   *
   * @return
   */
  def getMinimumScale: Short = 0.toShort

  /**
   * Maximum scale supported for this type
   *
   * @return
   */
  def getMaximumScale: Short = 0.toShort

  def javaSQLType: Int

  def toJavaSQLType: Int = javaSQLType
}

object Type {

  def types: Seq[Type] =
    Seq(NULL,
      STRING,
      INTEGER,
      BOOLEAN,
      DOUBLE,
      FLOAT,
      DECIMAL,
      LONG,
      BYTE,
      SHORT,
      DATE,
      TIMESTAMP,
      BINARY)

  def getType(name: String): Type = {
    val typeForName: Option[Type] = types.find(_.getName == name)
    if (typeForName.isDefined) {
      typeForName.get
    } else {
      if (Array("ARRAY", "STRUCT", "MAP", "USERDEFINED").contains(name)) {
        STRING
      } else {
        throw new IllegalArgumentException("Unrecognized type name: " + name)
      }
    }
  }

  case object NULL extends Type() {
    override def getName: String = "NULL"

    override def isComplex: Boolean = false

    override def isQualifiedType: Boolean = false

    override def isCollectionType: Boolean = false

    override def getMaxPrecision(): Option[Int] = None

    override def javaSQLType: Int = java.sql.Types.NULL
  }

  case object STRING extends Type {
    override def getName: String = "STRING"

    override def isComplex: Boolean = false

    override def isQualifiedType: Boolean = false

    override def isCollectionType: Boolean = false

    override def getMaxPrecision(): Option[Int] = None

    override def javaSQLType: Int = java.sql.Types.VARCHAR

    override def isCaseSensitive: Boolean = true
  }

  case object INTEGER extends Type {
    override def getName: String = "INT"

    override def isComplex: Boolean = false

    override def isQualifiedType: Boolean = false

    override def isCollectionType: Boolean = false

    override def getMaxPrecision(): Option[Int] = Some(10)

    override def javaSQLType: Int = java.sql.Types.INTEGER

    override def isNumericType(): Boolean = true
  }

  case object BOOLEAN extends Type {
    override def getName: String = "BOOLEAN"

    override def isComplex: Boolean = false

    override def isQualifiedType: Boolean = false

    override def isCollectionType: Boolean = false

    override def getMaxPrecision(): Option[Int] = None

    override def javaSQLType: Int = java.sql.Types.BOOLEAN
  }

  case object DOUBLE extends Type {
    override def getName: String = "DOUBLE"

    override def isComplex: Boolean = false

    override def isQualifiedType: Boolean = false

    override def isCollectionType: Boolean = false

    override def getMaxPrecision(): Option[Int] = Some(15)

    override def javaSQLType: Int = java.sql.Types.DOUBLE

    override def isNumericType(): Boolean = true
  }

  case object FLOAT extends Type {
    override def getName: String = "FLOAT"

    override def isComplex: Boolean = false

    override def isQualifiedType: Boolean = false

    override def isCollectionType: Boolean = false

    override def getMaxPrecision(): Option[Int] = Some(7)

    override def javaSQLType: Int = java.sql.Types.FLOAT

    override def isNumericType(): Boolean = true
  }

  case object DECIMAL extends Type {
    override def getName: String = "DECIMAL"

    override def isComplex: Boolean = false

    override def isQualifiedType: Boolean = true

    override def isCollectionType: Boolean = false

    override def getMaxPrecision(): Option[Int] = Some(DecimalType.MAX_PRECISION)

    override def javaSQLType: Int = java.sql.Types.DECIMAL

    override def isNumericType(): Boolean = true
  }

  case object LONG extends Type {
    override def getName: String = "LONG"

    override def isComplex: Boolean = false

    override def isQualifiedType: Boolean = false

    override def isCollectionType: Boolean = false

    override def getMaxPrecision(): Option[Int] = Some(19)

    override def javaSQLType: Int = java.sql.Types.BIGINT

    override def isNumericType(): Boolean = true
  }

  case object BYTE extends Type {
    override def getName: String = "BYTE"

    override def isComplex: Boolean = false

    override def isQualifiedType: Boolean = false

    override def isCollectionType: Boolean = false

    override def getMaxPrecision(): Option[Int] = None

    override def javaSQLType: Int = java.sql.Types.TINYINT
  }

  case object SHORT extends Type {
    override def getName: String = "SHORT"

    override def isComplex: Boolean = false

    override def isQualifiedType: Boolean = false

    override def isCollectionType: Boolean = false

    override def getMaxPrecision(): Option[Int] = Some(5)

    override def javaSQLType: Int = java.sql.Types.SMALLINT

    override def isNumericType(): Boolean = true
  }

  case object DATE extends Type {
    override def getName: String = "DATE"

    override def isComplex: Boolean = false

    override def isQualifiedType: Boolean = false

    override def isCollectionType: Boolean = false

    override def getMaxPrecision(): Option[Int] = None

    override def javaSQLType: Int = java.sql.Types.DATE
  }

  case object TIMESTAMP extends Type {
    override def getName: String = "TIMESTAMP"

    override def isComplex: Boolean = false

    override def isQualifiedType: Boolean = false

    override def isCollectionType: Boolean = false

    override def getMaxPrecision(): Option[Int] = None

    override def javaSQLType: Int = java.sql.Types.TIMESTAMP
  }

  case object BINARY extends Type {
    override def getName: String = "BINARY"

    override def isComplex: Boolean = false

    override def isQualifiedType: Boolean = false

    override def isCollectionType: Boolean = false

    override def getMaxPrecision(): Option[Int] = None

    override def javaSQLType: Int = java.sql.Types.BINARY
  }

}