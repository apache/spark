/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.cli;

import java.sql.DatabaseMetaData;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hive.service.cli.thrift.TTypeId;

/**
 * Type.
 *
 */
public enum Type {
  NULL_TYPE("VOID",
      java.sql.Types.NULL,
      TTypeId.NULL_TYPE),
  BOOLEAN_TYPE("BOOLEAN",
      java.sql.Types.BOOLEAN,
      TTypeId.BOOLEAN_TYPE),
  TINYINT_TYPE("TINYINT",
      java.sql.Types.TINYINT,
      TTypeId.TINYINT_TYPE),
  SMALLINT_TYPE("SMALLINT",
      java.sql.Types.SMALLINT,
      TTypeId.SMALLINT_TYPE),
  INT_TYPE("INT",
      java.sql.Types.INTEGER,
      TTypeId.INT_TYPE),
  BIGINT_TYPE("BIGINT",
      java.sql.Types.BIGINT,
      TTypeId.BIGINT_TYPE),
  FLOAT_TYPE("FLOAT",
      java.sql.Types.FLOAT,
      TTypeId.FLOAT_TYPE),
  DOUBLE_TYPE("DOUBLE",
      java.sql.Types.DOUBLE,
      TTypeId.DOUBLE_TYPE),
  STRING_TYPE("STRING",
      java.sql.Types.VARCHAR,
      TTypeId.STRING_TYPE),
  CHAR_TYPE("CHAR",
      java.sql.Types.CHAR,
      TTypeId.CHAR_TYPE,
      true, false, false),
  VARCHAR_TYPE("VARCHAR",
      java.sql.Types.VARCHAR,
      TTypeId.VARCHAR_TYPE,
      true, false, false),
  DATE_TYPE("DATE",
      java.sql.Types.DATE,
      TTypeId.DATE_TYPE),
  TIMESTAMP_TYPE("TIMESTAMP",
      java.sql.Types.TIMESTAMP,
      TTypeId.TIMESTAMP_TYPE),
  INTERVAL_YEAR_MONTH_TYPE("INTERVAL_YEAR_MONTH",
      java.sql.Types.OTHER,
      TTypeId.INTERVAL_YEAR_MONTH_TYPE),
  INTERVAL_DAY_TIME_TYPE("INTERVAL_DAY_TIME",
      java.sql.Types.OTHER,
      TTypeId.INTERVAL_DAY_TIME_TYPE),
  BINARY_TYPE("BINARY",
      java.sql.Types.BINARY,
      TTypeId.BINARY_TYPE),
  DECIMAL_TYPE("DECIMAL",
      java.sql.Types.DECIMAL,
      TTypeId.DECIMAL_TYPE,
      true, false, false),
  ARRAY_TYPE("ARRAY",
      java.sql.Types.ARRAY,
      TTypeId.ARRAY_TYPE,
      true, true),
  MAP_TYPE("MAP",
      java.sql.Types.JAVA_OBJECT,
      TTypeId.MAP_TYPE,
      true, true),
  STRUCT_TYPE("STRUCT",
      java.sql.Types.STRUCT,
      TTypeId.STRUCT_TYPE,
      true, false),
  UNION_TYPE("UNIONTYPE",
      java.sql.Types.OTHER,
      TTypeId.UNION_TYPE,
      true, false),
  USER_DEFINED_TYPE("USER_DEFINED",
      java.sql.Types.OTHER,
      TTypeId.USER_DEFINED_TYPE,
      true, false);

  private final String name;
  private final TTypeId tType;
  private final int javaSQLType;
  private final boolean isQualified;
  private final boolean isComplex;
  private final boolean isCollection;

  Type(String name, int javaSQLType, TTypeId tType, boolean isQualified, boolean isComplex, boolean isCollection) {
    this.name = name;
    this.javaSQLType = javaSQLType;
    this.tType = tType;
    this.isQualified = isQualified;
    this.isComplex = isComplex;
    this.isCollection = isCollection;
  }

  Type(String name, int javaSQLType, TTypeId tType, boolean isComplex, boolean isCollection) {
    this(name, javaSQLType, tType, false, isComplex, isCollection);
  }

  Type(String name, int javaSqlType, TTypeId tType) {
    this(name, javaSqlType, tType, false, false, false);
  }

  public boolean isPrimitiveType() {
    return !isComplex;
  }

  public boolean isQualifiedType() {
    return isQualified;
  }

  public boolean isComplexType() {
    return isComplex;
  }

  public boolean isCollectionType() {
    return isCollection;
  }

  public static Type getType(TTypeId tType) {
    for (Type type : values()) {
      if (tType.equals(type.tType)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unregonized Thrift TTypeId value: " + tType);
  }

  public static Type getType(String name) {
    if (name == null) {
      throw new IllegalArgumentException("Invalid type name: null");
    }
    for (Type type : values()) {
      if (name.equalsIgnoreCase(type.name)) {
        return type;
      } else if (type.isQualifiedType() || type.isComplexType()) {
        if (name.toUpperCase().startsWith(type.name)) {
            return type;
        }
      }
    }
    throw new IllegalArgumentException("Unrecognized type name: " + name);
  }

  /**
   * Radix for this type (typically either 2 or 10)
   * Null is returned for data types where this is not applicable.
   */
  public Integer getNumPrecRadix() {
    if (this.isNumericType()) {
      return 10;
    }
    return null;
  }

  /**
   * Maximum precision for numeric types.
   * Returns null for non-numeric types.
   * @return
   */
  public Integer getMaxPrecision() {
    switch (this) {
    case TINYINT_TYPE:
      return 3;
    case SMALLINT_TYPE:
      return 5;
    case INT_TYPE:
      return 10;
    case BIGINT_TYPE:
      return 19;
    case FLOAT_TYPE:
      return 7;
    case DOUBLE_TYPE:
      return 15;
    case DECIMAL_TYPE:
      return HiveDecimal.MAX_PRECISION;
    default:
      return null;
    }
  }

  public boolean isNumericType() {
    switch (this) {
    case TINYINT_TYPE:
    case SMALLINT_TYPE:
    case INT_TYPE:
    case BIGINT_TYPE:
    case FLOAT_TYPE:
    case DOUBLE_TYPE:
    case DECIMAL_TYPE:
      return true;
    default:
      return false;
    }
  }

  /**
   * Prefix used to quote a literal of this type (may be null)
   */
  public String getLiteralPrefix() {
    return null;
  }

  /**
   * Suffix used to quote a literal of this type (may be null)
   * @return
   */
  public String getLiteralSuffix() {
    return null;
  }

  /**
   * Can you use NULL for this type?
   * @return
   * DatabaseMetaData.typeNoNulls - does not allow NULL values
   * DatabaseMetaData.typeNullable - allows NULL values
   * DatabaseMetaData.typeNullableUnknown - nullability unknown
   */
  public Short getNullable() {
    // All Hive types are nullable
    return DatabaseMetaData.typeNullable;
  }

  /**
   * Is the type case sensitive?
   * @return
   */
  public Boolean isCaseSensitive() {
    switch (this) {
    case STRING_TYPE:
      return true;
    default:
      return false;
    }
  }

  /**
   * Parameters used in creating the type (may be null)
   * @return
   */
  public String getCreateParams() {
    return null;
  }

  /**
   * Can you use WHERE based on this type?
   * @return
   * DatabaseMetaData.typePredNone - No support
   * DatabaseMetaData.typePredChar - Only support with WHERE .. LIKE
   * DatabaseMetaData.typePredBasic - Supported except for WHERE .. LIKE
   * DatabaseMetaData.typeSearchable - Supported for all WHERE ..
   */
  public Short getSearchable() {
    if (isPrimitiveType()) {
      return DatabaseMetaData.typeSearchable;
    }
    return DatabaseMetaData.typePredNone;
  }

  /**
   * Is this type unsigned?
   * @return
   */
  public Boolean isUnsignedAttribute() {
    if (isNumericType()) {
      return false;
    }
    return true;
  }

  /**
   * Can this type represent money?
   * @return
   */
  public Boolean isFixedPrecScale() {
    return false;
  }

  /**
   * Can this type be used for an auto-increment value?
   * @return
   */
  public Boolean isAutoIncrement() {
    return false;
  }

  /**
   * Localized version of type name (may be null).
   * @return
   */
  public String getLocalizedName() {
    return null;
  }

  /**
   * Minimum scale supported for this type
   * @return
   */
  public Short getMinimumScale() {
    return 0;
  }

  /**
   * Maximum scale supported for this type
   * @return
   */
  public Short getMaximumScale() {
    return 0;
  }

  public TTypeId toTType() {
    return tType;
  }

  public int toJavaSQLType() {
    return javaSQLType;
  }

  public String getName() {
    return name;
  }
}
