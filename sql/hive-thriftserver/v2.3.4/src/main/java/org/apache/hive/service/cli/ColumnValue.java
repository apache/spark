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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hive.service.cli.thrift.TBoolValue;
import org.apache.hive.service.cli.thrift.TByteValue;
import org.apache.hive.service.cli.thrift.TColumnValue;
import org.apache.hive.service.cli.thrift.TDoubleValue;
import org.apache.hive.service.cli.thrift.TI16Value;
import org.apache.hive.service.cli.thrift.TI32Value;
import org.apache.hive.service.cli.thrift.TI64Value;
import org.apache.hive.service.cli.thrift.TStringValue;

/**
 * Protocols before HIVE_CLI_SERVICE_PROTOCOL_V6 (used by RowBasedSet)
 *
 */
public class ColumnValue {

  private static TColumnValue booleanValue(Boolean value) {
    TBoolValue tBoolValue = new TBoolValue();
    if (value != null) {
      tBoolValue.setValue(value);
    }
    return TColumnValue.boolVal(tBoolValue);
  }

  private static TColumnValue byteValue(Byte value) {
    TByteValue tByteValue = new TByteValue();
    if (value != null) {
      tByteValue.setValue(value);
    }
    return TColumnValue.byteVal(tByteValue);
  }

  private static TColumnValue shortValue(Short value) {
    TI16Value tI16Value = new TI16Value();
    if (value != null) {
      tI16Value.setValue(value);
    }
    return TColumnValue.i16Val(tI16Value);
  }

  private static TColumnValue intValue(Integer value) {
    TI32Value tI32Value = new TI32Value();
    if (value != null) {
      tI32Value.setValue(value);
    }
    return TColumnValue.i32Val(tI32Value);
  }

  private static TColumnValue longValue(Long value) {
    TI64Value tI64Value = new TI64Value();
    if (value != null) {
      tI64Value.setValue(value);
    }
    return TColumnValue.i64Val(tI64Value);
  }

  private static TColumnValue floatValue(Float value) {
    TDoubleValue tDoubleValue = new TDoubleValue();
    if (value != null) {
      tDoubleValue.setValue(value);
    }
    return TColumnValue.doubleVal(tDoubleValue);
  }

  private static TColumnValue doubleValue(Double value) {
    TDoubleValue tDoubleValue = new TDoubleValue();
    if (value != null) {
      tDoubleValue.setValue(value);
    }
    return TColumnValue.doubleVal(tDoubleValue);
  }

  private static TColumnValue stringValue(String value) {
    TStringValue tStringValue = new TStringValue();
    if (value != null) {
      tStringValue.setValue(value);
    }
    return TColumnValue.stringVal(tStringValue);
  }

  private static TColumnValue stringValue(HiveChar value) {
    TStringValue tStringValue = new TStringValue();
    if (value != null) {
      tStringValue.setValue(value.toString());
    }
    return TColumnValue.stringVal(tStringValue);
  }

  private static TColumnValue stringValue(HiveVarchar value) {
    TStringValue tStringValue = new TStringValue();
    if (value != null) {
      tStringValue.setValue(value.toString());
    }
    return TColumnValue.stringVal(tStringValue);
  }

  private static TColumnValue dateValue(Date value) {
    TStringValue tStringValue = new TStringValue();
    if (value != null) {
      tStringValue.setValue(value.toString());
    }
    return new TColumnValue(TColumnValue.stringVal(tStringValue));
  }

  private static TColumnValue timestampValue(Timestamp value) {
    TStringValue tStringValue = new TStringValue();
    if (value != null) {
      tStringValue.setValue(value.toString());
    }
    return TColumnValue.stringVal(tStringValue);
  }

  private static TColumnValue stringValue(HiveDecimal value) {
    TStringValue tStrValue = new TStringValue();
    if (value != null) {
      tStrValue.setValue(value.toString());
    }
    return TColumnValue.stringVal(tStrValue);
  }

  private static TColumnValue stringValue(HiveIntervalYearMonth value) {
    TStringValue tStrValue = new TStringValue();
    if (value != null) {
      tStrValue.setValue(value.toString());
    }
    return TColumnValue.stringVal(tStrValue);
  }

  private static TColumnValue stringValue(HiveIntervalDayTime value) {
    TStringValue tStrValue = new TStringValue();
    if (value != null) {
      tStrValue.setValue(value.toString());
    }
    return TColumnValue.stringVal(tStrValue);
  }

  public static TColumnValue toTColumnValue(Type type, Object value) {
    switch (type) {
    case BOOLEAN_TYPE:
      return booleanValue((Boolean)value);
    case TINYINT_TYPE:
      return byteValue((Byte)value);
    case SMALLINT_TYPE:
      return shortValue((Short)value);
    case INT_TYPE:
      return intValue((Integer)value);
    case BIGINT_TYPE:
      return longValue((Long)value);
    case FLOAT_TYPE:
      return floatValue((Float)value);
    case DOUBLE_TYPE:
      return doubleValue((Double)value);
    case STRING_TYPE:
      return stringValue((String)value);
    case CHAR_TYPE:
      return stringValue((HiveChar)value);
    case VARCHAR_TYPE:
      return stringValue((HiveVarchar)value);
    case DATE_TYPE:
      return dateValue((Date)value);
    case TIMESTAMP_TYPE:
      return timestampValue((Timestamp)value);
    case INTERVAL_YEAR_MONTH_TYPE:
      return stringValue((HiveIntervalYearMonth) value);
    case INTERVAL_DAY_TIME_TYPE:
      return stringValue((HiveIntervalDayTime) value);
    case DECIMAL_TYPE:
      return stringValue(((HiveDecimal)value));
    case BINARY_TYPE:
      return stringValue((String)value);
    case ARRAY_TYPE:
    case MAP_TYPE:
    case STRUCT_TYPE:
    case UNION_TYPE:
    case USER_DEFINED_TYPE:
      return stringValue((String)value);
    default:
      return null;
    }
  }

  private static Boolean getBooleanValue(TBoolValue tBoolValue) {
    if (tBoolValue.isSetValue()) {
      return tBoolValue.isValue();
    }
    return null;
  }

  private static Byte getByteValue(TByteValue tByteValue) {
    if (tByteValue.isSetValue()) {
      return tByteValue.getValue();
    }
    return null;
  }

  private static Short getShortValue(TI16Value tI16Value) {
    if (tI16Value.isSetValue()) {
      return tI16Value.getValue();
    }
    return null;
  }

  private static Integer getIntegerValue(TI32Value tI32Value) {
    if (tI32Value.isSetValue()) {
      return tI32Value.getValue();
    }
    return null;
  }

  private static Long getLongValue(TI64Value tI64Value) {
    if (tI64Value.isSetValue()) {
      return tI64Value.getValue();
    }
    return null;
  }

  private static Double getDoubleValue(TDoubleValue tDoubleValue) {
    if (tDoubleValue.isSetValue()) {
      return tDoubleValue.getValue();
    }
    return null;
  }

  private static String getStringValue(TStringValue tStringValue) {
    if (tStringValue.isSetValue()) {
      return tStringValue.getValue();
    }
    return null;
  }

  private static Date getDateValue(TStringValue tStringValue) {
    if (tStringValue.isSetValue()) {
      return Date.valueOf(tStringValue.getValue());
    }
    return null;
  }

  private static Timestamp getTimestampValue(TStringValue tStringValue) {
    if (tStringValue.isSetValue()) {
      return Timestamp.valueOf(tStringValue.getValue());
    }
    return null;
  }

  private static byte[] getBinaryValue(TStringValue tString) {
    if (tString.isSetValue()) {
      return tString.getValue().getBytes();
    }
    return null;
  }

  private static BigDecimal getBigDecimalValue(TStringValue tStringValue) {
    if (tStringValue.isSetValue()) {
      return new BigDecimal(tStringValue.getValue());
    }
    return null;
  }

  public static Object toColumnValue(TColumnValue value) {
    TColumnValue._Fields field = value.getSetField();
    switch (field) {
      case BOOL_VAL:
        return getBooleanValue(value.getBoolVal());
      case BYTE_VAL:
        return getByteValue(value.getByteVal());
      case I16_VAL:
        return getShortValue(value.getI16Val());
      case I32_VAL:
        return getIntegerValue(value.getI32Val());
      case I64_VAL:
        return getLongValue(value.getI64Val());
      case DOUBLE_VAL:
        return getDoubleValue(value.getDoubleVal());
      case STRING_VAL:
        return getStringValue(value.getStringVal());
    }
    throw new IllegalArgumentException("never");
  }
}
