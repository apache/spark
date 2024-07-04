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

package test.org.apache.spark.sql;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class JavaRowSuite {
  private byte byteValue;
  private short shortValue;
  private int intValue;
  private long longValue;
  private float floatValue;
  private double doubleValue;
  private BigDecimal decimalValue;
  private boolean booleanValue;
  private String stringValue;
  private byte[] binaryValue;
  private Date dateValue;
  private Timestamp timestampValue;

  @BeforeEach
  public void setUp() {
    byteValue = (byte)127;
    shortValue = (short)32767;
    intValue = 2147483647;
    longValue = 9223372036854775807L;
    floatValue = 3.4028235E38f;
    doubleValue = 1.7976931348623157E308;
    decimalValue = new BigDecimal("1.7976931348623157E328");
    booleanValue = true;
    stringValue = "this is a string";
    binaryValue = stringValue.getBytes(StandardCharsets.UTF_8);
    dateValue = Date.valueOf("2014-06-30");
    timestampValue = Timestamp.valueOf("2014-06-30 09:20:00.0");
  }

  @Test
  public void constructSimpleRow() {
    Row simpleRow = RowFactory.create(
      byteValue,                 // ByteType
      Byte.valueOf(byteValue),
      shortValue,                // ShortType
      Short.valueOf(shortValue),
      intValue,                  // IntegerType
      Integer.valueOf(intValue),
      longValue,                 // LongType
      Long.valueOf(longValue),
      floatValue,                // FloatType
      Float.valueOf(floatValue),
      doubleValue,               // DoubleType
      Double.valueOf(doubleValue),
      decimalValue,              // DecimalType
      booleanValue,              // BooleanType
      Boolean.valueOf(booleanValue),
      stringValue,               // StringType
      binaryValue,               // BinaryType
      dateValue,                 // DateType
      timestampValue,            // TimestampType
      null                       // null
    );

    Assertions.assertEquals(byteValue, simpleRow.getByte(0));
    Assertions.assertEquals(byteValue, simpleRow.get(0));
    Assertions.assertEquals(byteValue, simpleRow.getByte(1));
    Assertions.assertEquals(byteValue, simpleRow.get(1));
    Assertions.assertEquals(shortValue, simpleRow.getShort(2));
    Assertions.assertEquals(shortValue, simpleRow.get(2));
    Assertions.assertEquals(shortValue, simpleRow.getShort(3));
    Assertions.assertEquals(shortValue, simpleRow.get(3));
    Assertions.assertEquals(intValue, simpleRow.getInt(4));
    Assertions.assertEquals(intValue, simpleRow.get(4));
    Assertions.assertEquals(intValue, simpleRow.getInt(5));
    Assertions.assertEquals(intValue, simpleRow.get(5));
    Assertions.assertEquals(longValue, simpleRow.getLong(6));
    Assertions.assertEquals(longValue, simpleRow.get(6));
    Assertions.assertEquals(longValue, simpleRow.getLong(7));
    Assertions.assertEquals(longValue, simpleRow.get(7));
    // When we create the row, we do not do any conversion
    // for a float/double value, so we just set the delta to 0.
    Assertions.assertEquals(floatValue, simpleRow.getFloat(8), 0);
    Assertions.assertEquals(floatValue, simpleRow.get(8));
    Assertions.assertEquals(floatValue, simpleRow.getFloat(9), 0);
    Assertions.assertEquals(floatValue, simpleRow.get(9));
    Assertions.assertEquals(doubleValue, simpleRow.getDouble(10), 0);
    Assertions.assertEquals(doubleValue, simpleRow.get(10));
    Assertions.assertEquals(doubleValue, simpleRow.getDouble(11), 0);
    Assertions.assertEquals(doubleValue, simpleRow.get(11));
    Assertions.assertEquals(decimalValue, simpleRow.get(12));
    Assertions.assertEquals(booleanValue, simpleRow.getBoolean(13));
    Assertions.assertEquals(booleanValue, simpleRow.get(13));
    Assertions.assertEquals(booleanValue, simpleRow.getBoolean(14));
    Assertions.assertEquals(booleanValue, simpleRow.get(14));
    Assertions.assertEquals(stringValue, simpleRow.getString(15));
    Assertions.assertEquals(stringValue, simpleRow.get(15));
    Assertions.assertEquals(binaryValue, simpleRow.get(16));
    Assertions.assertEquals(dateValue, simpleRow.get(17));
    Assertions.assertEquals(timestampValue, simpleRow.get(18));
    Assertions.assertTrue(simpleRow.isNullAt(19));
    Assertions.assertNull(simpleRow.get(19));
  }

  @Test
  public void constructComplexRow() {
    // Simple array
    List<String> simpleStringArray = Arrays.asList(
      stringValue + " (1)", stringValue + " (2)", stringValue + "(3)");

    // Simple map
    Map<String, Long> simpleMap = new HashMap<>();
    simpleMap.put(stringValue + " (1)", longValue);
    simpleMap.put(stringValue + " (2)", longValue - 1);
    simpleMap.put(stringValue + " (3)", longValue - 2);

    // Simple struct
    Row simpleStruct = RowFactory.create(
      doubleValue, stringValue, timestampValue, null);

    // Complex array
    List<Map<String, Long>> arrayOfMaps = Arrays.asList(simpleMap);
    List<Row> arrayOfRows = Arrays.asList(simpleStruct);

    // Complex map
    Map<List<Row>, Row> complexMap = new HashMap<>();
    complexMap.put(arrayOfRows, simpleStruct);

    // Complex struct
    Row complexStruct = RowFactory.create(
      simpleStringArray,
      simpleMap,
      simpleStruct,
      arrayOfMaps,
      arrayOfRows,
      complexMap,
      null);
    Assertions.assertEquals(simpleStringArray, complexStruct.get(0));
    Assertions.assertEquals(simpleMap, complexStruct.get(1));
    Assertions.assertEquals(simpleStruct, complexStruct.get(2));
    Assertions.assertEquals(arrayOfMaps, complexStruct.get(3));
    Assertions.assertEquals(arrayOfRows, complexStruct.get(4));
    Assertions.assertEquals(complexMap, complexStruct.get(5));
    Assertions.assertNull(complexStruct.get(6));

    // A very complex row
    Row complexRow = RowFactory.create(arrayOfMaps, arrayOfRows, complexMap, complexStruct);
    Assertions.assertEquals(arrayOfMaps, complexRow.get(0));
    Assertions.assertEquals(arrayOfRows, complexRow.get(1));
    Assertions.assertEquals(complexMap, complexRow.get(2));
    Assertions.assertEquals(complexStruct, complexRow.get(3));
  }
}
