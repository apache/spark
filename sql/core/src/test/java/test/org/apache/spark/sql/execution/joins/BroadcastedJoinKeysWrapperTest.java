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

package test.org.apache.spark.sql.execution.joins;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Disabled;
import org.scalatest.Ignore;
import scala.reflect.ClassTag;

import org.apache.spark.sql.catalyst.bcvar.ArrayWrapper;
import org.apache.spark.sql.execution.joins.BroadcastedJoinKeysWrapperImpl;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.catalyst.bcvar.BroadcastedJoinKeysWrapper;
import org.apache.spark.sql.execution.joins.HashedRelation;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BroadcastedJoinKeysWrapperTest {
  private transient SparkSession spark;


  private SparkPlan sp;

  private final LocalDateTime now = LocalDateTime.now();

  private final int uniqueTimeStampValues = 10;
  private final Timestamp[] timeStampData = new Timestamp[uniqueTimeStampValues];
  private final StructType schema = new StructType(
    new StructField[]{
      new StructField("intCol", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("longCol", DataTypes.LongType, false, Metadata.empty()),
      new StructField("stringCol", DataTypes.StringType, false, Metadata.empty()),
      new StructField("dateCol", DataTypes.DateType, false, Metadata.empty()),
      new StructField("timestampCol", DataTypes.TimestampType, false,
          Metadata.empty()),
      new StructField("doubleCol", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("floatCol", DataTypes.FloatType, false, Metadata.empty()),
      new StructField("byteCol", DataTypes.ByteType, false, Metadata.empty()),
      new StructField("shortCol", DataTypes.ShortType, false, Metadata.empty()),
      new StructField("bigDecCol", DataTypes.createDecimalType(22, 3),
          false, Metadata.empty()),
      });

  // new StructField("calCol", DataTypes.CalendarIntervalType, false, Metadata.empty()),


  @BeforeAll
  public void setUp() {
    spark = SparkSession.builder()
        .master("local[*]")
        .appName("testing")
        .getOrCreate();
    for (int i = 0 ; i < uniqueTimeStampValues; ++i) {
      timeStampData[i] = Timestamp.valueOf(now.minusHours(i));
    }
    this.preparePlan();
  }

  private void preparePlan() {
    int numRows = 1000;
    List<Row> rows = new LinkedList<>();

    for (int i = 0; i < numRows; ++i) {
     rows.add(
       RowFactory.create(
         i % 10, Integer.MAX_VALUE + (long) i % 15, "String" + (i % 20),
         java.sql.Date.valueOf(now.minusDays(i % 10).toLocalDate()),
         timeStampData[i % uniqueTimeStampValues]  ,
         Double.valueOf(i % 75 + ".67367363615d"), Float.valueOf(i % 30 + ".67365f"),
           Byte.valueOf("" + (i % 127)),
         Short.valueOf(""+i % 50),
         new BigDecimal( BigInteger.valueOf(Integer.MAX_VALUE + (long)i % 43), 3)
      ));
    }
    Dataset<Row> df = spark.createDataFrame(rows, schema);
    sp = df.queryExecution().sparkPlan();
  }

  @AfterAll
  public void tearDown() {
    spark.stop();
    spark = null;
  }

  @Test
  public void testIntegerTypeForSingleKeyHashedRelation() {
    DummyBroadcast bc = new DummyBroadcast(1);
    int count = 10;
    Integer[] expectedKeys = new Integer[count];
    for (int i = 0; i < count; ++i) {
      expectedKeys[i] = i;
    }
    this.testDataTypeForSingleKeyHashedRelation(DataTypes.IntegerType,
        new HashSet<>(Arrays.asList(expectedKeys)),  1000, bc);
  }

  @Test
  public void testLongTypeForSingleKeyHashedRelation() {
    DummyBroadcast bc = new DummyBroadcast(2);
    int count = 15;
    Long[] expectedKeys = new Long[count];
    for (int i = 0; i < count; ++i) {
      expectedKeys[i] = Integer.MAX_VALUE + (long)i;
    }
    this.testDataTypeForSingleKeyHashedRelation(DataTypes.LongType,
        new HashSet<>(Arrays.asList(expectedKeys)), 15, bc);
  }

  @Test
  public void testByteTypeForSingleKeyHashedRelation() {
    DummyBroadcast bc = new DummyBroadcast(3);
    int count = 127;
    Byte[] expectedKeys = new Byte[count];
    for (int i = 0; i < count; ++i) {
      expectedKeys[i] = (byte)i;
    }
    this.testDataTypeForSingleKeyHashedRelation(DataTypes.ByteType,
        new HashSet<>(Arrays.asList(expectedKeys)), 1000, bc);
  }

  @Test
  public void testShortTypeForSingleKeyHashedRelation() {
    DummyBroadcast bc = new DummyBroadcast(4);
    int count = 50;
    Short[] expectedKeys = new Short[count];
    for (int i = 0; i < count; ++i) {
      expectedKeys[i] = (short) i;
    }
    this.testDataTypeForSingleKeyHashedRelation(DataTypes.ShortType,
        new HashSet<>(Arrays.asList(expectedKeys)), 1000, bc);
  }


  @Test
  public void testStringTypeForSingleKeyHashedRelation() {
    DummyBroadcast bc = new DummyBroadcast(5);
    int count = 20;
    String[] expectedKeys = new String[count];
    for (int i = 0; i < count; ++i) {
      expectedKeys[i] = "String" + i;
    }
    this.testDataTypeForSingleKeyHashedRelation(DataTypes.StringType,
        new HashSet<>(Arrays.asList(expectedKeys)), 1000, bc);
  }

  @Test
  public void testDateTypeForSingleKeyHashedRelation() {
    int count = 10;
    DummyBroadcast bc = new DummyBroadcast(6);
    Date[] expectedKeys = new Date[count];

    for (int i = 0; i < count; ++i) {
      expectedKeys[i] = java.sql.Date.valueOf(now.minusDays(i).toLocalDate());
    }
    this.testDataTypeForSingleKeyHashedRelation(DataTypes.DateType,
        new HashSet<>(Arrays.asList(expectedKeys)), 1000, bc);
  }

  @Disabled
  public void testTimestampTypeForSingleKeyHashedRelation() {
    DummyBroadcast bc = new DummyBroadcast(7);
    this.testDataTypeForSingleKeyHashedRelation(DataTypes.TimestampType,
        new HashSet<>(Arrays.asList(timeStampData)), 1000, bc);
  }

  @Test
  public void testDoubleTypeForSingleKeyHashedRelation() {
    int count = 75;
    DummyBroadcast bc = new DummyBroadcast(8);
    Double[] expectedKeys = new Double[count];

    for (int i = 0; i < count; ++i) {
      expectedKeys[i] = Double.valueOf(i  + ".67367363615d");
    }
    this.testDataTypeForSingleKeyHashedRelation(DataTypes.DoubleType,
        new HashSet<>(Arrays.asList(expectedKeys)), 1000, bc);
  }

  @Test
  public void testFloatTypeForSingleKeyHashedRelation() {
    int count = 30;
    DummyBroadcast bc = new DummyBroadcast(9);
    Float[] expectedKeys = new Float[count];

    for (int i = 0; i < count; ++i) {
      expectedKeys[i] = Float.valueOf(i  + ".67365f");
    }
    this.testDataTypeForSingleKeyHashedRelation(DataTypes.FloatType,
        new HashSet<>(Arrays.asList(expectedKeys)), 1000, bc);
  }

  @Test
  public void testBigDecimalTypeForSingleKeyHashedRelation() {
    int count = 43;
    DummyBroadcast bc = new DummyBroadcast(10);
    BigDecimal[] expectedKeys = new BigDecimal[count];

    for (int i = 0; i < count; ++i) {
      expectedKeys[i] = new BigDecimal( BigInteger.valueOf(Integer.MAX_VALUE + (long)i ), 3);
    }
    this.testDataTypeForSingleKeyHashedRelation(DataTypes.createDecimalType(22, 3),
        new HashSet<>(Arrays.asList(expectedKeys)), 1000, bc);
  }


  private void testDataTypeForSingleKeyHashedRelation(DataType dataType, Set<Object> expectedKeys,
      int expectedObjArrLengt, DummyBroadcast bc) {
    int indexOfAttrib =  sp.output().indexWhere(attr -> attr.dataType().equals(dataType));
    Expression expr = new BoundReference(indexOfAttrib, sp.output().apply(indexOfAttrib).dataType(),
        false);
    scala.collection.mutable.ArrayBuffer<Expression> temp =
        new scala.collection.mutable.ArrayBuffer();
    temp.$plus$eq(expr);
    final HashedRelation hr = HashedRelation.apply(sp.executeToIterator(), temp.toSeq(),
        64, null, false, false,
        false);
    bc.setHashedRelation(hr);
    BroadcastedJoinKeysWrapper wrapper =
        new BroadcastedJoinKeysWrapperImpl(bc, new DataType[]{dataType},
            0, new int[]{0}, 1);
    ArrayWrapper<? extends Object> keys = wrapper.getKeysArray();
    assert (keys.getLength() == expectedObjArrLengt);
    HashSet<Object> uniqueKeys =  new HashSet<>(Arrays.asList(keys.getBaseArray()));
    assert(uniqueKeys.size() == expectedKeys.size());
    for (Object key : uniqueKeys) {
      assert (expectedKeys.contains(key));
    }
  }

  private static class DummyBroadcast extends Broadcast<HashedRelation> {
    private HashedRelation hr = null;

    DummyBroadcast(long id) {
      super(id, ClassTag.<HashedRelation>apply(HashedRelation.class));
    }

    @Override
    public HashedRelation getValue() {
      return this.hr;
    }

    @Override
    public void doUnpersist(boolean blocking) {

    }

    @Override
    public void doDestroy(boolean blocking) {

    }

    void setHashedRelation(HashedRelation hr) {
      this.hr = hr;
    }
  }
}
