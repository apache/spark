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
package org.apache.spark.sql;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import static org.apache.spark.sql.Encoders.*;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.RowFactory.create;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.connect.test.IntegrationTestUtils;
import org.apache.spark.sql.connect.test.SparkConnectServerUtils;
import org.apache.spark.sql.types.StructType;

/**
 * Tests for the encoders class.
 */
public class JavaEncoderSuite implements Serializable {
  private static SparkSession spark;

  @BeforeAll
  public static void setup() {
    Assumptions.assumeTrue(IntegrationTestUtils.isAssemblyJarsDirExists(),
      "Skipping all tests because assembly jars directory does not exist.");
    spark = SparkConnectServerUtils.createSparkSession();
  }

  @AfterAll
  public static void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
      SparkConnectServerUtils.stop();
    }
  }

  private static BigDecimal bigDec(long unscaled, int scale) {
    return BigDecimal.valueOf(unscaled, scale);
  }


  private <T> Dataset<T> dataset(Encoder<T> encoder, T... elements) {
    return spark.createDataset(Arrays.asList(elements), encoder);
  }

  @Test
  public void testSimpleEncoders() {
    final Column v = col("value");
    assertFalse(
        dataset(BOOLEAN(), false, true, false).select(every(v)).as(BOOLEAN()).head());
    assertEquals(
        7L,
        dataset(BYTE(), (byte) -120, (byte)127).select(sum(v)).as(LONG()).head().longValue());
    assertEquals(
        (short) 16,
        dataset(SHORT(), (short)16, (short)2334).select(min(v)).as(SHORT()).head().shortValue());
    assertEquals(
        10L,
        dataset(INT(), 1, 2, 3, 4).select(sum(v)).as(LONG()).head().longValue());
    assertEquals(
        96L,
        dataset(LONG(), 77L, 19L).select(sum(v)).as(LONG()).head().longValue());
    assertEquals(
        0.12f,
        dataset(FLOAT(), 0.12f, 0.3f, 44f).select(min(v)).as(FLOAT()).head(),
        0.0001f);
    assertEquals(
        789d,
        dataset(DOUBLE(), 789d, 12.213d, 10.01d).select(max(v)).as(DOUBLE()).head(),
        0.0001f);
    assertEquals(
        bigDec(1002, 2),
        dataset(DECIMAL(), bigDec(1000, 2), bigDec(2, 2))
            .select(sum(v)).as(DECIMAL()).head().setScale(2));
  }

  @Test
  public void testRowEncoder() {
    final StructType schema = new StructType()
        .add("a", "int")
        .add("b", "string");
    final Dataset<Row> df = spark.range(3)
        .map(new MapFunction<Long, Row>() {
               @Override
               public Row call(Long i) {
                 return create(i.intValue(), "s" + i);
               }
             },
            Encoders.row(schema))
        .filter(col("a").geq(1));
    final List<Row> expected = Arrays.asList(create(1, "s1"), create(2, "s2"));
    Assertions.assertEquals(expected, df.collectAsList());
  }
}
