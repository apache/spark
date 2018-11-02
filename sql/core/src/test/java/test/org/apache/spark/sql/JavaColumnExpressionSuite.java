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

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.test.TestSparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.apache.spark.sql.types.DataTypes.*;

public class JavaColumnExpressionSuite {
  private transient TestSparkSession spark;

  @Before
  public void setUp() {
    spark = new TestSparkSession();
  }

  @After
  public void tearDown() {
    spark.stop();
    spark = null;
  }

  @Test
  public void isInCollectionWorksCorrectlyOnJava() {
    List<Row> rows = Arrays.asList(
      RowFactory.create(1, "x"),
      RowFactory.create(2, "y"),
      RowFactory.create(3, "z"));
    StructType schema = createStructType(Arrays.asList(
      createStructField("a", IntegerType, false),
      createStructField("b", StringType, false)));
    Dataset<Row> df = spark.createDataFrame(rows, schema);
    // Test with different types of collections
    Assert.assertTrue(Arrays.equals(
      (Row[]) df.filter(df.col("a").isInCollection(Arrays.asList(1, 2))).collect(),
      (Row[]) df.filter((FilterFunction<Row>) r -> r.getInt(0) == 1 || r.getInt(0) == 2).collect()
    ));
    Assert.assertTrue(Arrays.equals(
      (Row[]) df.filter(df.col("a").isInCollection(new HashSet<>(Arrays.asList(1, 2)))).collect(),
      (Row[]) df.filter((FilterFunction<Row>) r -> r.getInt(0) == 1 || r.getInt(0) == 2).collect()
    ));
    Assert.assertTrue(Arrays.equals(
      (Row[]) df.filter(df.col("a").isInCollection(new ArrayList<>(Arrays.asList(3, 1)))).collect(),
      (Row[]) df.filter((FilterFunction<Row>) r -> r.getInt(0) == 3 || r.getInt(0) == 1).collect()
    ));
  }

  @Test
  public void isInCollectionCheckExceptionMessage() {
    List<Row> rows = Arrays.asList(
      RowFactory.create(1, Arrays.asList(1)),
      RowFactory.create(2, Arrays.asList(2)),
      RowFactory.create(3, Arrays.asList(3)));
    StructType schema = createStructType(Arrays.asList(
      createStructField("a", IntegerType, false),
      createStructField("b", createArrayType(IntegerType, false), false)));
    Dataset<Row> df = spark.createDataFrame(rows, schema);
    try {
      df.filter(df.col("a").isInCollection(Arrays.asList(new Column("b"))));
      Assert.fail("Expected org.apache.spark.sql.AnalysisException");
    } catch (Exception e) {
      Arrays.asList("cannot resolve",
        "due to data type mismatch: Arguments must be same type but were")
        .forEach(s -> Assert.assertTrue(
          e.getMessage().toLowerCase(Locale.ROOT).contains(s.toLowerCase(Locale.ROOT))));
    }
  }
}
