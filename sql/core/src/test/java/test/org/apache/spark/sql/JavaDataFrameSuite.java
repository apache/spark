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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import scala.collection.JavaConversions;
import scala.collection.Seq;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import org.junit.*;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.test.TestSQLContext;
import org.apache.spark.sql.types.*;

public class JavaDataFrameSuite {
  private transient JavaSparkContext jsc;
  private transient TestSQLContext context;

  @Before
  public void setUp() {
    // Trigger static initializer of TestData
    SparkContext sc = new SparkContext("local[*]", "testing");
    jsc = new JavaSparkContext(sc);
    context = new TestSQLContext(sc);
    context.loadTestData();
  }

  @After
  public void tearDown() {
    context.sparkContext().stop();
    context = null;
    jsc = null;
  }

  @Test
  public void testExecution() {
    DataFrame df = context.table("testData").filter("key = 1");
    Assert.assertEquals(df.select("key").collect()[0].get(0), 1);
  }

  /**
   * See SPARK-5904. Abstract vararg methods defined in Scala do not work in Java.
   */
  @Test
  public void testVarargMethods() {
    DataFrame df = context.table("testData");

    df.toDF("key1", "value1");

    df.select("key", "value");
    df.select(col("key"), col("value"));
    df.selectExpr("key", "value + 1");

    df.sort("key", "value");
    df.sort(col("key"), col("value"));
    df.orderBy("key", "value");
    df.orderBy(col("key"), col("value"));

    df.groupBy("key", "value").agg(col("key"), col("value"), sum("value"));
    df.groupBy(col("key"), col("value")).agg(col("key"), col("value"), sum("value"));
    df.agg(first("key"), sum("value"));

    df.groupBy().avg("key");
    df.groupBy().mean("key");
    df.groupBy().max("key");
    df.groupBy().min("key");
    df.groupBy().sum("key");

    // Varargs in column expressions
    df.groupBy().agg(countDistinct("key", "value"));
    df.groupBy().agg(countDistinct(col("key"), col("value")));
    df.select(coalesce(col("key")));
    
    // Varargs with mathfunctions
    DataFrame df2 = context.table("testData2");
    df2.select(exp("a"), exp("b"));
    df2.select(exp(log("a")));
    df2.select(pow("a", "a"), pow("b", 2.0));
    df2.select(pow(col("a"), col("b")), exp("b"));
    df2.select(sin("a"), acos("b"));

    df2.select(rand(), acos("b"));
    df2.select(col("*"), randn(5L));
  }

  @Ignore
  public void testShow() {
    // This test case is intended ignored, but to make sure it compiles correctly
    DataFrame df = context.table("testData");
    df.show();
    df.show(1000);
  }

  public static class Bean implements Serializable {
    private double a = 0.0;
    private Integer[] b = new Integer[]{0, 1};
    private Map<String, int[]> c = ImmutableMap.of("hello", new int[] { 1, 2 });
    private List<String> d = Arrays.asList("floppy", "disk");

    public double getA() {
      return a;
    }

    public Integer[] getB() {
      return b;
    }

    public Map<String, int[]> getC() {
      return c;
    }

    public List<String> getD() {
      return d;
    }
  }

  @Test
  public void testCreateDataFrameFromJavaBeans() {
    Bean bean = new Bean();
    JavaRDD<Bean> rdd = jsc.parallelize(Arrays.asList(bean));
    DataFrame df = context.createDataFrame(rdd, Bean.class);
    StructType schema = df.schema();
    Assert.assertEquals(new StructField("a", DoubleType$.MODULE$, false, Metadata.empty()),
      schema.apply("a"));
    Assert.assertEquals(
      new StructField("b", new ArrayType(IntegerType$.MODULE$, true), true, Metadata.empty()),
      schema.apply("b"));
    ArrayType valueType = new ArrayType(DataTypes.IntegerType, false);
    MapType mapType = new MapType(DataTypes.StringType, valueType, true);
    Assert.assertEquals(
      new StructField("c", mapType, true, Metadata.empty()),
      schema.apply("c"));
    Assert.assertEquals(
      new StructField("d", new ArrayType(DataTypes.StringType, true), true, Metadata.empty()),
      schema.apply("d"));
    Row first = df.select("a", "b", "c", "d").first();
    Assert.assertEquals(bean.getA(), first.getDouble(0), 0.0);
    // Now Java lists and maps are converetd to Scala Seq's and Map's. Once we get a Seq below,
    // verify that it has the expected length, and contains expected elements.
    Seq<Integer> result = first.getAs(1);
    Assert.assertEquals(bean.getB().length, result.length());
    for (int i = 0; i < result.length(); i++) {
      Assert.assertEquals(bean.getB()[i], result.apply(i));
    }
    @SuppressWarnings("unchecked")
    Seq<Integer> outputBuffer = (Seq<Integer>) first.getJavaMap(2).get("hello");
    Assert.assertArrayEquals(
      bean.getC().get("hello"),
      Ints.toArray(JavaConversions.seqAsJavaList(outputBuffer)));
    Seq<String> d = first.getAs(3);
    Assert.assertEquals(bean.getD().size(), d.length());
    for (int i = 0; i < d.length(); i++) {
      Assert.assertEquals(bean.getD().get(i), d.apply(i));
    }
  }

  private static Comparator<Row> CrosstabRowComparator = new Comparator<Row>() {
    public int compare(Row row1, Row row2) {
      String item1 = row1.getString(0);
      String item2 = row2.getString(0);
      return item1.compareTo(item2);
    }
  };

  @Test
  public void testCrosstab() {
    DataFrame df = context.table("testData2");
    DataFrame crosstab = df.stat().crosstab("a", "b");
    String[] columnNames = crosstab.schema().fieldNames();
    Assert.assertEquals(columnNames[0], "a_b");
    Assert.assertEquals(columnNames[1], "1");
    Assert.assertEquals(columnNames[2], "2");
    Row[] rows = crosstab.collect();
    Arrays.sort(rows, CrosstabRowComparator);
    Integer count = 1;
    for (Row row : rows) {
      Assert.assertEquals(row.get(0).toString(), count.toString());
      Assert.assertEquals(row.getLong(1), 1L);
      Assert.assertEquals(row.getLong(2), 1L);
      count++;
    }
  }
  
  @Test
  public void testFrequentItems() {
    DataFrame df = context.table("testData2");
    String[] cols = new String[]{"a"};
    DataFrame results = df.stat().freqItems(cols, 0.2);
    Assert.assertTrue(results.collect()[0].getSeq(0).contains(1));
  }

  @Test
  public void testCorrelation() {
    DataFrame df = context.table("testData2");
    Double pearsonCorr = df.stat().corr("a", "b", "pearson");
    Assert.assertTrue(Math.abs(pearsonCorr) < 1e-6);
  }

  @Test
  public void testCovariance() {
    DataFrame df = context.table("testData2");
    Double result = df.stat().cov("a", "b");
    Assert.assertTrue(Math.abs(result) < 1e-6);
  }

  @Test
  public void testSampleBy() {
    DataFrame df = context.range(0, 100, 1, 2).select(col("id").mod(3).as("key"));
    DataFrame sampled = df.stat().<Integer>sampleBy("key", ImmutableMap.of(0, 0.1, 1, 0.2), 0L);
    Row[] actual = sampled.groupBy("key").count().orderBy("key").collect();
    Row[] expected = new Row[] {RowFactory.create(0, 5), RowFactory.create(1, 8)};
    Assert.assertArrayEquals(expected, actual);
  }
}
