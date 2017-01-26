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
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

import com.google.common.base.Objects;
import org.junit.*;
import org.junit.rules.ExpectedException;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.OuterScopes;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.test.TestSparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.types.DataTypes.*;

public class JavaDatasetSuite implements Serializable {
  private transient TestSparkSession spark;
  private transient JavaSparkContext jsc;

  @Before
  public void setUp() {
    // Trigger static initializer of TestData
    spark = new TestSparkSession();
    jsc = new JavaSparkContext(spark.sparkContext());
    spark.loadTestData();
  }

  @After
  public void tearDown() {
    spark.stop();
    spark = null;
  }

  private <T1, T2> Tuple2<T1, T2> tuple2(T1 t1, T2 t2) {
    return new Tuple2<>(t1, t2);
  }

  @Test
  public void testCollect() {
    List<String> data = Arrays.asList("hello", "world");
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    List<String> collected = ds.collectAsList();
    Assert.assertEquals(Arrays.asList("hello", "world"), collected);
  }

  @Test
  public void testTake() {
    List<String> data = Arrays.asList("hello", "world");
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    List<String> collected = ds.takeAsList(1);
    Assert.assertEquals(Arrays.asList("hello"), collected);
  }

  @Test
  public void testToLocalIterator() {
    List<String> data = Arrays.asList("hello", "world");
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    Iterator<String> iter = ds.toLocalIterator();
    Assert.assertEquals("hello", iter.next());
    Assert.assertEquals("world", iter.next());
    Assert.assertFalse(iter.hasNext());
  }

  // SPARK-15632: typed filter should preserve the underlying logical schema
  @Test
  public void testTypedFilterPreservingSchema() {
    Dataset<Long> ds = spark.range(10);
    Dataset<Long> ds2 = ds.filter(new FilterFunction<Long>() {
      @Override
      public boolean call(Long value) throws Exception {
        return value > 3;
      }
    });
    Assert.assertEquals(ds.schema(), ds2.schema());
  }

  @Test
  public void testCommonOperation() {
    List<String> data = Arrays.asList("hello", "world");
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    Assert.assertEquals("hello", ds.first());

    Dataset<String> filtered = ds.filter(new FilterFunction<String>() {
      @Override
      public boolean call(String v) throws Exception {
        return v.startsWith("h");
      }
    });
    Assert.assertEquals(Arrays.asList("hello"), filtered.collectAsList());


    Dataset<Integer> mapped = ds.map(new MapFunction<String, Integer>() {
      @Override
      public Integer call(String v) throws Exception {
        return v.length();
      }
    }, Encoders.INT());
    Assert.assertEquals(Arrays.asList(5, 5), mapped.collectAsList());

    Dataset<String> parMapped = ds.mapPartitions(new MapPartitionsFunction<String, String>() {
      @Override
      public Iterator<String> call(Iterator<String> it) {
        List<String> ls = new LinkedList<>();
        while (it.hasNext()) {
          ls.add(it.next().toUpperCase(Locale.ENGLISH));
        }
        return ls.iterator();
      }
    }, Encoders.STRING());
    Assert.assertEquals(Arrays.asList("HELLO", "WORLD"), parMapped.collectAsList());

    Dataset<String> flatMapped = ds.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String s) {
        List<String> ls = new LinkedList<>();
        for (char c : s.toCharArray()) {
          ls.add(String.valueOf(c));
        }
        return ls.iterator();
      }
    }, Encoders.STRING());
    Assert.assertEquals(
      Arrays.asList("h", "e", "l", "l", "o", "w", "o", "r", "l", "d"),
      flatMapped.collectAsList());
  }

  @Test
  public void testForeach() {
    final LongAccumulator accum = jsc.sc().longAccumulator();
    List<String> data = Arrays.asList("a", "b", "c");
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

    ds.foreach(new ForeachFunction<String>() {
      @Override
      public void call(String s) throws Exception {
        accum.add(1);
      }
    });
    Assert.assertEquals(3, accum.value().intValue());
  }

  @Test
  public void testReduce() {
    List<Integer> data = Arrays.asList(1, 2, 3);
    Dataset<Integer> ds = spark.createDataset(data, Encoders.INT());

    int reduced = ds.reduce(new ReduceFunction<Integer>() {
      @Override
      public Integer call(Integer v1, Integer v2) throws Exception {
        return v1 + v2;
      }
    });
    Assert.assertEquals(6, reduced);
  }

  @Test
  public void testGroupBy() {
    List<String> data = Arrays.asList("a", "foo", "bar");
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    KeyValueGroupedDataset<Integer, String> grouped = ds.groupByKey(
      new MapFunction<String, Integer>() {
        @Override
        public Integer call(String v) throws Exception {
          return v.length();
        }
      },
      Encoders.INT());

    Dataset<String> mapped = grouped.mapGroups(new MapGroupsFunction<Integer, String, String>() {
      @Override
      public String call(Integer key, Iterator<String> values) throws Exception {
        StringBuilder sb = new StringBuilder(key.toString());
        while (values.hasNext()) {
          sb.append(values.next());
        }
        return sb.toString();
      }
    }, Encoders.STRING());

    Assert.assertEquals(asSet("1a", "3foobar"), toSet(mapped.collectAsList()));

    Dataset<String> flatMapped = grouped.flatMapGroups(
      new FlatMapGroupsFunction<Integer, String, String>() {
        @Override
        public Iterator<String> call(Integer key, Iterator<String> values) {
          StringBuilder sb = new StringBuilder(key.toString());
          while (values.hasNext()) {
            sb.append(values.next());
          }
          return Collections.singletonList(sb.toString()).iterator();
        }
      },
      Encoders.STRING());

    Assert.assertEquals(asSet("1a", "3foobar"), toSet(flatMapped.collectAsList()));

    Dataset<Tuple2<Integer, String>> reduced = grouped.reduceGroups(new ReduceFunction<String>() {
      @Override
      public String call(String v1, String v2) throws Exception {
        return v1 + v2;
      }
    });

    Assert.assertEquals(
      asSet(tuple2(1, "a"), tuple2(3, "foobar")),
      toSet(reduced.collectAsList()));

    List<Integer> data2 = Arrays.asList(2, 6, 10);
    Dataset<Integer> ds2 = spark.createDataset(data2, Encoders.INT());
    KeyValueGroupedDataset<Integer, Integer> grouped2 = ds2.groupByKey(
      new MapFunction<Integer, Integer>() {
        @Override
        public Integer call(Integer v) throws Exception {
          return v / 2;
        }
      },
      Encoders.INT());

    Dataset<String> cogrouped = grouped.cogroup(
      grouped2,
      new CoGroupFunction<Integer, String, Integer, String>() {
        @Override
        public Iterator<String> call(Integer key, Iterator<String> left, Iterator<Integer> right) {
          StringBuilder sb = new StringBuilder(key.toString());
          while (left.hasNext()) {
            sb.append(left.next());
          }
          sb.append("#");
          while (right.hasNext()) {
            sb.append(right.next());
          }
          return Collections.singletonList(sb.toString()).iterator();
        }
      },
      Encoders.STRING());

    Assert.assertEquals(asSet("1a#2", "3foobar#6", "5#10"), toSet(cogrouped.collectAsList()));
  }

  @Test
  public void testSelect() {
    List<Integer> data = Arrays.asList(2, 6);
    Dataset<Integer> ds = spark.createDataset(data, Encoders.INT());

    Dataset<Tuple2<Integer, String>> selected = ds.select(
      expr("value + 1"),
      col("value").cast("string")).as(Encoders.tuple(Encoders.INT(), Encoders.STRING()));

    Assert.assertEquals(
      Arrays.asList(tuple2(3, "2"), tuple2(7, "6")),
      selected.collectAsList());
  }

  @Test
  public void testSetOperation() {
    List<String> data = Arrays.asList("abc", "abc", "xyz");
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

    Assert.assertEquals(asSet("abc", "xyz"), toSet(ds.distinct().collectAsList()));

    List<String> data2 = Arrays.asList("xyz", "foo", "foo");
    Dataset<String> ds2 = spark.createDataset(data2, Encoders.STRING());

    Dataset<String> intersected = ds.intersect(ds2);
    Assert.assertEquals(Arrays.asList("xyz"), intersected.collectAsList());

    Dataset<String> unioned = ds.union(ds2).union(ds);
    Assert.assertEquals(
      Arrays.asList("abc", "abc", "xyz", "xyz", "foo", "foo", "abc", "abc", "xyz"),
      unioned.collectAsList());

    Dataset<String> subtracted = ds.except(ds2);
    Assert.assertEquals(Arrays.asList("abc"), subtracted.collectAsList());
  }

  private static <T> Set<T> toSet(List<T> records) {
    return new HashSet<>(records);
  }

  @SafeVarargs
  @SuppressWarnings("varargs")
  private static <T> Set<T> asSet(T... records) {
    return toSet(Arrays.asList(records));
  }

  @Test
  public void testJoin() {
    List<Integer> data = Arrays.asList(1, 2, 3);
    Dataset<Integer> ds = spark.createDataset(data, Encoders.INT()).as("a");
    List<Integer> data2 = Arrays.asList(2, 3, 4);
    Dataset<Integer> ds2 = spark.createDataset(data2, Encoders.INT()).as("b");

    Dataset<Tuple2<Integer, Integer>> joined =
      ds.joinWith(ds2, col("a.value").equalTo(col("b.value")));
    Assert.assertEquals(
      Arrays.asList(tuple2(2, 2), tuple2(3, 3)),
      joined.collectAsList());
  }

  @Test
  public void testTupleEncoder() {
    Encoder<Tuple2<Integer, String>> encoder2 = Encoders.tuple(Encoders.INT(), Encoders.STRING());
    List<Tuple2<Integer, String>> data2 = Arrays.asList(tuple2(1, "a"), tuple2(2, "b"));
    Dataset<Tuple2<Integer, String>> ds2 = spark.createDataset(data2, encoder2);
    Assert.assertEquals(data2, ds2.collectAsList());

    Encoder<Tuple3<Integer, Long, String>> encoder3 =
      Encoders.tuple(Encoders.INT(), Encoders.LONG(), Encoders.STRING());
    List<Tuple3<Integer, Long, String>> data3 =
      Arrays.asList(new Tuple3<>(1, 2L, "a"));
    Dataset<Tuple3<Integer, Long, String>> ds3 = spark.createDataset(data3, encoder3);
    Assert.assertEquals(data3, ds3.collectAsList());

    Encoder<Tuple4<Integer, String, Long, String>> encoder4 =
      Encoders.tuple(Encoders.INT(), Encoders.STRING(), Encoders.LONG(), Encoders.STRING());
    List<Tuple4<Integer, String, Long, String>> data4 =
      Arrays.asList(new Tuple4<>(1, "b", 2L, "a"));
    Dataset<Tuple4<Integer, String, Long, String>> ds4 = spark.createDataset(data4, encoder4);
    Assert.assertEquals(data4, ds4.collectAsList());

    Encoder<Tuple5<Integer, String, Long, String, Boolean>> encoder5 =
      Encoders.tuple(Encoders.INT(), Encoders.STRING(), Encoders.LONG(), Encoders.STRING(),
        Encoders.BOOLEAN());
    List<Tuple5<Integer, String, Long, String, Boolean>> data5 =
      Arrays.asList(new Tuple5<>(1, "b", 2L, "a", true));
    Dataset<Tuple5<Integer, String, Long, String, Boolean>> ds5 =
      spark.createDataset(data5, encoder5);
    Assert.assertEquals(data5, ds5.collectAsList());
  }

  @Test
  public void testNestedTupleEncoder() {
    // test ((int, string), string)
    Encoder<Tuple2<Tuple2<Integer, String>, String>> encoder =
      Encoders.tuple(Encoders.tuple(Encoders.INT(), Encoders.STRING()), Encoders.STRING());
    List<Tuple2<Tuple2<Integer, String>, String>> data =
      Arrays.asList(tuple2(tuple2(1, "a"), "a"), tuple2(tuple2(2, "b"), "b"));
    Dataset<Tuple2<Tuple2<Integer, String>, String>> ds = spark.createDataset(data, encoder);
    Assert.assertEquals(data, ds.collectAsList());

    // test (int, (string, string, long))
    Encoder<Tuple2<Integer, Tuple3<String, String, Long>>> encoder2 =
      Encoders.tuple(Encoders.INT(),
        Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.LONG()));
    List<Tuple2<Integer, Tuple3<String, String, Long>>> data2 =
      Arrays.asList(tuple2(1, new Tuple3<>("a", "b", 3L)));
    Dataset<Tuple2<Integer, Tuple3<String, String, Long>>> ds2 =
      spark.createDataset(data2, encoder2);
    Assert.assertEquals(data2, ds2.collectAsList());

    // test (int, ((string, long), string))
    Encoder<Tuple2<Integer, Tuple2<Tuple2<String, Long>, String>>> encoder3 =
      Encoders.tuple(Encoders.INT(),
        Encoders.tuple(Encoders.tuple(Encoders.STRING(), Encoders.LONG()), Encoders.STRING()));
    List<Tuple2<Integer, Tuple2<Tuple2<String, Long>, String>>> data3 =
      Arrays.asList(tuple2(1, tuple2(tuple2("a", 2L), "b")));
    Dataset<Tuple2<Integer, Tuple2<Tuple2<String, Long>, String>>> ds3 =
      spark.createDataset(data3, encoder3);
    Assert.assertEquals(data3, ds3.collectAsList());
  }

  @Test
  public void testPrimitiveEncoder() {
    Encoder<Tuple5<Double, BigDecimal, Date, Timestamp, Float>> encoder =
      Encoders.tuple(Encoders.DOUBLE(), Encoders.DECIMAL(), Encoders.DATE(), Encoders.TIMESTAMP(),
        Encoders.FLOAT());
    List<Tuple5<Double, BigDecimal, Date, Timestamp, Float>> data =
      Arrays.asList(new Tuple5<>(
        1.7976931348623157E308, new BigDecimal("0.922337203685477589"),
          Date.valueOf("1970-01-01"), new Timestamp(System.currentTimeMillis()), Float.MAX_VALUE));
    Dataset<Tuple5<Double, BigDecimal, Date, Timestamp, Float>> ds =
      spark.createDataset(data, encoder);
    Assert.assertEquals(data, ds.collectAsList());
  }

  public static class KryoSerializable {
    String value;

    KryoSerializable(String value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;

      return this.value.equals(((KryoSerializable) other).value);
    }

    @Override
    public int hashCode() {
      return this.value.hashCode();
    }
  }

  public static class JavaSerializable implements Serializable {
    String value;

    JavaSerializable(String value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other == null || getClass() != other.getClass()) return false;

      return this.value.equals(((JavaSerializable) other).value);
    }

    @Override
    public int hashCode() {
      return this.value.hashCode();
    }
  }

  @Test
  public void testKryoEncoder() {
    Encoder<KryoSerializable> encoder = Encoders.kryo(KryoSerializable.class);
    List<KryoSerializable> data = Arrays.asList(
      new KryoSerializable("hello"), new KryoSerializable("world"));
    Dataset<KryoSerializable> ds = spark.createDataset(data, encoder);
    Assert.assertEquals(data, ds.collectAsList());
  }

  @Test
  public void testJavaEncoder() {
    Encoder<JavaSerializable> encoder = Encoders.javaSerialization(JavaSerializable.class);
    List<JavaSerializable> data = Arrays.asList(
      new JavaSerializable("hello"), new JavaSerializable("world"));
    Dataset<JavaSerializable> ds = spark.createDataset(data, encoder);
    Assert.assertEquals(data, ds.collectAsList());
  }

  @Test
  public void testRandomSplit() {
    List<String> data = Arrays.asList("hello", "world", "from", "spark");
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    double[] arraySplit = {1, 2, 3};

    List<Dataset<String>> randomSplit =  ds.randomSplitAsList(arraySplit, 1);
    Assert.assertEquals("wrong number of splits", randomSplit.size(), 3);
  }

  /**
   * For testing error messages when creating an encoder on a private class. This is done
   * here since we cannot create truly private classes in Scala.
   */
  private static class PrivateClassTest { }

  @Test(expected = UnsupportedOperationException.class)
  public void testJavaEncoderErrorMessageForPrivateClass() {
    Encoders.javaSerialization(PrivateClassTest.class);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testKryoEncoderErrorMessageForPrivateClass() {
    Encoders.kryo(PrivateClassTest.class);
  }

  public static class SimpleJavaBean implements Serializable {
    private boolean a;
    private int b;
    private byte[] c;
    private String[] d;
    private List<String> e;
    private List<Long> f;
    private Map<Integer, String> g;
    private Map<List<Long>, Map<String, String>> h;

    public boolean isA() {
      return a;
    }

    public void setA(boolean a) {
      this.a = a;
    }

    public int getB() {
      return b;
    }

    public void setB(int b) {
      this.b = b;
    }

    public byte[] getC() {
      return c;
    }

    public void setC(byte[] c) {
      this.c = c;
    }

    public String[] getD() {
      return d;
    }

    public void setD(String[] d) {
      this.d = d;
    }

    public List<String> getE() {
      return e;
    }

    public void setE(List<String> e) {
      this.e = e;
    }

    public List<Long> getF() {
      return f;
    }

    public void setF(List<Long> f) {
      this.f = f;
    }

    public Map<Integer, String> getG() {
      return g;
    }

    public void setG(Map<Integer, String> g) {
      this.g = g;
    }

    public Map<List<Long>, Map<String, String>> getH() {
      return h;
    }

    public void setH(Map<List<Long>, Map<String, String>> h) {
      this.h = h;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SimpleJavaBean that = (SimpleJavaBean) o;

      if (a != that.a) return false;
      if (b != that.b) return false;
      if (!Arrays.equals(c, that.c)) return false;
      if (!Arrays.equals(d, that.d)) return false;
      if (!e.equals(that.e)) return false;
      if (!f.equals(that.f)) return false;
      if (!g.equals(that.g)) return false;
      return h.equals(that.h);

    }

    @Override
    public int hashCode() {
      int result = (a ? 1 : 0);
      result = 31 * result + b;
      result = 31 * result + Arrays.hashCode(c);
      result = 31 * result + Arrays.hashCode(d);
      result = 31 * result + e.hashCode();
      result = 31 * result + f.hashCode();
      result = 31 * result + g.hashCode();
      result = 31 * result + h.hashCode();
      return result;
    }
  }

  public static class SimpleJavaBean2 implements Serializable {
    private Timestamp a;
    private Date b;
    private java.math.BigDecimal c;

    public Timestamp getA() { return a; }

    public void setA(Timestamp a) { this.a = a; }

    public Date getB() { return b; }

    public void setB(Date b) { this.b = b; }

    public java.math.BigDecimal getC() { return c; }

    public void setC(java.math.BigDecimal c) { this.c = c; }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SimpleJavaBean2 that = (SimpleJavaBean2) o;

      if (!a.equals(that.a)) return false;
      if (!b.equals(that.b)) return false;
      return c.equals(that.c);
    }

    @Override
    public int hashCode() {
      int result = a.hashCode();
      result = 31 * result + b.hashCode();
      result = 31 * result + c.hashCode();
      return result;
    }
  }

  public static class NestedJavaBean implements Serializable {
    private SimpleJavaBean a;

    public SimpleJavaBean getA() {
      return a;
    }

    public void setA(SimpleJavaBean a) {
      this.a = a;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      NestedJavaBean that = (NestedJavaBean) o;

      return a.equals(that.a);
    }

    @Override
    public int hashCode() {
      return a.hashCode();
    }
  }

  @Test
  public void testJavaBeanEncoder() {
    OuterScopes.addOuterScope(this);
    SimpleJavaBean obj1 = new SimpleJavaBean();
    obj1.setA(true);
    obj1.setB(3);
    obj1.setC(new byte[]{1, 2});
    obj1.setD(new String[]{"hello", null});
    obj1.setE(Arrays.asList("a", "b"));
    obj1.setF(Arrays.asList(100L, null, 200L));
    Map<Integer, String> map1 = new HashMap<Integer, String>();
    map1.put(1, "a");
    map1.put(2, "b");
    obj1.setG(map1);
    Map<String, String> nestedMap1 = new HashMap<String, String>();
    nestedMap1.put("x", "1");
    nestedMap1.put("y", "2");
    Map<List<Long>, Map<String, String>> complexMap1 = new HashMap<>();
    complexMap1.put(Arrays.asList(1L, 2L), nestedMap1);
    obj1.setH(complexMap1);

    SimpleJavaBean obj2 = new SimpleJavaBean();
    obj2.setA(false);
    obj2.setB(30);
    obj2.setC(new byte[]{3, 4});
    obj2.setD(new String[]{null, "world"});
    obj2.setE(Arrays.asList("x", "y"));
    obj2.setF(Arrays.asList(300L, null, 400L));
    Map<Integer, String> map2 = new HashMap<Integer, String>();
    map2.put(3, "c");
    map2.put(4, "d");
    obj2.setG(map2);
    Map<String, String> nestedMap2 = new HashMap<String, String>();
    nestedMap2.put("q", "1");
    nestedMap2.put("w", "2");
    Map<List<Long>, Map<String, String>> complexMap2 = new HashMap<>();
    complexMap2.put(Arrays.asList(3L, 4L), nestedMap2);
    obj2.setH(complexMap2);

    List<SimpleJavaBean> data = Arrays.asList(obj1, obj2);
    Dataset<SimpleJavaBean> ds = spark.createDataset(data, Encoders.bean(SimpleJavaBean.class));
    Assert.assertEquals(data, ds.collectAsList());

    NestedJavaBean obj3 = new NestedJavaBean();
    obj3.setA(obj1);

    List<NestedJavaBean> data2 = Arrays.asList(obj3);
    Dataset<NestedJavaBean> ds2 = spark.createDataset(data2, Encoders.bean(NestedJavaBean.class));
    Assert.assertEquals(data2, ds2.collectAsList());

    Row row1 = new GenericRow(new Object[]{
      true,
      3,
      new byte[]{1, 2},
      new String[]{"hello", null},
      Arrays.asList("a", "b"),
      Arrays.asList(100L, null, 200L),
      map1,
      complexMap1});
    Row row2 = new GenericRow(new Object[]{
      false,
      30,
      new byte[]{3, 4},
      new String[]{null, "world"},
      Arrays.asList("x", "y"),
      Arrays.asList(300L, null, 400L),
      map2,
      complexMap2});
    StructType schema = new StructType()
      .add("a", BooleanType, false)
      .add("b", IntegerType, false)
      .add("c", BinaryType)
      .add("d", createArrayType(StringType))
      .add("e", createArrayType(StringType))
      .add("f", createArrayType(LongType))
      .add("g", createMapType(IntegerType, StringType))
      .add("h",createMapType(createArrayType(LongType), createMapType(StringType, StringType)));
    Dataset<SimpleJavaBean> ds3 = spark.createDataFrame(Arrays.asList(row1, row2), schema)
      .as(Encoders.bean(SimpleJavaBean.class));
    Assert.assertEquals(data, ds3.collectAsList());
  }

  @Test
  public void testJavaBeanEncoder2() {
    // This is a regression test of SPARK-12404
    OuterScopes.addOuterScope(this);
    SimpleJavaBean2 obj = new SimpleJavaBean2();
    obj.setA(new Timestamp(0));
    obj.setB(new Date(0));
    obj.setC(java.math.BigDecimal.valueOf(1));
    Dataset<SimpleJavaBean2> ds =
      spark.createDataset(Arrays.asList(obj), Encoders.bean(SimpleJavaBean2.class));
    ds.collect();
  }

  public static class SmallBean implements Serializable {
    private String a;

    private int b;

    public int getB() {
      return b;
    }

    public void setB(int b) {
      this.b = b;
    }

    public String getA() {
      return a;
    }

    public void setA(String a) {
      this.a = a;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SmallBean smallBean = (SmallBean) o;
      return b == smallBean.b && com.google.common.base.Objects.equal(a, smallBean.a);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(a, b);
    }
  }

  public static class NestedSmallBean implements Serializable {
    private SmallBean f;

    public SmallBean getF() {
      return f;
    }

    public void setF(SmallBean f) {
      this.f = f;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      NestedSmallBean that = (NestedSmallBean) o;
      return Objects.equal(f, that.f);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(f);
    }
  }

  @Rule
  public transient ExpectedException nullabilityCheck = ExpectedException.none();

  @Test
  public void testRuntimeNullabilityCheck() {
    OuterScopes.addOuterScope(this);

    StructType schema = new StructType()
      .add("f", new StructType()
        .add("a", StringType, true)
        .add("b", IntegerType, true), true);

    // Shouldn't throw runtime exception since it passes nullability check.
    {
      Row row = new GenericRow(new Object[] {
          new GenericRow(new Object[] {
              "hello", 1
          })
      });

      Dataset<Row> df = spark.createDataFrame(Collections.singletonList(row), schema);
      Dataset<NestedSmallBean> ds = df.as(Encoders.bean(NestedSmallBean.class));

      SmallBean smallBean = new SmallBean();
      smallBean.setA("hello");
      smallBean.setB(1);

      NestedSmallBean nestedSmallBean = new NestedSmallBean();
      nestedSmallBean.setF(smallBean);

      Assert.assertEquals(ds.collectAsList(), Collections.singletonList(nestedSmallBean));
    }

    // Shouldn't throw runtime exception when parent object (`ClassData`) is null
    {
      Row row = new GenericRow(new Object[] { null });

      Dataset<Row> df = spark.createDataFrame(Collections.singletonList(row), schema);
      Dataset<NestedSmallBean> ds = df.as(Encoders.bean(NestedSmallBean.class));

      NestedSmallBean nestedSmallBean = new NestedSmallBean();
      Assert.assertEquals(ds.collectAsList(), Collections.singletonList(nestedSmallBean));
    }

    nullabilityCheck.expect(RuntimeException.class);
    nullabilityCheck.expectMessage("Null value appeared in non-nullable field");

    {
      Row row = new GenericRow(new Object[] {
          new GenericRow(new Object[] {
              "hello", null
          })
      });

      Dataset<Row> df = spark.createDataFrame(Collections.singletonList(row), schema);
      Dataset<NestedSmallBean> ds = df.as(Encoders.bean(NestedSmallBean.class));

      ds.collect();
    }
  }

  public static class Nesting3 implements Serializable {
    private Integer field3_1;
    private Double field3_2;
    private String field3_3;

    public Nesting3() {
    }

    public Nesting3(Integer field3_1, Double field3_2, String field3_3) {
      this.field3_1 = field3_1;
      this.field3_2 = field3_2;
      this.field3_3 = field3_3;
    }

    private Nesting3(Builder builder) {
      setField3_1(builder.field3_1);
      setField3_2(builder.field3_2);
      setField3_3(builder.field3_3);
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    public Integer getField3_1() {
      return field3_1;
    }

    public void setField3_1(Integer field3_1) {
      this.field3_1 = field3_1;
    }

    public Double getField3_2() {
      return field3_2;
    }

    public void setField3_2(Double field3_2) {
      this.field3_2 = field3_2;
    }

    public String getField3_3() {
      return field3_3;
    }

    public void setField3_3(String field3_3) {
      this.field3_3 = field3_3;
    }

    public static final class Builder {
      private Integer field3_1 = 0;
      private Double field3_2 = 0.0;
      private String field3_3 = "value";

      private Builder() {
      }

      public Builder field3_1(Integer field3_1) {
        this.field3_1 = field3_1;
        return this;
      }

      public Builder field3_2(Double field3_2) {
        this.field3_2 = field3_2;
        return this;
      }

      public Builder field3_3(String field3_3) {
        this.field3_3 = field3_3;
        return this;
      }

      public Nesting3 build() {
        return new Nesting3(this);
      }
    }
  }

  public static class Nesting2 implements Serializable {
    private Nesting3 field2_1;
    private Nesting3 field2_2;
    private Nesting3 field2_3;

    public Nesting2() {
    }

    public Nesting2(Nesting3 field2_1, Nesting3 field2_2, Nesting3 field2_3) {
      this.field2_1 = field2_1;
      this.field2_2 = field2_2;
      this.field2_3 = field2_3;
    }

    private Nesting2(Builder builder) {
      setField2_1(builder.field2_1);
      setField2_2(builder.field2_2);
      setField2_3(builder.field2_3);
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    public Nesting3 getField2_1() {
      return field2_1;
    }

    public void setField2_1(Nesting3 field2_1) {
      this.field2_1 = field2_1;
    }

    public Nesting3 getField2_2() {
      return field2_2;
    }

    public void setField2_2(Nesting3 field2_2) {
      this.field2_2 = field2_2;
    }

    public Nesting3 getField2_3() {
      return field2_3;
    }

    public void setField2_3(Nesting3 field2_3) {
      this.field2_3 = field2_3;
    }


    public static final class Builder {
      private Nesting3 field2_1 = Nesting3.newBuilder().build();
      private Nesting3 field2_2 = Nesting3.newBuilder().build();
      private Nesting3 field2_3 = Nesting3.newBuilder().build();

      private Builder() {
      }

      public Builder field2_1(Nesting3 field2_1) {
        this.field2_1 = field2_1;
        return this;
      }

      public Builder field2_2(Nesting3 field2_2) {
        this.field2_2 = field2_2;
        return this;
      }

      public Builder field2_3(Nesting3 field2_3) {
        this.field2_3 = field2_3;
        return this;
      }

      public Nesting2 build() {
        return new Nesting2(this);
      }
    }
  }

  public static class Nesting1 implements Serializable {
    private Nesting2 field1_1;
    private Nesting2 field1_2;
    private Nesting2 field1_3;

    public Nesting1() {
    }

    public Nesting1(Nesting2 field1_1, Nesting2 field1_2, Nesting2 field1_3) {
      this.field1_1 = field1_1;
      this.field1_2 = field1_2;
      this.field1_3 = field1_3;
    }

    private Nesting1(Builder builder) {
      setField1_1(builder.field1_1);
      setField1_2(builder.field1_2);
      setField1_3(builder.field1_3);
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    public Nesting2 getField1_1() {
      return field1_1;
    }

    public void setField1_1(Nesting2 field1_1) {
      this.field1_1 = field1_1;
    }

    public Nesting2 getField1_2() {
      return field1_2;
    }

    public void setField1_2(Nesting2 field1_2) {
      this.field1_2 = field1_2;
    }

    public Nesting2 getField1_3() {
      return field1_3;
    }

    public void setField1_3(Nesting2 field1_3) {
      this.field1_3 = field1_3;
    }


    public static final class Builder {
      private Nesting2 field1_1 = Nesting2.newBuilder().build();
      private Nesting2 field1_2 = Nesting2.newBuilder().build();
      private Nesting2 field1_3 = Nesting2.newBuilder().build();

      private Builder() {
      }

      public Builder field1_1(Nesting2 field1_1) {
        this.field1_1 = field1_1;
        return this;
      }

      public Builder field1_2(Nesting2 field1_2) {
        this.field1_2 = field1_2;
        return this;
      }

      public Builder field1_3(Nesting2 field1_3) {
        this.field1_3 = field1_3;
        return this;
      }

      public Nesting1 build() {
        return new Nesting1(this);
      }
    }
  }

  public static class NestedComplicatedJavaBean implements Serializable {
    private Nesting1 field1;
    private Nesting1 field2;
    private Nesting1 field3;
    private Nesting1 field4;
    private Nesting1 field5;
    private Nesting1 field6;
    private Nesting1 field7;
    private Nesting1 field8;
    private Nesting1 field9;
    private Nesting1 field10;

    public NestedComplicatedJavaBean() {
    }

    private NestedComplicatedJavaBean(Builder builder) {
      setField1(builder.field1);
      setField2(builder.field2);
      setField3(builder.field3);
      setField4(builder.field4);
      setField5(builder.field5);
      setField6(builder.field6);
      setField7(builder.field7);
      setField8(builder.field8);
      setField9(builder.field9);
      setField10(builder.field10);
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    public Nesting1 getField1() {
      return field1;
    }

    public void setField1(Nesting1 field1) {
      this.field1 = field1;
    }

    public Nesting1 getField2() {
      return field2;
    }

    public void setField2(Nesting1 field2) {
      this.field2 = field2;
    }

    public Nesting1 getField3() {
      return field3;
    }

    public void setField3(Nesting1 field3) {
      this.field3 = field3;
    }

    public Nesting1 getField4() {
      return field4;
    }

    public void setField4(Nesting1 field4) {
      this.field4 = field4;
    }

    public Nesting1 getField5() {
      return field5;
    }

    public void setField5(Nesting1 field5) {
      this.field5 = field5;
    }

    public Nesting1 getField6() {
      return field6;
    }

    public void setField6(Nesting1 field6) {
      this.field6 = field6;
    }

    public Nesting1 getField7() {
      return field7;
    }

    public void setField7(Nesting1 field7) {
      this.field7 = field7;
    }

    public Nesting1 getField8() {
      return field8;
    }

    public void setField8(Nesting1 field8) {
      this.field8 = field8;
    }

    public Nesting1 getField9() {
      return field9;
    }

    public void setField9(Nesting1 field9) {
      this.field9 = field9;
    }

    public Nesting1 getField10() {
      return field10;
    }

    public void setField10(Nesting1 field10) {
      this.field10 = field10;
    }

    public static final class Builder {
      private Nesting1 field1 = Nesting1.newBuilder().build();
      private Nesting1 field2 = Nesting1.newBuilder().build();
      private Nesting1 field3 = Nesting1.newBuilder().build();
      private Nesting1 field4 = Nesting1.newBuilder().build();
      private Nesting1 field5 = Nesting1.newBuilder().build();
      private Nesting1 field6 = Nesting1.newBuilder().build();
      private Nesting1 field7 = Nesting1.newBuilder().build();
      private Nesting1 field8 = Nesting1.newBuilder().build();
      private Nesting1 field9 = Nesting1.newBuilder().build();
      private Nesting1 field10 = Nesting1.newBuilder().build();

      private Builder() {
      }

      public Builder field1(Nesting1 field1) {
        this.field1 = field1;
        return this;
      }

      public Builder field2(Nesting1 field2) {
        this.field2 = field2;
        return this;
      }

      public Builder field3(Nesting1 field3) {
        this.field3 = field3;
        return this;
      }

      public Builder field4(Nesting1 field4) {
        this.field4 = field4;
        return this;
      }

      public Builder field5(Nesting1 field5) {
        this.field5 = field5;
        return this;
      }

      public Builder field6(Nesting1 field6) {
        this.field6 = field6;
        return this;
      }

      public Builder field7(Nesting1 field7) {
        this.field7 = field7;
        return this;
      }

      public Builder field8(Nesting1 field8) {
        this.field8 = field8;
        return this;
      }

      public Builder field9(Nesting1 field9) {
        this.field9 = field9;
        return this;
      }

      public Builder field10(Nesting1 field10) {
        this.field10 = field10;
        return this;
      }

      public NestedComplicatedJavaBean build() {
        return new NestedComplicatedJavaBean(this);
      }
    }
  }

  @Test
  public void test() {
    /* SPARK-15285 Large numbers of Nested JavaBeans generates more than 64KB java bytecode */
    List<NestedComplicatedJavaBean> data = new ArrayList<NestedComplicatedJavaBean>();
    data.add(NestedComplicatedJavaBean.newBuilder().build());

    NestedComplicatedJavaBean obj3 = new NestedComplicatedJavaBean();

    Dataset<NestedComplicatedJavaBean> ds =
      spark.createDataset(data, Encoders.bean(NestedComplicatedJavaBean.class));
    ds.collectAsList();
  }
}
