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
import java.util.*;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;
import org.junit.*;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.GroupedDataset;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.test.TestSQLContext;

import static org.apache.spark.sql.functions.*;

public class JavaDatasetSuite implements Serializable {
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

  private <T1, T2> Tuple2<T1, T2> tuple2(T1 t1, T2 t2) {
    return new Tuple2<T1, T2>(t1, t2);
  }

  @Test
  public void testCollect() {
    List<String> data = Arrays.asList("hello", "world");
    Dataset<String> ds = context.createDataset(data, Encoders.STRING());
    List<String> collected = ds.collectAsList();
    Assert.assertEquals(Arrays.asList("hello", "world"), collected);
  }

  @Test
  public void testTake() {
    List<String> data = Arrays.asList("hello", "world");
    Dataset<String> ds = context.createDataset(data, Encoders.STRING());
    List<String> collected = ds.takeAsList(1);
    Assert.assertEquals(Arrays.asList("hello"), collected);
  }

  @Test
  public void testCommonOperation() {
    List<String> data = Arrays.asList("hello", "world");
    Dataset<String> ds = context.createDataset(data, Encoders.STRING());
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
      public Iterable<String> call(Iterator<String> it) throws Exception {
        List<String> ls = new LinkedList<String>();
        while (it.hasNext()) {
          ls.add(it.next().toUpperCase());
        }
        return ls;
      }
    }, Encoders.STRING());
    Assert.assertEquals(Arrays.asList("HELLO", "WORLD"), parMapped.collectAsList());

    Dataset<String> flatMapped = ds.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String s) throws Exception {
        List<String> ls = new LinkedList<String>();
        for (char c : s.toCharArray()) {
          ls.add(String.valueOf(c));
        }
        return ls;
      }
    }, Encoders.STRING());
    Assert.assertEquals(
      Arrays.asList("h", "e", "l", "l", "o", "w", "o", "r", "l", "d"),
      flatMapped.collectAsList());
  }

  @Test
  public void testForeach() {
    final Accumulator<Integer> accum = jsc.accumulator(0);
    List<String> data = Arrays.asList("a", "b", "c");
    Dataset<String> ds = context.createDataset(data, Encoders.STRING());

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
    Dataset<Integer> ds = context.createDataset(data, Encoders.INT());

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
    Dataset<String> ds = context.createDataset(data, Encoders.STRING());
    GroupedDataset<Integer, String> grouped = ds.groupBy(new MapFunction<String, Integer>() {
      @Override
      public Integer call(String v) throws Exception {
        return v.length();
      }
    }, Encoders.INT());

    Dataset<String> mapped = grouped.map(new MapGroupFunction<Integer, String, String>() {
      @Override
      public String call(Integer key, Iterator<String> values) throws Exception {
        StringBuilder sb = new StringBuilder(key.toString());
        while (values.hasNext()) {
          sb.append(values.next());
        }
        return sb.toString();
      }
    }, Encoders.STRING());

    Assert.assertEquals(Arrays.asList("1a", "3foobar"), mapped.collectAsList());

    Dataset<String> flatMapped = grouped.flatMap(
      new FlatMapGroupFunction<Integer, String, String>() {
        @Override
        public Iterable<String> call(Integer key, Iterator<String> values) throws Exception {
          StringBuilder sb = new StringBuilder(key.toString());
          while (values.hasNext()) {
            sb.append(values.next());
          }
          return Collections.singletonList(sb.toString());
        }
      },
      Encoders.STRING());

    Assert.assertEquals(Arrays.asList("1a", "3foobar"), flatMapped.collectAsList());

    Dataset<Tuple2<Integer, String>> reduced = grouped.reduce(new ReduceFunction<String>() {
      @Override
      public String call(String v1, String v2) throws Exception {
        return v1 + v2;
      }
    });

    Assert.assertEquals(
      Arrays.asList(tuple2(1, "a"), tuple2(3, "foobar")),
      reduced.collectAsList());

    List<Integer> data2 = Arrays.asList(2, 6, 10);
    Dataset<Integer> ds2 = context.createDataset(data2, Encoders.INT());
    GroupedDataset<Integer, Integer> grouped2 = ds2.groupBy(new MapFunction<Integer, Integer>() {
      @Override
      public Integer call(Integer v) throws Exception {
        return v / 2;
      }
    }, Encoders.INT());

    Dataset<String> cogrouped = grouped.cogroup(
      grouped2,
      new CoGroupFunction<Integer, String, Integer, String>() {
        @Override
        public Iterable<String> call(
          Integer key,
          Iterator<String> left,
          Iterator<Integer> right) throws Exception {
          StringBuilder sb = new StringBuilder(key.toString());
          while (left.hasNext()) {
            sb.append(left.next());
          }
          sb.append("#");
          while (right.hasNext()) {
            sb.append(right.next());
          }
          return Collections.singletonList(sb.toString());
        }
      },
      Encoders.STRING());

    Assert.assertEquals(Arrays.asList("1a#2", "3foobar#6", "5#10"), cogrouped.collectAsList());
  }

  @Test
  public void testGroupByColumn() {
    List<String> data = Arrays.asList("a", "foo", "bar");
    Dataset<String> ds = context.createDataset(data, Encoders.STRING());
    GroupedDataset<Integer, String> grouped =
      ds.groupBy(length(col("value"))).asKey(Encoders.INT());

    Dataset<String> mapped = grouped.map(
      new MapGroupFunction<Integer, String, String>() {
        @Override
        public String call(Integer key, Iterator<String> data) throws Exception {
          StringBuilder sb = new StringBuilder(key.toString());
          while (data.hasNext()) {
            sb.append(data.next());
          }
          return sb.toString();
        }
      },
      Encoders.STRING());

    Assert.assertEquals(Arrays.asList("1a", "3foobar"), mapped.collectAsList());
  }

  @Test
  public void testSelect() {
    List<Integer> data = Arrays.asList(2, 6);
    Dataset<Integer> ds = context.createDataset(data, Encoders.INT());

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
    Dataset<String> ds = context.createDataset(data, Encoders.STRING());

    Assert.assertEquals(
      Arrays.asList("abc", "xyz"),
      sort(ds.distinct().collectAsList().toArray(new String[0])));

    List<String> data2 = Arrays.asList("xyz", "foo", "foo");
    Dataset<String> ds2 = context.createDataset(data2, Encoders.STRING());

    Dataset<String> intersected = ds.intersect(ds2);
    Assert.assertEquals(Arrays.asList("xyz"), intersected.collectAsList());

    Dataset<String> unioned = ds.union(ds2);
    Assert.assertEquals(
      Arrays.asList("abc", "abc", "foo", "foo", "xyz", "xyz"),
      sort(unioned.collectAsList().toArray(new String[0])));

    Dataset<String> subtracted = ds.subtract(ds2);
    Assert.assertEquals(Arrays.asList("abc", "abc"), subtracted.collectAsList());
  }

  private <T extends Comparable<T>> List<T> sort(T[] data) {
    Arrays.sort(data);
    return Arrays.asList(data);
  }

  @Test
  public void testJoin() {
    List<Integer> data = Arrays.asList(1, 2, 3);
    Dataset<Integer> ds = context.createDataset(data, Encoders.INT()).as("a");
    List<Integer> data2 = Arrays.asList(2, 3, 4);
    Dataset<Integer> ds2 = context.createDataset(data2, Encoders.INT()).as("b");

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
    Dataset<Tuple2<Integer, String>> ds2 = context.createDataset(data2, encoder2);
    Assert.assertEquals(data2, ds2.collectAsList());

    Encoder<Tuple3<Integer, Long, String>> encoder3 =
      Encoders.tuple(Encoders.INT(), Encoders.LONG(), Encoders.STRING());
    List<Tuple3<Integer, Long, String>> data3 =
      Arrays.asList(new Tuple3<Integer, Long, String>(1, 2L, "a"));
    Dataset<Tuple3<Integer, Long, String>> ds3 = context.createDataset(data3, encoder3);
    Assert.assertEquals(data3, ds3.collectAsList());

    Encoder<Tuple4<Integer, String, Long, String>> encoder4 =
      Encoders.tuple(Encoders.INT(), Encoders.STRING(), Encoders.LONG(), Encoders.STRING());
    List<Tuple4<Integer, String, Long, String>> data4 =
      Arrays.asList(new Tuple4<Integer, String, Long, String>(1, "b", 2L, "a"));
    Dataset<Tuple4<Integer, String, Long, String>> ds4 = context.createDataset(data4, encoder4);
    Assert.assertEquals(data4, ds4.collectAsList());

    Encoder<Tuple5<Integer, String, Long, String, Boolean>> encoder5 =
      Encoders.tuple(Encoders.INT(), Encoders.STRING(), Encoders.LONG(), Encoders.STRING(),
        Encoders.BOOLEAN());
    List<Tuple5<Integer, String, Long, String, Boolean>> data5 =
      Arrays.asList(new Tuple5<Integer, String, Long, String, Boolean>(1, "b", 2L, "a", true));
    Dataset<Tuple5<Integer, String, Long, String, Boolean>> ds5 =
      context.createDataset(data5, encoder5);
    Assert.assertEquals(data5, ds5.collectAsList());
  }

  @Test
  public void testNestedTupleEncoder() {
    // test ((int, string), string)
    Encoder<Tuple2<Tuple2<Integer, String>, String>> encoder =
      Encoders.tuple(Encoders.tuple(Encoders.INT(), Encoders.STRING()), Encoders.STRING());
    List<Tuple2<Tuple2<Integer, String>, String>> data =
      Arrays.asList(tuple2(tuple2(1, "a"), "a"), tuple2(tuple2(2, "b"), "b"));
    Dataset<Tuple2<Tuple2<Integer, String>, String>> ds = context.createDataset(data, encoder);
    Assert.assertEquals(data, ds.collectAsList());

    // test (int, (string, string, long))
    Encoder<Tuple2<Integer, Tuple3<String, String, Long>>> encoder2 =
      Encoders.tuple(Encoders.INT(),
        Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.LONG()));
    List<Tuple2<Integer, Tuple3<String, String, Long>>> data2 =
      Arrays.asList(tuple2(1, new Tuple3<String, String, Long>("a", "b", 3L)));
    Dataset<Tuple2<Integer, Tuple3<String, String, Long>>> ds2 =
      context.createDataset(data2, encoder2);
    Assert.assertEquals(data2, ds2.collectAsList());

    // test (int, ((string, long), string))
    Encoder<Tuple2<Integer, Tuple2<Tuple2<String, Long>, String>>> encoder3 =
      Encoders.tuple(Encoders.INT(),
        Encoders.tuple(Encoders.tuple(Encoders.STRING(), Encoders.LONG()), Encoders.STRING()));
    List<Tuple2<Integer, Tuple2<Tuple2<String, Long>, String>>> data3 =
      Arrays.asList(tuple2(1, tuple2(tuple2("a", 2L), "b")));
    Dataset<Tuple2<Integer, Tuple2<Tuple2<String, Long>, String>>> ds3 =
      context.createDataset(data3, encoder3);
    Assert.assertEquals(data3, ds3.collectAsList());
  }

  @Test
  public void testTypedAggregation() {
    Encoder<Tuple2<String, Integer>> encoder = Encoders.tuple(Encoders.STRING(), Encoders.INT());
    List<Tuple2<String, Integer>> data =
      Arrays.asList(tuple2("a", 1), tuple2("a", 2), tuple2("b", 3));
    Dataset<Tuple2<String, Integer>> ds = context.createDataset(data, encoder);

    GroupedDataset<String, Tuple2<String, Integer>> grouped = ds.groupBy(
      new MapFunction<Tuple2<String, Integer>, String>() {
        @Override
        public String call(Tuple2<String, Integer> value) throws Exception {
          return value._1();
        }
      },
      Encoders.STRING());

    Dataset<Tuple2<String, Integer>> agged =
      grouped.agg(new IntSumOf().toColumn(Encoders.INT(), Encoders.INT()));
    Assert.assertEquals(Arrays.asList(tuple2("a", 3), tuple2("b", 3)), agged.collectAsList());

    Dataset<Tuple4<String, Integer, Long, Long>> agged2 = grouped.agg(
      new IntSumOf().toColumn(Encoders.INT(), Encoders.INT()),
      expr("sum(_2)"),
      count("*"))
      .as(Encoders.tuple(Encoders.STRING(), Encoders.INT(), Encoders.LONG(), Encoders.LONG()));
    Assert.assertEquals(
      Arrays.asList(
        new Tuple4<String, Integer, Long, Long>("a", 3, 3L, 2L),
        new Tuple4<String, Integer, Long, Long>("b", 3, 3L, 1L)),
      agged2.collectAsList());
  }

  static class IntSumOf extends Aggregator<Tuple2<String, Integer>, Integer, Integer> {

    @Override
    public Integer zero() {
      return 0;
    }

    @Override
    public Integer reduce(Integer l, Tuple2<String, Integer> t) {
      return l + t._2();
    }

    @Override
    public Integer merge(Integer b1, Integer b2) {
      return b1 + b2;
    }

    @Override
    public Integer finish(Integer reduction) {
      return reduction;
    }
  }
}
