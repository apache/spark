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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.catalyst.encoders.Encoder;
import scala.Tuple2;

import org.apache.spark.api.java.function.Function;
import org.junit.*;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.encoders.Encoder$;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.test.TestSQLContext;

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

  @Test
  public void testCommonOperation() {
    List<String> data = Arrays.asList("hello", "world");
    Dataset<String> ds = context.createDataset(data, Encoder$.MODULE$.forString());
    Assert.assertEquals("hello", ds.first());

    Dataset<String> filtered = ds.filter(new Function<String, Boolean>() {
      @Override
      public Boolean call(String v) throws Exception {
        return v.startsWith("h");
      }
    });
    Assert.assertEquals(Arrays.asList("hello"), filtered.jcollect());


    Dataset<Integer> mapped = ds.map(new Function<String, Integer>() {
      @Override
      public Integer call(String v) throws Exception {
        return v.length();
      }
    }, Encoder$.MODULE$.forInt());
    Assert.assertEquals(Arrays.asList(5, 5), mapped.jcollect());

    Dataset<String> parMapped = ds.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
      @Override
      public Iterable<String> call(Iterator<String> it) throws Exception {
        List<String> ls = new LinkedList<String>();
        while (it.hasNext()) {
          ls.add(it.next().toUpperCase());
        }
        return ls;
      }
    }, Encoder$.MODULE$.forString());
    Assert.assertEquals(Arrays.asList("HELLO", "WORLD"), parMapped.jcollect());
  }

  @Test
  public void testTuple2Encoder() {
    Encoder<Tuple2<Integer, String>> encoder =
      Encoder$.MODULE$.forTuple2(Integer.class, String.class);
    List<Tuple2<Integer, String>> data =
      Arrays.<Tuple2<Integer, String>>asList(new Tuple2(1, "a"), new Tuple2(2, "b"));
    Dataset<Tuple2<Integer, String>> ds = context.createDataset(data, encoder);

    Dataset<String> mapped = ds.map(new Function<Tuple2<Integer, String>, String>() {
      @Override
      public String call(Tuple2<Integer, String> v) throws Exception {
        return v._2();
      }
    }, Encoder$.MODULE$.forString());
    Assert.assertEquals(Arrays.asList("a", "b"), mapped.jcollect());
  }
}
