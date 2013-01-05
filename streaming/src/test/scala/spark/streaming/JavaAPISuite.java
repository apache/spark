package spark.streaming;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import spark.api.java.JavaRDD;
import spark.api.java.function.FlatMapFunction;
import spark.api.java.function.Function;
import spark.api.java.function.Function2;
import spark.streaming.JavaTestUtils;
import spark.streaming.api.java.JavaDStream;
import spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;
import java.util.*;

// The test suite itself is Serializable so that anonymous Function implementations can be
// serialized, as an alternative to converting these anonymous classes to static inner classes;
// see http://stackoverflow.com/questions/758570/.
public class JavaAPISuite implements Serializable {
  private transient JavaStreamingContext sc;

  @Before
  public void setUp() {
    sc = new JavaStreamingContext("local[2]", "test", new Time(1000));
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.master.port");
  }

  @Test
  public void testCount() {
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,2,3,4),
        Arrays.asList(3,4,5),
        Arrays.asList(3));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(4),
        Arrays.asList(3),
        Arrays.asList(1));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream count = stream.count();
    JavaTestUtils.attachTestOutputStream(count);
    List<List<Integer>> result = JavaTestUtils.runStreams(sc, 3, 3);
    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testMap() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("hello", "world"),
        Arrays.asList("goodnight", "moon"));

   List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(5,5),
        Arrays.asList(9,4));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream letterCount = stream.map(new Function<String, Integer>() {
        @Override
        public Integer call(String s) throws Exception {
          return s.length();
        }
    });
    JavaTestUtils.attachTestOutputStream(letterCount);
    List<List<Integer>> result = JavaTestUtils.runStreams(sc, 2, 2);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testWindow() {
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6),
        Arrays.asList(7,8,9));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6,1,2,3),
        Arrays.asList(7,8,9,4,5,6),
        Arrays.asList(7,8,9));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream windowed = stream.window(new Time(2000));
    JavaTestUtils.attachTestOutputStream(windowed);
    List<List<Integer>> result = JavaTestUtils.runStreams(sc, 4, 4);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testWindowWithSlideTime() {
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6),
        Arrays.asList(7,8,9),
        Arrays.asList(10,11,12),
        Arrays.asList(13,14,15),
        Arrays.asList(16,17,18));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(1,2,3,4,5,6),
        Arrays.asList(1,2,3,4,5,6,7,8,9,10,11,12),
        Arrays.asList(7,8,9,10,11,12,13,14,15,16,17,18),
        Arrays.asList(13,14,15,16,17,18));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream windowed = stream.window(new Time(4000), new Time(2000));
    JavaTestUtils.attachTestOutputStream(windowed);
    List<List<Integer>> result = JavaTestUtils.runStreams(sc, 8, 4);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testTumble() {
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6),
        Arrays.asList(7,8,9),
        Arrays.asList(10,11,12),
        Arrays.asList(13,14,15),
        Arrays.asList(16,17,18));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(1,2,3,4,5,6),
        Arrays.asList(7,8,9,10,11,12),
        Arrays.asList(13,14,15,16,17,18));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream windowed = stream.tumble(new Time(2000));
    JavaTestUtils.attachTestOutputStream(windowed);
    List<List<Integer>> result = JavaTestUtils.runStreams(sc, 6, 3);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testFilter() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("giants", "dodgers"),
        Arrays.asList("yankees", "red socks"));

    List<List<String>> expected = Arrays.asList(
        Arrays.asList("giants"),
        Arrays.asList("yankees"));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream filtered = stream.filter(new Function<String, Boolean>() {
      @Override
      public Boolean call(String s) throws Exception {
        return s.contains("a");
      }
    });
    JavaTestUtils.attachTestOutputStream(filtered);
    List<List<String>> result = JavaTestUtils.runStreams(sc, 2, 2);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testGlom() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("giants", "dodgers"),
        Arrays.asList("yankees", "red socks"));

    List<List<List<String>>> expected = Arrays.asList(
        Arrays.asList(Arrays.asList("giants", "dodgers")),
        Arrays.asList(Arrays.asList("yankees", "red socks")));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream glommed = stream.glom();
    JavaTestUtils.attachTestOutputStream(glommed);
    List<List<List<String>>> result = JavaTestUtils.runStreams(sc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testMapPartitions() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("giants", "dodgers"),
        Arrays.asList("yankees", "red socks"));

    List<List<String>> expected = Arrays.asList(
        Arrays.asList("GIANTSDODGERS"),
        Arrays.asList("YANKEESRED SOCKS"));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream mapped = stream.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
      @Override
      public Iterable<String> call(Iterator<String> in) {
        String out = "";
        while (in.hasNext()) {
          out = out + in.next().toUpperCase();
        }
        return Lists.newArrayList(out);
      }
    });
    JavaTestUtils.attachTestOutputStream(mapped);
    List<List<List<String>>> result = JavaTestUtils.runStreams(sc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  private class IntegerSum extends Function2<Integer, Integer, Integer> {
    @Override
    public Integer call(Integer i1, Integer i2) throws Exception {
      return i1 + i2;
    }
  }

  private class IntegerDifference extends Function2<Integer, Integer, Integer> {
    @Override
    public Integer call(Integer i1, Integer i2) throws Exception {
      return i1 - i2;
    }
  }

  @Test
  public void testReduce() {
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6),
        Arrays.asList(7,8,9));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(6),
        Arrays.asList(15),
        Arrays.asList(24));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream reduced = stream.reduce(new IntegerSum());
    JavaTestUtils.attachTestOutputStream(reduced);
    List<List<Integer>> result = JavaTestUtils.runStreams(sc, 3, 3);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testReduceByWindow() {
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6),
        Arrays.asList(7,8,9));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(6),
        Arrays.asList(21),
        Arrays.asList(39),
        Arrays.asList(24));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream reducedWindowed = stream.reduceByWindow(new IntegerSum(),
        new IntegerDifference(), new Time(2000), new Time(1000));
    JavaTestUtils.attachTestOutputStream(reducedWindowed);
    List<List<Integer>> result = JavaTestUtils.runStreams(sc, 4, 4);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testTransform() {
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6),
        Arrays.asList(7,8,9));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(3,4,5),
        Arrays.asList(6,7,8),
        Arrays.asList(9,10,11));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream transformed = stream.transform(new Function<JavaRDD<Integer>, JavaRDD<Integer>>() {
      @Override
      public JavaRDD<Integer> call(JavaRDD<Integer> in) throws Exception {
        return in.map(new Function<Integer, Integer>() {
          @Override
          public Integer call(Integer i) throws Exception {
            return i + 2;
          }
        });
      }});
    JavaTestUtils.attachTestOutputStream(transformed);
    List<List<Integer>> result = JavaTestUtils.runStreams(sc, 3, 3);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testUnion() {
    List<List<Integer>> inputData1 = Arrays.asList(
        Arrays.asList(1,1),
        Arrays.asList(2,2),
        Arrays.asList(3,3));

    List<List<Integer>> inputData2 = Arrays.asList(
        Arrays.asList(4,4),
        Arrays.asList(5,5),
        Arrays.asList(6,6));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(1,1,4,4),
        Arrays.asList(2,2,5,5),
        Arrays.asList(3,3,6,6));

    JavaDStream stream1 = JavaTestUtils.attachTestInputStream(sc, inputData1, 2);
    JavaDStream stream2 = JavaTestUtils.attachTestInputStream(sc, inputData2, 2);

    JavaDStream unioned = stream1.union(stream2);
    JavaTestUtils.attachTestOutputStream(unioned);
    List<List<Integer>> result = JavaTestUtils.runStreams(sc, 3, 3);

    assertOrderInvariantEquals(expected, result);
  }

  /*
   * Performs an order-invariant comparison of lists representing two RDD streams. This allows
   * us to account for ordering variation within individual RDD's which occurs during windowing.
   */
  public static <T extends Comparable> void assertOrderInvariantEquals(
      List<List<T>> expected, List<List<T>> actual) {
    for (List<T> list: expected) {
      Collections.sort(list);
    }
    for (List<T> list: actual) {
      Collections.sort(list);
    }
    Assert.assertEquals(expected, actual);
  }

}
