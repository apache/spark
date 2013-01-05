package spark.streaming;

import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import spark.api.java.JavaRDD;
import spark.api.java.function.Function;
import spark.api.java.function.Function2;
import spark.streaming.JavaTestUtils;
import spark.streaming.api.java.JavaDStream;
import spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
            Arrays.asList(1,2,3,4), Arrays.asList(3,4,5), Arrays.asList(3));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream count = stream.count();
    JavaTestUtils.attachTestOutputStream(count);
    List<List<Integer>> result = JavaTestUtils.runStreams(sc, 3, 3);

    Assert.assertEquals(result,
        Arrays.asList(Arrays.asList(4), Arrays.asList(3), Arrays.asList(1)));
  }

  @Test
  public void testMap() {
    List<List<String>> inputData = Arrays.asList(
            Arrays.asList("hello", "world"), Arrays.asList("goodnight", "moon"));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream letterCount = stream.map(new Function<String, Integer>() {
        @Override
        public Integer call(String s) throws Exception {
          return s.length();
        }
    });
    JavaTestUtils.attachTestOutputStream(letterCount);
    List<List<Integer>> result = JavaTestUtils.runStreams(sc, 2, 2);

    Assert.assertEquals(result,  Arrays.asList(Arrays.asList(5, 5), Arrays.asList(9, 4)));
  }
}
