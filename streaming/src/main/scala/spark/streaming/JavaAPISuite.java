package spark.streaming;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import spark.api.java.JavaRDD;
import spark.api.java.function.Function;
import spark.api.java.function.Function2;
import spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;

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
  public void simpleTest() {
    sc.textFileStream("/var/log/syslog").print();
    sc.start();
  }

  public static void main(String[] args) {
    JavaStreamingContext sc = new JavaStreamingContext("local[2]", "test", new Time(1000));

    sc.networkTextStream("localhost", 12345).map(new Function<String, Integer>() {
        @Override
        public Integer call(String s) throws Exception {
            return s.length();
        }
    }).reduce(new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer i1, Integer i2) throws Exception {
            return i1 + i2;
        }
    }).foreach(new Function2<JavaRDD<Integer>, Time, Void>() {
        @Override
        public Void call(JavaRDD<Integer> integerJavaRDD, Time t) throws Exception {
            System.out.println("Contents @ " + t.toFormattedString());
            for (int i: integerJavaRDD.collect()) {
              System.out.println(i + "\n");
            }
            return null;
        }
    });

    sc.start();
  }
}
