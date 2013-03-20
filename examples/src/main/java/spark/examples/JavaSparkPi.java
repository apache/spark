package spark.examples;

import spark.api.java.JavaRDD;
import spark.api.java.JavaSparkContext;
import spark.api.java.function.Function;
import spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.List;

/** Computes an approximation to pi */
public class JavaSparkPi {


  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("Usage: JavaLogQuery <master> [slices]");
      System.exit(1);
    }

    JavaSparkContext jsc = new JavaSparkContext(args[0], "JavaLogQuery",
      System.getenv("SPARK_HOME"), System.getenv("SPARK_EXAMPLES_JAR"));

    int slices = (args.length == 2) ? Integer.parseInt(args[1]) : 2;
    int n = 100000 * slices;
    List<Integer> l = new ArrayList<Integer>(n);
    for (int i = 0; i < n; i++)
      l.add(i);

    JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

    int count = dataSet.map(new Function<Integer, Integer>() {
      @Override
      public Integer call(Integer integer) throws Exception {
        double x = Math.random() * 2 - 1;
        double y = Math.random() * 2 - 1;
        return (x * x + y * y < 1) ? 1 : 0;
      }
    }).reduce(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer integer, Integer integer2) throws Exception {
        return integer + integer2;
      }
    });

    System.out.println("Pi is roughly " + 4.0 * count / n);
  }
}
