package spark.examples;

import spark.api.java.JavaDoubleRDD;
import spark.api.java.JavaRDD;
import spark.api.java.JavaSparkContext;
import spark.api.java.function.DoubleFunction;

import java.util.List;

public class JavaTest {

  public static class MapFunction extends DoubleFunction<String> {
    @Override
    public Double apply(String s) {
      return java.lang.Double.parseDouble(s);
    }
  }

  public static void main(String[] args) throws Exception {
    
    JavaSparkContext ctx = new JavaSparkContext("local", "JavaTest");
    JavaRDD<String> lines = ctx.textFile("numbers.txt", 1).cache();
    List<String> lineArr = lines.collect();
    
    for (String line : lineArr) {
      System.out.println(line);
    }

    JavaDoubleRDD data = lines.map(new MapFunction()).cache();

    System.out.println("output");
    List<Double> output = data.collect();
    for (Double num : output) {
     System.out.println(num);
    }
    System.exit(0);
  }
}
