package spark.examples;

import scala.Tuple2;
import scala.collection.immutable.StringOps;
import spark.api.java.JavaPairRDD;
import spark.api.java.JavaRDD;
import spark.api.java.JavaSparkContext;
import spark.api.java.function.FlatMapFunction;
import spark.api.java.function.Function2;
import spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.List;

public class JavaWordCount {

  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      System.err.println("Usage: JavaWordCount <master> <file>");
      System.exit(1);
    }

    JavaSparkContext ctx = new JavaSparkContext(args[0], "JavaWordCount");
    JavaRDD<String> lines = ctx.textFile(args[1], 1);

    JavaPairRDD<String, Integer> counts = lines.flatMap(new FlatMapFunction<String, String>() {
      public Iterable<String> apply(String s) {
        StringOps op = new StringOps(s);
        return Arrays.asList(op.split(' '));
      }
    }).map(new PairFunction<String, String, Integer>() {
      public Tuple2<String, Integer> apply(String s) {
        return new Tuple2(s, 1);
      }
    }).reduceByKey(new Function2<Integer, Integer, Integer>() {
      public Integer apply(Integer i1, Integer i2) {
        return i1 + i2;
      }
    });

    List<Tuple2<String, Integer>> output = counts.collect();
    for (Tuple2 tuple : output) {
      System.out.print(tuple._1 + ": ");
      System.out.println(tuple._2);
    }
    System.exit(0);
  }
}
