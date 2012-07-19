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

  public static class SplitFunction extends FlatMapFunction<String, String> {
    @Override
    public Iterable<String> apply(String s) {
      StringOps op = new StringOps(s);
      return Arrays.asList(op.split(' '));
    }
  }
  
  public static class MapFunction extends PairFunction<String, String, Integer> {
    @Override
    public Tuple2<String, Integer> apply(String s) {
      return new Tuple2(s, 1);
    }
  }
	
  public static class ReduceFunction extends Function2<Integer, Integer, Integer> {
    @Override
    public Integer apply(Integer i1, Integer i2) {
      return i1 + i2;
    }
  }
  public static void main(String[] args) throws Exception {
    JavaSparkContext ctx = new JavaSparkContext("local", "JavaWordCount");
    JavaRDD<String> lines = ctx.textFile("numbers.txt", 1).cache();
    List<String> lineArr = lines.collect();

    for (String line : lineArr) {
      System.out.println(line);
    }

    JavaRDD<String> words = lines.flatMap(new SplitFunction());
    
    JavaPairRDD<String, Integer> splits = words.map(new MapFunction());

    JavaPairRDD<String, Integer> counts = splits.reduceByKey(new ReduceFunction());
    
    System.out.println("output");
    List<Tuple2<String, Integer>> output = counts.collect();
    for (Tuple2 tuple : output) {
      System.out.print(tuple._1 + ": ");
      System.out.println(tuple._2);
    }
    System.exit(0);
  }
}
