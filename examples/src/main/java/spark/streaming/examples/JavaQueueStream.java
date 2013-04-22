package spark.streaming.examples;

import com.google.common.collect.Lists;
import scala.Tuple2;
import spark.api.java.JavaRDD;
import spark.api.java.function.Function2;
import spark.api.java.function.PairFunction;
import spark.streaming.Duration;
import spark.streaming.api.java.JavaDStream;
import spark.streaming.api.java.JavaPairDStream;
import spark.streaming.api.java.JavaStreamingContext;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class JavaQueueStream {
  public static void main(String[] args) throws InterruptedException {
    if (args.length < 1) {
      System.err.println("Usage: JavaQueueStream <master>");
      System.exit(1);
    }

    // Create the context
    JavaStreamingContext ssc = new JavaStreamingContext(args[0], "QueueStream", new Duration(1000),
            System.getenv("SPARK_HOME"), System.getenv("SPARK_EXAMPLES_JAR"));

    // Create the queue through which RDDs can be pushed to
    // a QueueInputDStream
    Queue<JavaRDD<Integer>> rddQueue = new LinkedList<JavaRDD<Integer>>();

    // Create and push some RDDs into the queue
    List<Integer> list = Lists.newArrayList();
    for (int i = 0; i < 1000; i++) {
      list.add(i);
    }

    for (int i = 0; i < 30; i++) {
      rddQueue.add(ssc.sc().parallelize(list));
    }


    // Create the QueueInputDStream and use it do some processing
    JavaDStream<Integer> inputStream = ssc.queueStream(rddQueue);
    JavaPairDStream<Integer, Integer> mappedStream = inputStream.map(
        new PairFunction<Integer, Integer, Integer>() {
          @Override
          public Tuple2<Integer, Integer> call(Integer i) throws Exception {
            return new Tuple2<Integer, Integer>(i % 10, 1);
          }
        });
    JavaPairDStream<Integer, Integer> reducedStream = mappedStream.reduceByKey(
      new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer i1, Integer i2) throws Exception {
          return i1 + i2;
        }
    });

    reducedStream.print();
    ssc.start();
  }
}
