package spark.streaming.examples;

import spark.api.java.function.Function;
import spark.streaming.*;
import spark.streaming.api.java.*;
import spark.streaming.dstream.SparkFlumeEvent;

/**
 *  Produces a count of events received from Flume.
 *
 *  This should be used in conjunction with an AvroSink in Flume. It will start
 *  an Avro server on at the request host:port address and listen for requests.
 *  Your Flume AvroSink should be pointed to this address.
 *
 *  Usage: JavaFlumeEventCount <master> <host> <port>
 *
 *    <master> is a Spark master URL
 *    <host> is the host the Flume receiver will be started on - a receiver
 *           creates a server and listens for flume events.
 *    <port> is the port the Flume receiver will listen on.
 */
public class JavaFlumeEventCount {
  public static void main(String[] args) {
    if (args.length != 3) {
      System.err.println("Usage: JavaFlumeEventCount <master> <host> <port>");
      System.exit(1);
    }

    String master = args[0];
    String host = args[1];
    int port = Integer.parseInt(args[2]);

    Duration batchInterval = new Duration(2000);

    JavaStreamingContext sc = new JavaStreamingContext(master, "FlumeEventCount", batchInterval,
            System.getenv("SPARK_HOME"), System.getenv("SPARK_EXAMPLES_JAR"));

    JavaDStream<SparkFlumeEvent> flumeStream = sc.flumeStream("localhost", port);

    flumeStream.count();

    flumeStream.count().map(new Function<Long, String>() {
      @Override
      public String call(Long in) {
        return "Received " + in + " flume events.";
      }
    }).print();

    sc.start();
  }
}
