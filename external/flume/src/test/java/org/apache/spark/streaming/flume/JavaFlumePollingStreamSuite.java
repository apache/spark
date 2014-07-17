package org.apache.spark.streaming.flume;

import java.net.InetSocketAddress;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.LocalJavaStreamingContext;

import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.junit.Test;

public class JavaFlumePollingStreamSuite extends LocalJavaStreamingContext {
  @Test
  public void testFlumeStream() {
    // tests the API, does not actually test data receiving
    InetSocketAddress[] addresses = new InetSocketAddress[] {
        new InetSocketAddress("localhost", 12345)
    };
    JavaReceiverInputDStream<SparkFlumePollingEvent> test1 =
        FlumeUtils.createPollingStream(ssc, "localhost", 12345);
    JavaReceiverInputDStream<SparkFlumePollingEvent> test2 = FlumeUtils.createPollingStream(
        ssc, "localhost", 12345, StorageLevel.MEMORY_AND_DISK_SER_2());
    JavaReceiverInputDStream<SparkFlumePollingEvent> test3 = FlumeUtils.createPollingStream(
        ssc, addresses, StorageLevel.MEMORY_AND_DISK_SER_2());
    JavaReceiverInputDStream<SparkFlumePollingEvent> test4 = FlumeUtils.createPollingStream(
        ssc, addresses, StorageLevel.MEMORY_AND_DISK_SER_2(), 100, 5);
  }
}
