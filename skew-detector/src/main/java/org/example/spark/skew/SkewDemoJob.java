package org.example.spark.skew;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * A demo Spark job that specifically creates a data skew.
 *
 * - 1_000_000 times the "HOT_KEY" key
 * - keys "a", "b", "c", "d", "e" are used once each
 *
 * reduceByKey will create a skew in task execution times.
 */
public class SkewDemoJob {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SkewDemoJob")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        List<String> data = new ArrayList<>();

        for (int i = 0; i < 1_000_000; i++) {
            data.add("HOT_KEY");
        }
        // Several "cold" keys
        data.add("a");
        data.add("b");
        data.add("c");
        data.add("d");
        data.add("e");

        // 100 partitions to have potential for skew
        JavaRDD<String> rdd = sc.parallelize(data, 100);

        JavaPairRDD<String, Integer> pairs = rdd.mapToPair(key -> new Tuple2<>(key, 1));

        JavaPairRDD<String, Integer> reduced = pairs.reduceByKey(Integer::sum);

        // Just to make the job do something
        reduced.collect().forEach(t -> System.out.println(t._1 + " -> " + t._2));

        spark.stop();
    }
}
