package org.apache.spark.examples;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class JavaPageRank {
    public static void main(String[] args) {
        if (args.length < 2 ) {
            System.err.println("Usage: JavaPageRank <file> <iter>");
            System.exit(1);
        }

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("JavaPageRank"));
        JavaRDD<String> lines = sc.textFile(args[0]);
        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(l -> {
                        String[] arr = l.split("\\s");
                        return new Tuple2(arr[0], arr[1]);
                    }).distinct().groupByKey().cache();

        JavaPairRDD<String, Double> ranks = links.mapValues(v -> 1.0);


        for (int current = 0; current < Integer.parseInt(args[1]); current++) {
            JavaPairRDD<String, Double> contribs =
                links.join(ranks).values().flatMapToPair(
                    s -> {
                        Iterable<String> url = s._1();
                        Double rank = s._2();
                        int size = Iterables.size(url);

                        List results = new ArrayList();
                        url.forEach(n -> results.add(new Tuple2(n,rank/size)));
                        return results ;
                    }
                );
            ranks = contribs.reduceByKey((x,y) -> x+y).mapValues(v -> 0.15+0.85*v);
        }

        List<Tuple2<String, Double>> output = ranks.collect();
        output.forEach( tup -> System.out.println( tup._1() + " has rank: " + tup._2() + "."));

        sc.stop();

    }
}