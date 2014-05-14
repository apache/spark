package org.apache.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class JavaTC {

    private static final int numEdges = 20;
    private static final int numVertices = 10;
    private static final Random rand = new Random(42);

    public static List generateGraph(){
        Set<Tuple2<Integer, Integer>> edges = new HashSet<Tuple2<Integer, Integer>>(numEdges);
        while (edges.size() < numEdges) {
            int from = rand.nextInt(numVertices);
            int to = rand.nextInt(numVertices);
            Tuple2<Integer, Integer> e = new Tuple2<Integer, Integer>(from, to);
            if (from != to) {
                edges.add(e);
            }
        }
        return new ArrayList<Tuple2<Integer, Integer>>(edges);
    }

    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("JavaTC"));
        Integer slices = (args.length > 0) ? Integer.parseInt(args[0]): 2;
        JavaPairRDD<Integer,Integer> tc = sc.parallelizePairs(generateGraph(), slices).cache();

        // Linear transitive closure: each round grows paths by one edge,
        // by joining the graph's edges with the already-discovered paths.
        // e.g. join the path (y, z) from the TC with the edge (x, y) from
        // the graph to obtain the path (x, z).

        // Because join() joins on keys, the edges are stored in reversed order.
        JavaPairRDD edges = tc.mapToPair( x -> new Tuple2(x._2() , x._1()) );
        long oldCount;
        long nextCount = tc.count();

        do {
            oldCount = nextCount ;
            // Perform the join, obtaining an RDD of (y, (z, x)) pairs,
            // then project the result to obtain the new (x, z) paths.
            tc = tc.union(
                tc.join(edges).mapToPair(x -> {
                    Tuple2<Integer,Integer> zx = ((Tuple2<Integer, Tuple2>)x)._2();
                    return new Tuple2(zx._2(), zx._1()) ;
                })
            ).distinct().cache();
            nextCount = tc.count();
        }while(nextCount != oldCount) ;

        System.out.println("TC has " + tc.count() + " edges.");
        sc.stop();

    }
}
