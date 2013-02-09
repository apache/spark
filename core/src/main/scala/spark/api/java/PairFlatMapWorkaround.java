package spark.api.java;

import spark.api.java.JavaPairRDD;
import spark.api.java.JavaRDDLike;
import spark.api.java.function.PairFlatMapFunction;

import java.io.Serializable;

/**
 * Workaround for SPARK-668.
 */
class PairFlatMapWorkaround<T> implements Serializable {
    /**
     *  Return a new RDD by first applying a function to all elements of this
     *  RDD, and then flattening the results.
     */
    public <K, V> JavaPairRDD<K, V> flatMap(PairFlatMapFunction<T, K, V> f) {
        return ((JavaRDDLike <T, ?>) this).doFlatMap(f);
    }
}
