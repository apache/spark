package spark.api.java;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

// See
// http://scala-programming-language.1934581.n4.nabble.com/Workaround-for-implementing-java-varargs-in-2-7-2-final-tp1944767p1944772.html
abstract class JavaSparkContextVarargsWorkaround {
  public <T> JavaRDD<T> union(JavaRDD<T>... rdds) {
    if (rdds.length == 0) {
      throw new IllegalArgumentException("Union called on empty list");
    }
    ArrayList<JavaRDD<T>> rest = new ArrayList<JavaRDD<T>>(rdds.length - 1);
    for (int i = 1; i < rdds.length; i++) {
      rest.add(rdds[i]);
    }
    return union(rdds[0], rest);
  }

  public JavaDoubleRDD union(JavaDoubleRDD... rdds) {
    if (rdds.length == 0) {
      throw new IllegalArgumentException("Union called on empty list");
    }
    ArrayList<JavaDoubleRDD> rest = new ArrayList<JavaDoubleRDD>(rdds.length - 1);
    for (int i = 1; i < rdds.length; i++) {
      rest.add(rdds[i]);
    }
    return union(rdds[0], rest);
  }

  public <K, V> JavaPairRDD<K, V> union(JavaPairRDD<K, V>... rdds) {
    if (rdds.length == 0) {
      throw new IllegalArgumentException("Union called on empty list");
    }
    ArrayList<JavaPairRDD<K, V>> rest = new ArrayList<JavaPairRDD<K, V>>(rdds.length - 1);
    for (int i = 1; i < rdds.length; i++) {
      rest.add(rdds[i]);
    }
    return union(rdds[0], rest);
  }

  // These methods take separate "first" and "rest" elements to avoid having the same type erasure
  abstract public <T> JavaRDD<T> union(JavaRDD<T> first, List<JavaRDD<T>> rest);
  abstract public JavaDoubleRDD union(JavaDoubleRDD first, List<JavaDoubleRDD> rest);
  abstract public <K, V> JavaPairRDD<K, V> union(JavaPairRDD<K, V> first, List<JavaPairRDD<K, V>> rest);
}
