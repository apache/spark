package spark.api.java.function;

import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

/**
 * A function that returns zero or more key-value pair records from each input record. The
 * key-value pairs are represented as scala.Tuple2 objects.
 */
// PairFlatMapFunction does not extend FlatMapFunction because flatMap is
// overloaded for both FlatMapFunction and PairFlatMapFunction.
public abstract class PairFlatMapFunction<T, K, V>
  extends WrappedFunction1<T, Iterable<Tuple2<K, V>>>
  implements Serializable {

  public abstract Iterable<Tuple2<K, V>> call(T t) throws Exception;

  public ClassTag<K> keyType() {
    return (ClassTag<K>) ClassTag$.MODULE$.apply(Object.class);
  }

  public ClassTag<V> valueType() {
    return (ClassTag<V>) ClassTag$.MODULE$.apply(Object.class);
  }
}
