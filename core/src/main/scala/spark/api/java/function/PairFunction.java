package spark.api.java.function;

import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;

/**
 * A function that returns key-value pairs (Tuple2<K, V>), and can be used to construct PairRDDs.
 */
// PairFunction does not extend Function because some UDF functions, like map,
// are overloaded for both Function and PairFunction.
public abstract class PairFunction<T, K, V>
  extends WrappedFunction1<T, Tuple2<K, V>>
  implements Serializable {

  public abstract Tuple2<K, V> call(T t) throws Exception;

  public ClassTag<K> keyType() {
    return (ClassTag<K>) ClassTag$.MODULE$.apply(Object.class);
  }

  public ClassTag<V> valueType() {
    return (ClassTag<V>) ClassTag$.MODULE$.apply(Object.class);
  }
}
