package spark.api.java.function;


import scala.runtime.AbstractFunction1;

import java.io.Serializable;

/**
 * A function that returns Doubles, and can be used to construct DoubleRDDs.
 */
// DoubleFunction does not extend Function because some UDF functions, like map,
// are overloaded for both Function and DoubleFunction.
public abstract class DoubleFunction<T> extends WrappedFunction1<T, Double>
  implements Serializable {

  public abstract Double call(T t) throws Exception;
}
