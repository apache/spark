package spark.api.java.function;


import scala.runtime.AbstractFunction1;

import java.io.Serializable;

// DoubleFunction does not extend Function because some UDF functions, like map,
// are overloaded for both Function and DoubleFunction.
public abstract class DoubleFunction<T> extends AbstractFunction1<T, Double>
  implements Serializable {
  public abstract Double apply(T t);
}
