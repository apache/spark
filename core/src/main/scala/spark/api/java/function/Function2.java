package spark.api.java.function;

import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction2;

import java.io.Serializable;

/**
 * A two-argument function that takes arguments of type T1 and T2 and returns an R.
 */
public abstract class Function2<T1, T2, R> extends WrappedFunction2<T1, T2, R>
  implements Serializable {

  public abstract R call(T1 t1, T2 t2) throws Exception;

  public ClassTag<R> returnType() {
    return (ClassTag<R>) ClassTag$.MODULE$.apply(Object.class);
  }

}

