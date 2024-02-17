package org.apache.spark.sql.connector.catalog.functions;

import org.apache.spark.annotation.Evolving;
import scala.Option;

/**
 * Base class for user-defined functions that can be 'reduced' on another function.
 *
 * A function f_source(x) is 'reducible' on another function f_target(x) if
 * there exists a reducer function r(x) such that r(f_source(x)) = f_target(x) for all input x.
 *
 * @since 4.0.0
 */
@Evolving
public interface ReducibleFunction<T, A> extends ScalarFunction<T> {

    /**
     * If this function is 'reducible' on another function, return the {@link Reducer} function.
     * @param other other function
     * @param thisArgument argument for this function instance
     * @param otherArgument argument for other function instance
     * @return a reduction function if it is reducible, none if not
     */
    Option<Reducer<A>> reducer(ReducibleFunction<?, ?> other, Option<?> thisArgument, Option<?> otherArgument);
}
