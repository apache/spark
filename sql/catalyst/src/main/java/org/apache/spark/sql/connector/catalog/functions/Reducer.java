package org.apache.spark.sql.connector.catalog.functions;

import org.apache.spark.annotation.Evolving;

/**
 * A 'reducer' for output of user-defined functions.
 *
 * A user_defined function f_source(x) is 'reducible' on another user_defined function f_target(x),
 * if there exists a 'reducer' r(x) such that r(f_source(x)) = f_target(x) for all input x.
 * @param <T> function output type
 * @since 4.0.0
 */
@Evolving
public interface Reducer<T> {
    T reduce(T arg1);
}
