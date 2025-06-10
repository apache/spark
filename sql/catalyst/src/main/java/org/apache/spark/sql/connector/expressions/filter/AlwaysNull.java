package org.apache.spark.sql.connector.expressions.filter;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * A predicate that always evaluates to {@code null}.
 *
 * @since 4.1.0
 */
@Evolving
public final class AlwaysNull extends Predicate implements Literal<Boolean> {

    public AlwaysNull() {
        super("ALWAYS_NULL", new Predicate[]{});
    }

    @Override
    public Boolean value() {
        return null;
    }

    @Override
    public DataType dataType() {
        return DataTypes.BooleanType;
    }

    @Override
    public String toString() { return "NULL"; }
}
