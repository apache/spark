package org.apache.spark.sql.catalyst.util.collationAwareStringFunctions;

import org.apache.spark.sql.catalyst.util.CollationFactory;

/**
 * Collation-aware string expression that takes one parameter.
 * @param <P1> The type of the first parameter.
 * @param <R> The return type of the function.
 */
public abstract class CollationAwareUnaryFunction<P1, R> {

    public R exec(P1 first, int collationId) {
        CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
        if (collation.supportsBinaryEquality) {
            return execBinary(first);
        } else if (collation.supportsLowercaseEquality) {
            return execLowercase(first);
        } else {
            return execICU(first, collationId);
        }
    }

    public String genCode(String first, int collationId) {
        CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
        String className = this.getClass().getSimpleName();
        String expr = "CollationSupport." + className + ".exec";
        if (collation.supportsBinaryEquality) {
            return String.format(expr + "Binary(%s)", first);
        } else if (collation.supportsLowercaseEquality) {
            return String.format(expr + "Lowercase(%s)", first);
        } else {
            return String.format(expr + "ICU(%s, %d)", first, collationId);
        }
    }

    public abstract R execBinary(P1 first);
    public abstract R execLowercase(P1 first);
    public abstract R execICU(P1 first, int collationId);
}

