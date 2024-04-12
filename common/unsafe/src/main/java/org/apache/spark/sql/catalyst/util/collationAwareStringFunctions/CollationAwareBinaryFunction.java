package org.apache.spark.sql.catalyst.util.collationAwareStringFunctions;

import org.apache.spark.sql.catalyst.util.CollationFactory;

/**
 * Collation-aware string expression that takes two parameters.
 *
 * @param <P1> The type of the first parameter.
 * @param <P2> The type of the second parameter.
 * @param <R>  The return type of the function.
 */
public abstract class CollationAwareBinaryFunction<P1, P2, R> {

    public R exec(P1 first, P2 second, int collationId) {
        CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
        if (collation.supportsBinaryEquality) {
            return execBinary(first, second);
        } else if (collation.supportsLowercaseEquality) {
            return execLowercase(first, second);
        } else {
            return execICU(first, second, collationId);
        }
    }

    public String genCode(String first, String second, int collationId) {
        CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
        String className = this.getClass().getSimpleName();
        String expr = "CollationSupport." + className + ".exec";
        if (collation.supportsBinaryEquality) {
            return String.format(expr + "Binary(%s, %s)", first, second);
        } else if (collation.supportsLowercaseEquality) {
            return String.format(expr + "Lowercase(%s, %s)", first, second);
        } else {
            return String.format(expr + "ICU(%s, %s, %d)", first, second, collationId);
        }
    }

    public abstract R execBinary(P1 first, P2 second);
    public abstract R execLowercase(P1 first, P2 second);
    public abstract R execICU(P1 first, P2 second, int collationId);
}

