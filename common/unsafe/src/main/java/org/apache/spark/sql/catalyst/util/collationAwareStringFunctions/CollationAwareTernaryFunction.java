package org.apache.spark.sql.catalyst.util.collationAwareStringFunctions;

import org.apache.spark.sql.catalyst.util.CollationFactory;

/**
 * Collation-aware string functions that takes three parameters.
 *
 * @param <P1> The type of the first parameter.
 * @param <P2> The type of the second parameter.
 * @param <P3> The type of the third parameter.
 * @param <R>  The return type of the function.
 */
public abstract class CollationAwareTernaryFunction<P1, P2, P3, R> {

    public R exec(P1 first, P2 second, P3 third, int collationId) {
        CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
        if (collation.supportsBinaryEquality) {
            return execBinary(first, second, third);
        } else if (collation.supportsLowercaseEquality) {
            return execLowercase(first, second, third);
        } else {
            return execICU(first, second, third, collationId);
        }
    }

    public String genCode(String first, String second, String third, int collationId) {
        CollationFactory.Collation collation = CollationFactory.fetchCollation(collationId);
        String className = this.getClass().getSimpleName();
        String expr = "CollationSupport." + className + ".exec";
        if (collation.supportsBinaryEquality) {
            return String.format(expr + "Binary(%s, %s, %s)", first, second, third);
        } else if (collation.supportsLowercaseEquality) {
            return String.format(expr + "Lowercase(%s, %s, %s)", first, second, third);
        } else {
            return String.format(expr + "ICU(%s, %s, %s, %d)", first, second, third, collationId);
        }
    }

    public abstract R execBinary(P1 first, P2 second, P3 third);
    public abstract R execLowercase(P1 first, P2 second, P3 third);
    public abstract R execICU(P1 first, P2 second, P3 third, int collationId);
}
