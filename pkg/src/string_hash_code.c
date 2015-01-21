/*
 * A C function for R extension which implements the Java String hash algorithm.
 * Refer to http://en.wikipedia.org/wiki/Java_hashCode%28%29#The_java.lang.String_hash_function
 *
 */

#include <R.h>
#include <Rinternals.h>

/* for compatibility with R before 3.1 */
#ifndef IS_SCALAR
#define IS_SCALAR(x, type) (TYPEOF(x) == (type) && XLENGTH(x) == 1)
#endif

SEXP stringHashCode(SEXP string) {
  const char* str;
  R_xlen_t len, i;
  int hashCode = 0;
  
  if (!IS_SCALAR(string, STRSXP)) {
    error("invalid input");
  }
  
  str = CHAR(asChar(string));
  len = XLENGTH(asChar(string));
  
  for (i = 0; i < len; i++) {
    hashCode = (hashCode << 5) - hashCode + *str++;
  }

  return ScalarInteger(hashCode);
}
