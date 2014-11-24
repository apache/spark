#include <R.h>
#include <Rinternals.h>

SEXP stringHashCode(SEXP string) {
  const char* str;
  R_xlen_t len, i;
  SEXP hashCodeR;
  int hashCode = 0;
  
  if (!IS_SCALAR(string, STRSXP)) {
    error("invalid input");
  }
  
  str = CHAR(asChar(string));
  len = XLENGTH(asChar(string));
  
  for (i = 0; i < len; i++) {
    hashCode = (hashCode << 5) - hashCode + *str++;
  }

  hashCodeR = PROTECT(allocVector(INTSXP, 1));
  INTEGER(hashCodeR)[0] = hashCode;
  UNPROTECT(1);
  return hashCodeR;
}
