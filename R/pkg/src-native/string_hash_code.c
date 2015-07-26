/*
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

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
