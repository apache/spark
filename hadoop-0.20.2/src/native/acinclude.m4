# AC_COMPUTE_NEEDED_DSO(LIBRARY, PREPROC_SYMBOL)
# --------------------------------------------------
# Compute the 'actual' dynamic-library used 
# for LIBRARY and set it to PREPROC_SYMBOL
AC_DEFUN([AC_COMPUTE_NEEDED_DSO],
[
AC_CACHE_CHECK([Checking for the 'actual' dynamic-library for '-l$1'], ac_cv_libname_$1,
  [
  echo 'int main(int argc, char **argv){return 0;}' > conftest.c
  if test -z "`${CC} ${LDFLAGS} -o conftest conftest.c -l$1 2>&1`"; then
    dnl Try objdump and ldd in that order to get the dynamic library
    if test ! -z "`which objdump | grep -v 'no objdump'`"; then
      ac_cv_libname_$1="`objdump -p conftest | grep NEEDED | grep $1 | sed 's/\W*NEEDED\W*\(.*\)\W*$/\"\1\"/'`"
    elif test ! -z "`which ldd | grep -v 'no ldd'`"; then
      ac_cv_libname_$1="`ldd conftest | grep $1 | sed 's/^[[[^A-Za-z0-9]]]*\([[[A-Za-z0-9\.]]]*\)[[[^A-Za-z0-9]]]*=>.*$/\"\1\"/'`"
    elif test ! -z "`which otool | grep -v 'no otool'`"; then
	  ac_cv_libname_$1=\"`otool -L conftest | grep $1 | sed -e 's/^[	 ]*//' -e 's/ .*//' -e 's/.*\/\(.*\)$/\1/'`\";
    else
      AC_MSG_ERROR(Can't find either 'objdump', 'ldd' or 'otool' to compute the dynamic library for '-l$1')
    fi
  else
    ac_cv_libname_$1=libnotfound.so
  fi
  rm -f conftest*
  ]
)
AC_DEFINE_UNQUOTED($2, ${ac_cv_libname_$1}, [The 'actual' dynamic-library for '-l$1'])
])# AC_COMPUTE_NEEDED_DSO
