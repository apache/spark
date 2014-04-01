#
# Copyright 2005 The Apache Software Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Autoconf input file
# $Id$

AC_INIT([libhdfs], [0.1.0], omalley@apache.org)
AC_PREFIX_DEFAULT([`pwd`/../install])
AC_CONFIG_AUX_DIR([config])

# Generates Makefile from Makefile.am. Modify when new subdirs are added.
# Change Makefile.am also to add subdirectly.
AM_INIT_AUTOMAKE(foreign no-dist)
AC_CONFIG_FILES(Makefile)

# Dont generate rules to re-automake if timestamps are off
# since we check in the generated files and revision control
# doesn't track timestamps
AM_MAINTAINER_MODE

LT_INIT
 
AC_CONFIG_MACRO_DIR([m4])
dnl -------------------------------------------------------------------------
dnl Check current host (forget about cross compilation) and validate it
dnl against the cache (fail if the cache differs)
dnl -------------------------------------------------------------------------
AP_MSG_HEADER([Current host])
AC_CANONICAL_HOST()
AP_CANONICAL_HOST_CHECK()

dnl -------------------------------------------------------------------------
dnl Check C environment
dnl -------------------------------------------------------------------------
AP_MSG_HEADER([C-Language compilation tools])
AC_PROG_CC()
AC_CHECK_TOOL(RANLIB, ranlib, :)

dnl -------------------------------------------------------------------------
dnl Check if this host is supported
dnl -------------------------------------------------------------------------
AP_MSG_HEADER([Host support])
AP_SUPPORTED_HOST()
if test "$supported_os" = "darwin"
then
  if test -z "$JAVA_HOME" -a -d /System/Library/Frameworks/JavaVM.framework/Home; then
  	JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Home
  fi

  _prevdir=`/bin/pwd`
  if test -n "$JAVA_HOME" -a -d "$JAVA_HOME/include"; then
    cd "$JAVA_HOME/include"
  elif test -n "$JAVA_HOME" -a -d "$JAVA_HOME/../Headers"; then
    cd "$JAVA_HOME/../Headers"
  else
    cd /System/Library/Frameworks/JavaVM.framework/Headers
  fi
  CFLAGS="$CFLAGS -m${JVM_ARCH} -I`/bin/pwd -P`"
  cd $_prevdir
  unset _prevdir
fi

dnl -------------------------------------------------------------------------
dnl Check JAVA environment
dnl -------------------------------------------------------------------------
AP_MSG_HEADER([Java compilation tools])
AP_JAVA()
AP_SABLEVM()
AP_KAFFE()
AP_PROG_JAVAC()
AP_PROG_JAR()
AP_JVM_LIBDIR()
if test "$supported_os" != "darwin"
then
  CFLAGS="$CFLAGS -m${JVM_ARCH} -I$JAVA_HOME/include -I$JAVA_HOME/include/$supported_os"
  LDFLAGS="$LDFLAGS -m${JVM_ARCH} -L$LIB_JVM_DIR -ljvm -shared -Wl,-x"
  AC_MSG_RESULT([VALUE OF JVM_ARCH IS :$JVM_ARCH])
fi

dnl -------------------------------------------------------------------------
dnl Add gcc specific CFLAGS.
dnl -------------------------------------------------------------------------
if test "$GCC" = "yes"
then
   CFLAGS="$CFLAGS -Wall -Wstrict-prototypes"
   AC_MSG_RESULT([gcc flags added])
fi
dnl -------------------------------------------------------------------------
dnl Add gcc specific CFLAGS.
dnl -------------------------------------------------------------------------
if test -z "$LDCMD"
then
   LDCMD="$CC"
fi
AC_SUBST(LDCMD)


AC_PROG_CC
AC_PROG_LIBTOOL

AC_TYPE_SIZE_T
AC_CHECK_FUNCS([strdup strerror strtoul])
AC_CHECK_HEADERS([fcntl.h])
AC_C_CONST
AC_C_VOLATILE
#AC_FUNC_MALLOC
AC_HEADER_STDBOOL
AC_SUBST(PRODUCT_MK)
AC_OUTPUT
