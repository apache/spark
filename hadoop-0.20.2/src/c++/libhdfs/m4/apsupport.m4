dnl
dnl Licensed to the Apache Software Foundation (ASF) under one or more
dnl contributor license agreements.  See the NOTICE file distributed with
dnl this work for additional information regarding copyright ownership.
dnl The ASF licenses this file to You under the Apache License, Version 2.0
dnl (the "License"); you may not use this file except in compliance with
dnl the License.  You may obtain a copy of the License at
dnl
dnl     http://www.apache.org/licenses/LICENSE-2.0
dnl
dnl Unless required by applicable law or agreed to in writing, software
dnl distributed under the License is distributed on an "AS IS" BASIS,
dnl WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
dnl See the License for the specific language governing permissions and
dnl limitations under the License.
dnl

dnl -------------------------------------------------------------------------
dnl Author  Pier Fumagalli <mailto:pier.fumagalli@eng.sun.com>
dnl Version $Id$
dnl -------------------------------------------------------------------------

AC_DEFUN([AP_SUPPORTED_HOST],[
  AC_MSG_CHECKING([C flags dependant on host system type])

  case $host_os in
  darwin*)
    CFLAGS="$CFLAGS -DOS_DARWIN -DDSO_DYLD"
    supported_os="darwin"
    ;;
  solaris*)
    CFLAGS="$CFLAGS -DOS_SOLARIS -DDSO_DLFCN"
    supported_os="solaris"
    LIBS="$LIBS -ldl -lthread"
    ;;
  linux*)
    CFLAGS="$CFLAGS -DOS_LINUX -DDSO_DLFCN"
    supported_os="linux"
    LIBS="$LIBS -ldl -lpthread"
    ;;
  cygwin)
    CFLAGS="$CFLAGS -DOS_CYGWIN -DDSO_DLFCN -DNO_SETSID"
    supported_os="win32"
    ;;
  sysv)
    CFLAGS="$CFLAGS -DOS_SYSV -DDSO_DLFCN"
    LIBS="$LIBS -ldl"
    ;;
  sysv4)
    CFLAGS="$CFLAGS -DOS_SYSV -DDSO_DLFCN -Kthread"
    LDFLAGS="-Kthread $LDFLAGS"
    LIBS="$LIBS -ldl"
    ;;
  freebsd*)
    CFLAGS="$CFLAGS -DOS_FREEBSD -DDSO_DLFCN -D_THREAD_SAFE -pthread"
    LDFLAGS="-pthread $LDFLAGS"
    supported_os="freebsd"
    ;;
  osf5*)
    CFLAGS="$CFLAGS -pthread -DOS_TRU64 -DDSO_DLFCN -D_XOPEN_SOURCE_EXTENDED"
    LDFLAGS="$LDFLAGS -pthread"
    ;;
  hpux11*)
    CFLAGS="$CFLAGS -pthread -DOS_HPUX -DDSO_DLFCN"
    LDFLAGS="$LDFLAGS -pthread"
    LIBS="$LIBS -lpthread"
    ;;
  *)
    AC_MSG_RESULT([failed])
    AC_MSG_ERROR([Unsupported operating system "$host_os"]);;
  esac

  case $host_cpu in
  powerpc)
    CFLAGS="$CFLAGS -DCPU=\\\"$host_cpu\\\""
    HOST_CPU=$host_cpu;;
  sparc*)
    CFLAGS="$CFLAGS -DCPU=\\\"$host_cpu\\\""
    HOST_CPU=$host_cpu;;
  i?86)
    CFLAGS="$CFLAGS -DCPU=\\\"i386\\\""
    HOST_CPU=i386;;
  x86_64)
    CFLAGS="$CFLAGS -DCPU=\\\"amd64\\\""
    HOST_CPU=amd64;;
  bs2000)
    CFLAGS="$CFLAGS -DCPU=\\\"osd\\\" -DCHARSET_EBCDIC -DOSD_POSIX"
    supported_os="osd"
    LDFLAGS="-Kno_link_stdlibs -B llm4"
    LIBS="$LIBS -lBLSLIB"
    LDCMD="/opt/C/bin/cc"
    HOST_CPU=osd;;
  mips)
    CFLAGS="$CFLAGS -DCPU=\\\"mips\\\""
    supported_os="mips"
    HOST_CPU=mips;;
  alpha*)
    CFLAGS="$CFLAGS -DCPU=\\\"alpha\\\""
    supported_os="alpha"
    HOST_CPU=alpha;;
  hppa2.0w)
    CFLAGS="$CFLAGS -DCPU=\\\"PA_RISC2.0W\\\""
    supported_os="hp-ux"
    HOST_CPU=PA_RISC2.0W;;
  hppa2.0)
    CFLAGS="$CFLAGS -DCPU=\\\"PA_RISC2.0\\\""
    supported_os="hp-ux"
    HOST_CPU=PA_RISC2.0;;
  mipsel)
    CFLAGS="$CFLAGS -DCPU=\\\"mipsel\\\""
    supported_os="mipsel"
    HOST_CPU=mipsel;;
  ia64)
    CFLAGS="$CFLAGS -DCPU=\\\"ia64\\\""
    supported_os="ia64"
    HOST_CPU=ia64;;
  s390)
    CFLAGS="$CFLAGS -DCPU=\\\"s390\\\""
    supported_os="s390"
    HOST_CPU=s390;;
  *)
    AC_MSG_RESULT([failed])
    AC_MSG_ERROR([Unsupported CPU architecture "$host_cpu"]);;
  esac

  AC_MSG_RESULT([ok])
  AC_SUBST(CFLAGS)
  AC_SUBST(LDFLAGS)
])

AC_DEFUN([AP_JVM_LIBDIR],[
  AC_MSG_CHECKING([where on earth this jvm library is..])
  javabasedir=$JAVA_HOME
  case $host_os in
    cygwin* | mingw* | pw23* )
    lib_jvm_dir=`find $javabasedir -follow \( \
	\( -name client -type d -prune \) -o \
        \( -name "jvm.dll" -exec dirname {} \; \) \) 2> /dev/null | tr "\n" " "`
    ;;
    aix*)
    lib_jvm_dir=`find $javabasedir \( \
        \( -name client -type d -prune \) -o \
	\( -name "libjvm.*" -exec dirname {} \; \) \) 2> /dev/null | tr "\n" " "`
    if test -z "$lib_jvm_dir"; then
       lib_jvm_dir=`find $javabasedir \( \
       \( -name client -type d -prune \) -o \
       \( -name "libkaffevm.*" -exec dirname {} \; \) \) 2> /dev/null | tr "\n" " "`
    fi
    ;;
    *)
    lib_jvm_dir=`find $javabasedir -follow \( \
       \( -name client -type d -prune \) -o \
       \( -name "libjvm.*" -exec dirname {} \; \) \) 2> /dev/null | tr "\n" " "`
    if test -z "$lib_jvm_dir"; then
       lib_jvm_dir=`find $javabasedir -follow \( \
       \( -name client -type d -prune \) -o \
       \( -name "libkaffevm.*" -exec dirname {} \; \) \) 2> /dev/null | tr "\n" " "`
    fi 
    ;;
  esac
  LIB_JVM_DIR=$lib_jvm_dir
  AC_MSG_RESULT([ohh u there ... $LIB_JVM_DIR])
  AC_SUBST(LIB_JVM_DIR)
])
