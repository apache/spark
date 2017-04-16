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

AC_DEFUN([AP_PROG_JAVAC_WORKS],[
  AC_CACHE_CHECK([wether the Java compiler ($JAVAC) works],ap_cv_prog_javac_works,[
    echo "public class Test {}" > Test.java
    $JAVAC $JAVACFLAGS Test.java > /dev/null 2>&1
    if test $? -eq 0
    then
      rm -f Test.java Test.class
      ap_cv_prog_javac_works=yes
    else
      rm -f Test.java Test.class
      AC_MSG_RESULT(no)
      AC_MSG_ERROR([installation or configuration problem: javac cannot compile])
    fi
  ])
])

dnl Check for JAVA compilers.
AC_DEFUN([AP_PROG_JAVAC],[
  if test "$SABLEVM" != "NONE"
  then
    AC_PATH_PROG(JAVACSABLE,javac-sablevm,NONE,$JAVA_HOME/bin)
  else
    JAVACSABLE="NONE"
  fi
  if test "$JAVACSABLE" = "NONE"
  then
    XPATH="$JAVA_HOME/bin:$JAVA_HOME/Commands:$PATH"
    AC_PATH_PROG(JAVAC,javac,NONE,$XPATH)
  else
    AC_PATH_PROG(JAVAC,javac-sablevm,NONE,$JAVA_HOME/bin)
  fi
  AC_MSG_RESULT([$JAVAC])
  if test "$JAVAC" = "NONE"
  then
    AC_MSG_ERROR([javac not found])
  fi
  AP_PROG_JAVAC_WORKS()
  AC_PROVIDE([$0])
  AC_SUBST(JAVAC)
  AC_SUBST(JAVACFLAGS)
])

dnl Check for jar archivers.
AC_DEFUN([AP_PROG_JAR],[
  if test "$SABLEVM" != "NONE"
  then
    AC_PATH_PROG(JARSABLE,jar-sablevm,NONE,$JAVA_HOME/bin)
  else
    JARSABLE="NONE"
  fi
  if test "$JARSABLE" = "NONE"
  then
    XPATH="$JAVA_HOME/bin:$JAVA_HOME/Commands:$PATH"
    AC_PATH_PROG(JAR,jar,NONE,$XPATH)
  else
    AC_PATH_PROG(JAR,jar-sablevm,NONE,$JAVA_HOME/bin)
  fi
  if test "$JAR" = "NONE"
  then
    AC_MSG_ERROR([jar not found])
  fi
  AC_PROVIDE([$0])
  AC_SUBST(JAR)
])

AC_DEFUN([AP_JAVA],[
  AC_ARG_WITH(java,[  --with-java=DIR         Specify the location of your JDK installation],[
    AC_MSG_CHECKING([JAVA_HOME])
    if test -d "$withval"
    then
      JAVA_HOME="$withval"
      AC_MSG_RESULT([$JAVA_HOME])
    else
      AC_MSG_RESULT([failed])
      AC_MSG_ERROR([$withval is not a directory])
    fi
    AC_SUBST(JAVA_HOME)
  ])
  if test x"$JAVA_HOME" = x
  then
    AC_MSG_ERROR([Java Home not defined. Rerun with --with-java=[...] parameter])
  fi
])

dnl check if the JVM in JAVA_HOME is sableVM
dnl $JAVA_HOME/bin/sablevm and /opt/java/lib/sablevm/bin are tested.
AC_DEFUN([AP_SABLEVM],[
  if test x"$JAVA_HOME" != x
  then
    AC_PATH_PROG(SABLEVM,sablevm,NONE,$JAVA_HOME/bin)
    if test "$SABLEVM" = "NONE"
    then
      dnl java may be SableVM.
      if $JAVA_HOME/bin/java -version 2> /dev/null | grep SableVM > /dev/null
      then
        SABLEVM=$JAVA_HOME/bin/java
      fi
    fi
    if test "$SABLEVM" != "NONE"
    then
      AC_MSG_RESULT([Using sableVM: $SABLEVM])
      CFLAGS="$CFLAGS -DHAVE_SABLEVM"
    fi
  fi
])

dnl check if the JVM in JAVA_HOME is kaffe
dnl $JAVA_HOME/bin/kaffe is tested.
AC_DEFUN([AP_KAFFE],[
  if test x"$JAVA_HOME" != x
  then
    AC_PATH_PROG(KAFFEVM,kaffe,NONE,$JAVA_HOME/bin)
    if test "$KAFFEVM" != "NONE"
    then
      AC_MSG_RESULT([Using kaffe: $KAFFEVM])
      CFLAGS="$CFLAGS -DHAVE_KAFFEVM"
      LDFLAGS="$LDFLAGS -Wl,-rpath $JAVA_HOME/jre/lib/$HOST_CPU -L $JAVA_HOME/jre/lib/$HOST_CPU -lkaffevm"
    fi
  fi
])
