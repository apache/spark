#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

AC_PREREQ(2.59)
AC_INIT([runAs],[0.1])

#changing default prefix value to empty string, so that binary does not
#gets installed within system
AC_PREFIX_DEFAULT(.)

#add new arguments --with-home
AC_ARG_WITH(home,[--with-home path to hadoop home dir])
AC_CONFIG_SRCDIR([main.c])
AC_CONFIG_HEADER([runAs.h])

# Checks for programs.
AC_PROG_CC

# Checks for libraries.

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([stdlib.h string.h unistd.h fcntl.h])

#check for HADOOP_HOME
if test "$with_home" != ""
then
AC_DEFINE_UNQUOTED(HADOOP_HOME,"$with_home")
fi

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_TYPE_PID_T
AC_TYPE_MODE_T
AC_TYPE_SIZE_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_FUNC_CHOWN
AC_CHECK_FUNCS([strerror memset mkdir rmdir strdup])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT

AC_HEADER_STDBOOL
AC_PROG_MAKE_SET
