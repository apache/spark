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
#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(linux-task-controller, 1.0.0, mapreduce-dev@hadoop.apache.org)
AC_GNU_SOURCE

AM_INIT_AUTOMAKE([subdir-objects foreign no-dist])

# Dont generate rules to re-automake if timestamps are off
# since we check in the generated files and revision control
# doesn't track timestamps
AM_MAINTAINER_MODE

AC_CONFIG_SRCDIR([impl/task-controller.c])
AC_CONFIG_FILES([Makefile])

AC_PREFIX_DEFAULT(`pwd`/../install)

# Checks for programs.
AC_PROG_CC
AM_PROG_CC_C_O

# Checks for libraries.

# Checks for header files.
AC_LANG(C)
AC_CHECK_HEADERS([unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_CONST
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_FUNC_STRERROR_R

# Checks for library functions.
AC_CHECK_FUNCS([mkdir uname])
AC_OUTPUT
