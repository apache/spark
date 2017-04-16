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
# AC - autoconf
#########################################################################
# Section 1:
# DO NOT TOUCH EXCEPT TO CHANGE Product-Name and Rev# IN AC_INIT

AC_PREREQ(2.52)
AC_INIT([fuse_dfs], [0.1.0])
#AC_CONFIG_AUX_DIR([/usr/share/automake-1.9])
# To install locally
AC_CANONICAL_TARGET()
FUSE_DFS_INITIALIZE([localinstall])
AC_PREFIX_DEFAULT([`pwd`])



#case $target in
#*64*intel)
#     OS_ARCH=intel64 ;;
#*64*amd* | *64*unknown*)
#     OS_ARCH=amd64 ;;
#$esac
#AC_SUBST(OS_ARCH)
 DEFS=""
AC_SUBST([DEFS])

# Need GNU source for multiple hashtables from glibc
AC_GNU_SOURCE

AC_FUNC_GETGROUPS
AC_TYPE_GETGROUPS

AC_PROG_CC
AC_SYS_LARGEFILE

############################################################################
# Section 2:
# User Configurable system defaults. Change With CAUTION!

# User can include custom makefile rules. Uncomment and update only <name> in PRODUCT_MK.
# Include where appropriate in any Makefile.am as @PRODUCT_MK@

# Default path to external components and shared build tools
# To point to other locations set  environment variable EXTERNAL_PATH.
# DO NOT change default. Changing default value requires changing bootstrap.sh.
FUSE_DFS_WITH_EXTERNAL_PATH([`pwd`])

# Pre-defined macro to set optimized build mode. Configure with --disable-opt option to turn off optimization. Default CXXFLAGS set to '-Wall -O3'. In debug mode  CXXFLAGS is  '-Wall -g'
# FUSE_DFSENABLE_DEFAULT_DEBUG_BUILD
FUSE_DFS_ENABLE_DEFAULT_OPT_BUILD

# Predefined macro to set static library mode. Configure with --disable-static option to turn off static lib mode.
# FUSE_DFS_ENABLE_DEFAULT_SHARED
FUSE_DFS_ENABLE_DEFAULT_STATIC

AC_CONFIG_FILES(Makefile src/Makefile)


############################################################################
# Section 4:
# DO NOT TOUCH.

AC_SUBST(PRODUCT_MK)
AC_OUTPUT
