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

#
# Makefile template for building native 'zlib' for hadoop.
#

#
# Notes: 
# 1. This makefile is designed to do the actual builds in $(HADOOP_HOME)/build/native/${os.name}-${os.arch}/$(subdir) .
# 2. This makefile depends on the following environment variables to function correctly:
#    * HADOOP_NATIVE_SRCDIR 
#    * JAVA_HOME
#    * JVM_DATA_MODEL
#    * OS_ARCH 
#    * PLATFORM
#    All these are setup by build.xml and/or the top-level makefile.
# 3. The creation of requisite jni headers/stubs are also done by build.xml and they are
#    assumed to be in $(HADOOP_HOME)/build/native/src/org/apache/hadoop/io/compress/zlib.
#

# The 'vpath directive' to locate the actual source files 
vpath %.c $(HADOOP_NATIVE_SRCDIR)/$(subdir)

AM_CPPFLAGS = @JNI_CPPFLAGS@ -I$(HADOOP_NATIVE_SRCDIR)/src
AM_LDFLAGS = @JNI_LDFLAGS@
AM_CFLAGS = -g -Wall -fPIC -O2 -m$(JVM_DATA_MODEL)

noinst_LTLIBRARIES = libnativezlib.la
libnativezlib_la_SOURCES = ZlibCompressor.c ZlibDecompressor.c
libnativezlib_la_LIBADD = -ldl

#
#vim: sw=4: ts=4: noet
#
