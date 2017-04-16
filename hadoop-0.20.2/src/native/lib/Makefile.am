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
# Makefile template for building libhadoop.so 
#

#
# Notes: 
# 1. This makefile is designed to do the actual builds in $(HADOOP_HOME)/build/native/${os.name}-${os.arch}/lib 
# 2. This makefile depends on the following environment variables to function correctly:
#    * HADOOP_NATIVE_SRCDIR 
#    * JAVA_HOME
#    * OS_ARCH 
#    All these are setup by build.xml and/or the top-level makefile.
#

# Add .lo files in $(SUBDIRS) to construct libhadoop.so
HADOOP_OBJS = $(foreach path,$(addprefix ../,$(SUBDIRS)),$(wildcard $(path)/*.lo))
AM_LDFLAGS = @JNI_LDFLAGS@ -m$(JVM_DATA_MODEL)

lib_LTLIBRARIES = libhadoop.la
libhadoop_la_SOURCES = 
libhadoop_la_LDFLAGS = -version-info 1:0:0 $(AM_LDFLAGS)
libhadoop_la_LIBADD = $(HADOOP_OBJS) -ldl

#
#vim: sw=4: ts=4: noet
#
