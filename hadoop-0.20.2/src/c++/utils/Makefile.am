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
ACLOCAL_AMFLAGS = -I m4
AM_CXXFLAGS=-I$(srcdir)/api -Wall

# List the api header files and where they will be installed
apidir = $(includedir)/hadoop
api_HEADERS = \
	api/hadoop/StringUtils.hh \
	api/hadoop/SerialUtils.hh


# Define the libaries that need to be built
lib_LIBRARIES = libhadooputils.a

# Define the sources for lib 
libhadooputils_a_SOURCES = \
	impl/StringUtils.cc \
	impl/SerialUtils.cc

