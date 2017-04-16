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
ACLOCAL_AMFLAGS = -I ../../c++/utils/m4
AM_CXXFLAGS=-Wall -I$(HADOOP_UTILS_PREFIX)/include \
            -I$(HADOOP_PIPES_PREFIX)/include
LDADD=-L$(HADOOP_UTILS_PREFIX)/lib -L$(HADOOP_PIPES_PREFIX)/lib \
      -lhadooppipes -lhadooputils

bin_PROGRAMS= wordcount-simple wordcount-part wordcount-nopipe pipes-sort

# Define the sources for each program
wordcount_simple_SOURCES = \
	impl/wordcount-simple.cc

wordcount_part_SOURCES = \
	impl/wordcount-part.cc

wordcount_nopipe_SOURCES = \
	impl/wordcount-nopipe.cc

pipes_sort_SOURCES = \
        impl/sort.cc

