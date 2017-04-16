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

@PRODUCT_MK@

#AM_CPPFLAGS = -I$(top_srcdir)
ACLOCAL_AMFLAGS = -I m4

lib_LTLIBRARIES = libhdfs.la
libhdfs_la_SOURCES = hdfs.c hdfsJniHelper.c hdfs.h

#check_PROGRAMS = hdfs_test hdfs_read hdfs_write
check_PROGRAMS = hdfs_test hdfs_read hdfs_write

hdfs_test_SOURCES = hdfs_test.c hdfs.h
hdfs_test_LDADD = ${libdir}/libhdfs.la 

hdfs_read_SOURCES = hdfs_read.c
hdfs_read_LDADD = ${libdir}/libhdfs.la 

hdfs_write_SOURCES = hdfs_write.c
hdfs_write_LDADD = ${libdir}/libhdfs.la 

test: hdfs_test hdfs_read hdfs_write 
	${LIBHDFS_SRC_DIR}/tests/test-libhdfs.sh	


# vim: sw=4: ts=4: noet
