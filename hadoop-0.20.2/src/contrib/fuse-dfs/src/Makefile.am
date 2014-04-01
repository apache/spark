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
bin_PROGRAMS = fuse_dfs
fuse_dfs_SOURCES = fuse_dfs.c fuse_options.c fuse_trash.c fuse_stat_struct.c fuse_users.c fuse_init.c fuse_connect.c fuse_impls_access.c fuse_impls_chmod.c  fuse_impls_chown.c  fuse_impls_create.c  fuse_impls_flush.c fuse_impls_getattr.c  fuse_impls_mkdir.c  fuse_impls_mknod.c  fuse_impls_open.c fuse_impls_read.c fuse_impls_release.c fuse_impls_readdir.c fuse_impls_rename.c fuse_impls_rmdir.c fuse_impls_statfs.c fuse_impls_symlink.c fuse_impls_truncate.c fuse_impls_utimens.c  fuse_impls_unlink.c fuse_impls_write.c
AM_CFLAGS= -Wall -g
AM_CPPFLAGS= -DPERMS=$(PERMS) -D_FILE_OFFSET_BITS=64 -I$(JAVA_HOME)/include -I$(HADOOP_HOME)/src/c++/libhdfs -I$(JAVA_HOME)/include/linux -D_FUSE_DFS_VERSION=\"$(PACKAGE_VERSION)\" -DPROTECTED_PATHS=\"$(PROTECTED_PATHS)\" -I$(FUSE_HOME)/include
AM_LDFLAGS= -L$(HADOOP_HOME)/build/c++/$(BUILD_PLATFORM)/lib -lhdfs -L$(FUSE_HOME)/lib -lfuse -L$(JAVA_HOME)/jre/lib/$(OS_ARCH)/server -ljvm
